package analyzer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Config struct {
	SiteID                string
	DeviceID              string
	OutboxDir             string
	LogRoot               string
	IncludeGlobs          []string
	ExcludeDirs           []string
	DuplicateRunThreshold int
	FallbackToLatestFile  bool
	Debug                 bool
	StatusThresholds      StatusThresholds
}

type StatusThresholds struct {
	ErrorTimeout      int
	ErrorNoResponse   int
	ErrorZeroData     int
	ErrorDuplicates   int
	WarningTimeout    int
	WarningNoResponse int
	WarningZeroData   int
	WarningDuplicates int
}

type Metrics struct {
	Lines          int      `json:"lines"`
	Timeout        int      `json:"timeout"`
	NoResponse     int      `json:"no_response"`
	ZeroData       int      `json:"zero_data"`
	Duplicates     int      `json:"duplicates"`
	UniqueRatioPct *float64 `json:"unique_ratio_pct"`
	TotalPayloads  int      `json:"-"`
	UniquePayloads int      `json:"-"`
}

type Examples struct {
	FirstTimeoutLine    string `json:"first_timeout_line,omitempty"`
	FirstNoResponseLine string `json:"first_no_response_line,omitempty"`
	FirstZeroDataLine   string `json:"first_zero_data_line,omitempty"`
	TopDuplicatePayload string `json:"top_duplicate_payload,omitempty"`
	Note                string `json:"note,omitempty"`
}

type SensorResult struct {
	SensorID   string   `json:"sensor_id"`
	SensorType string   `json:"sensor_type"`
	Status     string   `json:"status"`
	Metrics    Metrics  `json:"metrics"`
	Examples   Examples `json:"examples"`
}

type Summary struct {
	SiteID      string         `json:"site_id"`
	DeviceID    string         `json:"device_id"`
	Date        string         `json:"date"`
	GeneratedAt string         `json:"generated_at"`
	LogRoot     string         `json:"log_root"`
	Sensors     []SensorResult `json:"sensors"`
	TopIssues   []TopIssue     `json:"top_issues"`
}

type TopIssue struct {
	Type     string `json:"type"`
	SensorID string `json:"sensor_id"`
	Count    int    `json:"count"`
}

func AnalyzeDaily(cfg Config, date string, maxLines int) (Summary, error) {
	if date == "" {
		return Summary{}, errors.New("date is required")
	}
	if maxLines <= 0 {
		maxLines = 5000
	}
	if cfg.DuplicateRunThreshold <= 0 {
		cfg.DuplicateRunThreshold = 3
	}
	cfg = withDefaultThresholds(cfg)

	datePrefix, err := normalizeDatePrefix(date)
	if err != nil {
		return Summary{}, err
	}

	dirs, err := findSensorDirs(cfg.LogRoot, cfg.IncludeGlobs, cfg.ExcludeDirs)
	if err != nil {
		return Summary{}, err
	}

	var results []SensorResult
	for _, dir := range dirs {
		result, err := analyzeSensorDir(dir, datePrefix, maxLines, cfg)
		if err != nil {
			return Summary{}, err
		}
		if result.SensorID != "" {
			results = append(results, result)
		}
	}

	summary := Summary{
		SiteID:      cfg.SiteID,
		DeviceID:    cfg.DeviceID,
		Date:        date,
		GeneratedAt: time.Now().Format(time.RFC3339),
		LogRoot:     cfg.LogRoot,
		Sensors:     results,
		TopIssues:   buildTopIssues(results),
	}
	return summary, nil
}

func analyzeSensorDir(dir, datePrefix string, maxLines int, cfg Config) (SensorResult, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return SensorResult{}, err
	}

	sensorID := filepath.Base(dir)
	sensorType := sensorTypeFromID(sensorID)
	if sensorType == "" {
		return SensorResult{}, nil
	}

	metrics := Metrics{}
	examples := Examples{}
	payloadCounts := map[string]int{}
	var lastPayload string
	consecutive := 0
	linesRead := 0

	files, fileNotes, err := selectFiles(entries, dir, datePrefix, cfg.FallbackToLatestFile)
	if err != nil {
		return SensorResult{}, err
	}
	if cfg.Debug {
		fmt.Printf("sensor=%s files=%d fallback=%t\n", sensorID, len(files), fileNotes.usedFallback)
	}

	for _, path := range files {
		file, err := os.Open(path)
		if err != nil {
			return SensorResult{}, err
		}

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			if linesRead >= maxLines {
				break
			}
			line := scanner.Text()
			if !strings.HasPrefix(line, datePrefix) {
				continue
			}
			linesRead++
			metrics, examples, lastPayload, consecutive = updateMetrics(metrics, examples, line, cfg, payloadCounts, lastPayload, consecutive)
		}
		file.Close()
		if err := scanner.Err(); err != nil {
			return SensorResult{}, err
		}
		if linesRead >= maxLines {
			break
		}
	}

	metrics.UniqueRatioPct = calculateRatio(metrics.UniquePayloads, metrics.TotalPayloads)
	examples.TopDuplicatePayload = topDuplicatePayload(payloadCounts)
	if metrics.TotalPayloads == 0 {
		examples.Note = "no payload for date"
	}
	if cfg.Debug {
		fmt.Printf("sensor=%s lines=%d payloads=%d\n", sensorID, metrics.Lines, metrics.TotalPayloads)
	}
	status := evaluateStatus(metrics, cfg.StatusThresholds)

	return SensorResult{
		SensorID:   sensorID,
		SensorType: sensorType,
		Status:     status,
		Metrics:    metrics,
		Examples:   examples,
	}, nil
}

func findSensorDirs(root string, includeGlobs, excludeDirs []string) ([]string, error) {
	if root == "" {
		return nil, errors.New("log_root is required")
	}
	if len(includeGlobs) == 0 {
		includeGlobs = []string{"GATE*", "WLS*", "PUMP*", "TEMP*"}
	}

	exclude := map[string]struct{}{}
	for _, name := range excludeDirs {
		exclude[strings.ToLower(name)] = struct{}{}
	}

	var dirs []string
	for _, pattern := range includeGlobs {
		matches, err := filepath.Glob(filepath.Join(root, pattern))
		if err != nil {
			return nil, err
		}
		for _, match := range matches {
			info, err := os.Stat(match)
			if err != nil || !info.IsDir() {
				continue
			}
			base := strings.ToLower(filepath.Base(match))
			if _, skip := exclude[base]; skip {
				continue
			}
			dirs = append(dirs, match)
		}
	}

	sort.Strings(dirs)
	return dirs, nil
}

func normalizeDatePrefix(date string) (string, error) {
	if len(date) != 8 {
		return "", fmt.Errorf("invalid date: %s", date)
	}
	return fmt.Sprintf("%s-%s-%s", date[:4], date[4:6], date[6:]), nil
}

func sensorTypeFromID(sensorID string) string {
	upper := strings.ToUpper(sensorID)
	switch {
	case strings.HasPrefix(upper, "GATE"):
		return "GATE"
	case strings.HasPrefix(upper, "WLS"):
		return "WLS"
	case strings.HasPrefix(upper, "PUMP"):
		return "PUMP"
	case strings.HasPrefix(upper, "TEMP"):
		return "TEMP"
	default:
		return ""
	}
}

func hasNoResponse(line string) bool {
	lower := strings.ToLower(line)
	if strings.Contains(lower, "no response") {
		return true
	}
	if strings.Contains(lower, "응답없음") || strings.Contains(lower, "응답 없음") {
		return true
	}
	return false
}

func extractPayload(line string) (string, bool) {
	idx := strings.Index(strings.ToLower(line), "rcv:")
	if idx == -1 {
		return "", false
	}
	payload := strings.TrimSpace(line[idx+4:])
	if payload == "" {
		return "", false
	}
	return payload, true
}

func isZeroPayload(payload string) bool {
	clean := strings.Trim(payload, "()[]{} ")
	clean = strings.ReplaceAll(clean, ",", " ")
	parts := strings.Fields(clean)
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		part = strings.TrimPrefix(strings.ToLower(part), "0x")
		if strings.Trim(part, "0") != "" {
			return false
		}
	}
	return true
}

func calculateRatio(unique, total int) *float64 {
	if total == 0 {
		return nil
	}
	value := float64(unique) / float64(total) * 100
	return &value
}

func topDuplicatePayload(counts map[string]int) string {
	max := 0
	var top string
	for payload, count := range counts {
		if count > max {
			max = count
			top = payload
		}
	}
	return top
}

func evaluateStatus(metrics Metrics, thresholds StatusThresholds) string {
	if metrics.Timeout >= thresholds.ErrorTimeout ||
		metrics.NoResponse >= thresholds.ErrorNoResponse ||
		metrics.ZeroData >= thresholds.ErrorZeroData ||
		metrics.Duplicates >= thresholds.ErrorDuplicates {
		return "ERROR"
	}
	if metrics.Timeout >= thresholds.WarningTimeout ||
		metrics.NoResponse >= thresholds.WarningNoResponse ||
		metrics.ZeroData >= thresholds.WarningZeroData ||
		metrics.Duplicates >= thresholds.WarningDuplicates {
		return "WARNING"
	}
	return "NORMAL"
}

func withDefaultThresholds(cfg Config) Config {
	if cfg.StatusThresholds.ErrorTimeout == 0 {
		cfg.StatusThresholds.ErrorTimeout = 3
	}
	if cfg.StatusThresholds.ErrorNoResponse == 0 {
		cfg.StatusThresholds.ErrorNoResponse = 3
	}
	if cfg.StatusThresholds.ErrorZeroData == 0 {
		cfg.StatusThresholds.ErrorZeroData = 10
	}
	if cfg.StatusThresholds.ErrorDuplicates == 0 {
		cfg.StatusThresholds.ErrorDuplicates = 50
	}
	if cfg.StatusThresholds.WarningTimeout == 0 {
		cfg.StatusThresholds.WarningTimeout = 1
	}
	if cfg.StatusThresholds.WarningNoResponse == 0 {
		cfg.StatusThresholds.WarningNoResponse = 1
	}
	if cfg.StatusThresholds.WarningZeroData == 0 {
		cfg.StatusThresholds.WarningZeroData = 1
	}
	if cfg.StatusThresholds.WarningDuplicates == 0 {
		cfg.StatusThresholds.WarningDuplicates = 10
	}
	return cfg
}

func buildTopIssues(results []SensorResult) []TopIssue {
	var issues []TopIssue
	for _, result := range results {
		metrics := result.Metrics
		if metrics.Timeout > 0 {
			issues = append(issues, TopIssue{Type: "timeout", SensorID: result.SensorID, Count: metrics.Timeout})
		}
		if metrics.NoResponse > 0 {
			issues = append(issues, TopIssue{Type: "no_response", SensorID: result.SensorID, Count: metrics.NoResponse})
		}
		if metrics.ZeroData > 0 {
			issues = append(issues, TopIssue{Type: "zero_data", SensorID: result.SensorID, Count: metrics.ZeroData})
		}
		if metrics.Duplicates > 0 {
			issues = append(issues, TopIssue{Type: "duplicates", SensorID: result.SensorID, Count: metrics.Duplicates})
		}
	}

	sort.Slice(issues, func(i, j int) bool {
		if issues[i].Count == issues[j].Count {
			return issues[i].SensorID < issues[j].SensorID
		}
		return issues[i].Count > issues[j].Count
	})
	if len(issues) > 5 {
		issues = issues[:5]
	}
	return issues
}

func analyzeLines(lines []string, datePrefix string, cfg Config) (Metrics, Examples) {
	metrics := Metrics{}
	examples := Examples{}
	payloadCounts := map[string]int{}
	var lastPayload string
	consecutive := 0
	for _, line := range lines {
		if !strings.HasPrefix(line, datePrefix) {
			continue
		}
		metrics, examples, lastPayload, consecutive = updateMetrics(metrics, examples, line, cfg, payloadCounts, lastPayload, consecutive)
	}
	metrics.UniqueRatioPct = calculateRatio(metrics.UniquePayloads, metrics.TotalPayloads)
	examples.TopDuplicatePayload = topDuplicatePayload(payloadCounts)
	if metrics.TotalPayloads == 0 {
		examples.Note = "no payload for date"
	}
	return metrics, examples
}

func updateMetrics(metrics Metrics, examples Examples, line string, cfg Config, payloadCounts map[string]int, lastPayload string, consecutive int) (Metrics, Examples, string, int) {
	metrics.Lines++
	lower := strings.ToLower(line)
	if strings.Contains(lower, "timeout") {
		metrics.Timeout++
		if examples.FirstTimeoutLine == "" {
			examples.FirstTimeoutLine = line
		}
	}
	if hasNoResponse(line) {
		metrics.NoResponse++
		if examples.FirstNoResponseLine == "" {
			examples.FirstNoResponseLine = line
		}
	}

	payload, ok := extractPayload(line)
	if ok {
		metrics.TotalPayloads++
		payloadCounts[payload]++
		if payloadCounts[payload] == 1 {
			metrics.UniquePayloads++
		}
		if isZeroPayload(payload) {
			metrics.ZeroData++
			if examples.FirstZeroDataLine == "" {
				examples.FirstZeroDataLine = line
			}
		}

		if payload == lastPayload {
			consecutive++
			if consecutive >= cfg.DuplicateRunThreshold {
				metrics.Duplicates++
			}
		} else {
			lastPayload = payload
			consecutive = 1
		}
	} else {
		lastPayload = ""
		consecutive = 0
	}

	return metrics, examples, lastPayload, consecutive
}

type fileSelectionNotes struct {
	usedFallback bool
}

func selectFiles(entries []os.DirEntry, dir string, datePrefix string, fallback bool) ([]string, fileSelectionNotes, error) {
	dateToken := datePrefix
	var matched []string
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		files = append(files, path)
		if strings.Contains(entry.Name(), dateToken) {
			matched = append(matched, path)
		}
	}
	sort.Strings(matched)
	if len(matched) > 0 {
		return matched, fileSelectionNotes{}, nil
	}
	if !fallback || len(files) == 0 {
		return nil, fileSelectionNotes{}, nil
	}
	latest, err := latestFile(files)
	if err != nil {
		return nil, fileSelectionNotes{}, err
	}
	return []string{latest}, fileSelectionNotes{usedFallback: true}, nil
}

func latestFile(files []string) (string, error) {
	var latest string
	var latestTime time.Time
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		if latest == "" || info.ModTime().After(latestTime) {
			latest = path
			latestTime = info.ModTime()
		}
	}
	if latest == "" {
		return "", errors.New("no files available")
	}
	return latest, nil
}
