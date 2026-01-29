package analyzer

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
}

type Metrics struct {
	Lines          int       `json:"-"`
	Timeout        int       `json:"timeout"`
	NoResponse     int       `json:"no_response"`
	ZeroData       int       `json:"zero_data"`
	Duplicates     int       `json:"duplicates"`
	TimeRange      TimeRange `json:"time_range"`
	SndCount       int       `json:"snd_count"`
	RcvCount       int       `json:"rcv_count"`
	WLSLastValueCm *int      `json:"wls_last_value_cm,omitempty"`
	WLSMinValueCm  *int      `json:"wls_min_value_cm,omitempty"`
	WLSMaxValueCm  *int      `json:"wls_max_value_cm,omitempty"`
	TotalPayloads  int       `json:"-"`
	UniquePayloads int       `json:"-"`
}

type Examples struct {
	FirstTimeoutLine    string `json:"first_timeout_line,omitempty"`
	FirstNoResponseLine string `json:"first_no_response_line,omitempty"`
	FirstZeroDataLine   string `json:"first_zero_data_line,omitempty"`
	TopDuplicatePayload string `json:"top_duplicate_payload,omitempty"`
	ZeroDataPayload     string `json:"zero_data_payload,omitempty"`
	Note                string `json:"note,omitempty"`
}

type TimeRange struct {
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
}

type SensorResult struct {
	SensorID   string   `json:"sensor_id"`
	SensorType string   `json:"sensor_type"`
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
	state := SensorState{}
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
			trimmed := strings.TrimLeft(line, " \t")
			if !strings.HasPrefix(trimmed, datePrefix) {
				continue
			}
			linesRead++
			metrics, examples, lastPayload, consecutive, state = updateMetrics(metrics, examples, trimmed, sensorType, cfg, payloadCounts, lastPayload, consecutive, state)
		}
		file.Close()
		if err := scanner.Err(); err != nil {
			return SensorResult{}, err
		}
		if linesRead >= maxLines {
			break
		}
	}

	metrics, examples = finalizeMetrics(metrics, examples, state, payloadCounts, datePrefix)
	if cfg.Debug {
		fmt.Printf("sensor=%s lines=%d payloads=%d\n", sensorID, metrics.Lines, metrics.TotalPayloads)
	}

	return SensorResult{
		SensorID:   sensorID,
		SensorType: sensorType,
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
	if len(excludeDirs) == 0 {
		excludeDirs = []string{"ALL", "PING"}
	}
	for _, name := range excludeDirs {
		exclude[strings.ToLower(name)] = struct{}{}
	}
	exclude["server"] = struct{}{}

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
			if base == "server" {
				continue
			}
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
		return "", fmt.Errorf("invalid date %q: expected YYYYMMDD", date)
	}
	if _, err := time.Parse("20060102", date); err != nil {
		return "", fmt.Errorf("invalid date %q: expected YYYYMMDD", date)
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

func analyzeLines(lines []string, datePrefix string, sensorType string, cfg Config) (Metrics, Examples) {
	metrics := Metrics{}
	examples := Examples{}
	payloadCounts := map[string]int{}
	state := SensorState{}
	var lastPayload string
	consecutive := 0
	for _, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		if !strings.HasPrefix(trimmed, datePrefix) {
			continue
		}
		metrics, examples, lastPayload, consecutive, state = updateMetrics(metrics, examples, trimmed, sensorType, cfg, payloadCounts, lastPayload, consecutive, state)
	}
	return finalizeMetrics(metrics, examples, state, payloadCounts, datePrefix)
}

func updateMetrics(metrics Metrics, examples Examples, line string, sensorType string, cfg Config, payloadCounts map[string]int, lastPayload string, consecutive int, state SensorState) (Metrics, Examples, string, int, SensorState) {
	metrics.Lines++
	trimmed := strings.TrimLeft(line, " \t")
	lower := strings.ToLower(trimmed)
	lineTime, hasTime := parseLineTime(trimmed)
	if strings.Contains(lower, "timeout") {
		metrics.Timeout++
		if examples.FirstTimeoutLine == "" {
			examples.FirstTimeoutLine = line
		}
	}
	if hasTime && strings.Contains(lower, "snd:") {
		state = updateTimeRange(state, lineTime)
		state.PendingSentAt = lineTime
		state.PendingLine = metrics.Lines
		state.HasPending = true
		state.SndCount++
	}

	if hasTime && strings.Contains(lower, "rcv:") {
		state = updateTimeRange(state, lineTime)
		state.RcvCount++
		state.HasPending = false
	}

	payload, ok := extractPayload(trimmed)
	if ok {
		metrics.TotalPayloads++
		isValid, isZero := validateWLSFrame(payload, sensorType)
		if isZero {
			metrics.ZeroData++
			if examples.FirstZeroDataLine == "" {
				examples.FirstZeroDataLine = line
			}
			if examples.ZeroDataPayload == "" {
				examples.ZeroDataPayload = payload
			}
		}
		if isValid {
			payloadCounts[payload]++
			if payloadCounts[payload] == 1 {
				metrics.UniquePayloads++
			}
		}
		if !isZero && isZeroPayload(payload) {
			metrics.ZeroData++
			if examples.FirstZeroDataLine == "" {
				examples.FirstZeroDataLine = line
			}
			if examples.ZeroDataPayload == "" {
				examples.ZeroDataPayload = payload
			}
		}

		if payload == lastPayload && isValid {
			consecutive++
			if consecutive >= cfg.DuplicateRunThreshold {
				metrics.Duplicates++
			}
		} else {
			lastPayload = payload
			consecutive = 1
		}
		if strings.EqualFold(sensorType, "WLS") && isValid && !isZero {
			if value, ok := parseWLSValue(payload); ok {
				state.WLSLast = &value
				if state.WLSMin == nil || value < *state.WLSMin {
					state.WLSMin = &value
				}
				if state.WLSMax == nil || value > *state.WLSMax {
					state.WLSMax = &value
				}
			}
		}
	} else {
		lastPayload = ""
		consecutive = 0
	}

	return metrics, examples, lastPayload, consecutive, state
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

type SensorState struct {
	PendingSentAt  time.Time
	PendingLine    int
	HasPending     bool
	TimeRangeStart time.Time
	TimeRangeEnd   time.Time
	HasTimeRange   bool
	SndCount       int
	RcvCount       int
	WLSLast        *int
	WLSMin         *int
	WLSMax         *int
}

func finalizeMetrics(metrics Metrics, examples Examples, state SensorState, payloadCounts map[string]int, datePrefix string) (Metrics, Examples) {
	if state.HasTimeRange {
		metrics.TimeRange = TimeRange{
			From: state.TimeRangeStart.Format(time.RFC3339),
			To:   state.TimeRangeEnd.Format(time.RFC3339),
		}
	} else {
		if metrics.Lines > 0 {
			estimated, ok := estimateRangeFromDate(datePrefix)
			if ok {
				metrics.TimeRange = estimated
				if examples.Note == "" {
					examples.Note = "time_range estimated from filename"
				}
			}
		} else if examples.Note == "" {
			examples.Note = "no timestamps found for date"
		}
	}
	metrics.SndCount = state.SndCount
	metrics.RcvCount = state.RcvCount
	if state.SndCount > 0 && state.RcvCount == 0 {
		metrics.NoResponse = state.SndCount
		if examples.Note == "" {
			examples.Note = "snd exists but no rcv found; treated as no_response"
		}
	}
	examples.TopDuplicatePayload = topDuplicatePayload(payloadCounts)
	metrics.WLSLastValueCm = state.WLSLast
	metrics.WLSMinValueCm = state.WLSMin
	metrics.WLSMaxValueCm = state.WLSMax
	if metrics.TotalPayloads == 0 {
		if examples.Note == "" {
			examples.Note = "no payload for date"
		}
	}
	return metrics, examples
}

func parseLineTime(line string) (time.Time, bool) {
	if len(line) < len("2006-01-02 15:04:05.000") {
		return time.Time{}, false
	}
	trimmed := strings.TrimLeft(line, " \t")
	if len(trimmed) < len("2006-01-02 15:04:05.000") {
		return time.Time{}, false
	}
	value := trimmed[:23]
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000", value, time.Local)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func updateTimeRange(state SensorState, value time.Time) SensorState {
	if state.HasTimeRange {
		if value.Before(state.TimeRangeStart) {
			state.TimeRangeStart = value
		}
		if value.After(state.TimeRangeEnd) {
			state.TimeRangeEnd = value
		}
	} else {
		state.TimeRangeStart = value
		state.TimeRangeEnd = value
		state.HasTimeRange = true
	}
	return state
}

func estimateRangeFromDate(datePrefix string) (TimeRange, bool) {
	parsed, err := time.ParseInLocation("2006-01-02", datePrefix, time.Local)
	if err != nil {
		return TimeRange{}, false
	}
	start := parsed
	end := parsed.Add(23*time.Hour + 59*time.Minute + 59*time.Second)
	return TimeRange{
		From: start.Format(time.RFC3339),
		To:   end.Format(time.RFC3339),
	}, true
}

func parseWLSValue(payload string) (int, bool) {
	bytes, ok := parsePayloadBytes(payload)
	if !ok {
		return 0, false
	}
	if len(bytes) < 6 {
		return 0, false
	}
	value := int(bytes[4])<<8 + int(bytes[5])
	if value > 96 {
		return 0, false
	}
	return value, true
}

func validateWLSFrame(payload string, sensorType string) (bool, bool) {
	if !strings.EqualFold(sensorType, "WLS") {
		return true, false
	}
	bytes, ok := parsePayloadBytes(payload)
	if !ok || len(bytes) != 11 {
		return false, true
	}
	if bytes[0] != 0xFA || bytes[len(bytes)-1] != 0x76 {
		return false, true
	}
	return true, false
}

func parsePayloadBytes(payload string) ([]byte, bool) {
	clean := strings.Trim(payload, "()[]{} ")
	if clean == "" {
		return nil, false
	}
	parts := strings.FieldsFunc(clean, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	if len(parts) == 0 {
		return nil, false
	}
	bytes := make([]byte, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(part), "0x"))
		if part == "" {
			continue
		}
		base := 10
		if strings.ContainsAny(part, "abcdef") || len(part) == 2 {
			base = 16
		}
		value, err := parseUint(part, base)
		if err != nil {
			return nil, false
		}
		bytes = append(bytes, byte(value))
	}
	if len(bytes) == 0 {
		return nil, false
	}
	return bytes, true
}

func parseUint(value string, base int) (uint64, error) {
	return strconv.ParseUint(value, base, 8)
}
