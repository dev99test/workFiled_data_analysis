package main

import (
	"archive/zip"
	"bufio"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Manifest struct {
	Files map[string]ManifestEntry `json:"files"`
}

type ManifestEntry struct {
	SHA256 string `json:"sha256"`
	Lines  int    `json:"lines"`
}

type SensorMapping struct {
	SensorID  string  `json:"sensor_id"`
	Type      string  `json:"type"`
	Field     string  `json:"field"`
	JSONType  string  `json:"json_type"`
	Tolerance float64 `json:"tolerance"`
}

type SnapshotEnvelope struct {
	CapturedAt string          `json:"captured_at"`
	WorkField  string          `json:"work_field"`
	Payload    json.RawMessage `json:"payload"`
}

type SensorPayload struct {
	PublishAt string           `json:"PublishAt"`
	Time      string           `json:"time"`
	WorkField string           `json:"work_field"`
	Cmd       string           `json:"cmd"`
	Data      []SensorDataItem `json:"data"`
}

type SensorDataItem struct {
	ID       int             `json:"id"`
	Value    json.RawMessage `json:"value"`
	Ping     json.RawMessage `json:"ping"`
	Position json.RawMessage `json:"position"`
	Type     string          `json:"type"`
}

type RawObservation struct {
	Timestamp time.Time
	Value     string
	Evidence  string
}

func main() {
	fs := flag.NewFlagSet("field-ingest-worker", flag.ExitOnError)
	incoming := fs.String("incoming", "/srv/field-ingest/incoming", "incoming directory")
	workDir := fs.String("work", "/srv/field-ingest/work", "work directory")
	doneDir := fs.String("done", "/srv/field-ingest/done", "done directory")
	dbPath := fs.String("db", "/srv/field-ingest/db/field_metrics.sqlite3", "sqlite database path")
	mappingPath := fs.String("mapping", "mapping.json", "sensor mapping json")
	windowSeconds := fs.Int("window", 3, "comparison window in seconds")
	fs.Parse(os.Args[1:])

	mapping, err := loadMapping(*mappingPath)
	if err != nil {
		fatal(err)
	}

	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(*doneDir, 0o755); err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*dbPath), 0o755); err != nil {
		fatal(err)
	}

	db, err := sql.Open("sqlite", *dbPath)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	if err := initSchema(db); err != nil {
		fatal(err)
	}

	zips, err := listZipFiles(*incoming)
	if err != nil {
		fatal(err)
	}

	for _, zipPath := range zips {
		if err := processZip(zipPath, *workDir, *doneDir, db, mapping, time.Duration(*windowSeconds)*time.Second); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

func listZipFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var zips []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".partial") || !strings.HasSuffix(name, ".zip") {
			continue
		}
		zips = append(zips, filepath.Join(dir, name))
	}
	sort.Strings(zips)
	return zips, nil
}

func processZip(zipPath, workDir, doneDir string, db *sql.DB, mapping map[string]SensorMapping, window time.Duration) error {
	zipBase := strings.TrimSuffix(filepath.Base(zipPath), filepath.Ext(zipPath))
	workPath := filepath.Join(workDir, zipBase)
	if err := os.RemoveAll(workPath); err != nil {
		return err
	}
	if err := os.MkdirAll(workPath, 0o755); err != nil {
		return err
	}

	if err := unzip(zipPath, workPath); err != nil {
		return err
	}

	manifestPath := filepath.Join(workPath, "manifest.json")
	if err := verifyManifest(manifestPath, workPath); err != nil {
		return err
	}

	siteID, deviceID, err := parseZipName(zipBase)
	if err != nil {
		return err
	}

	ingestFile := filepath.Base(zipPath)
	eventsPath := filepath.Join(workPath, "events.jsonl")
	if err := ingestEvents(db, eventsPath, siteID, deviceID, ingestFile); err != nil {
		return err
	}

	sensorPath := filepath.Join(workPath, "sensor_data.jsonl")
	snapshots, err := ingestSnapshots(db, sensorPath, siteID, deviceID, ingestFile)
	if err != nil {
		return err
	}

	rawDir := filepath.Join(workPath, "raw_session")
	rawObservations, err := loadRawObservations(rawDir, mapping)
	if err != nil {
		return err
	}

	if err := compareSnapshots(db, snapshots, rawObservations, mapping, window, ingestFile, siteID, deviceID); err != nil {
		return err
	}

	donePath := filepath.Join(doneDir, filepath.Base(zipPath))
	if err := os.Rename(zipPath, donePath); err != nil {
		return err
	}
	return nil
}

func unzip(zipPath, dest string) error {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer reader.Close()

	cleanDest := filepath.Clean(dest) + string(os.PathSeparator)
	for _, file := range reader.File {
		path := filepath.Join(dest, file.Name)
		cleanPath := filepath.Clean(path)
		if !strings.HasPrefix(cleanPath, cleanDest) {
			return fmt.Errorf("invalid zip path: %s", file.Name)
		}
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := writeZipFile(file, path); err != nil {
			return err
		}
	}
	return nil
}

func writeZipFile(file *zip.File, path string) error {
	src, err := file.Open()
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(path)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}

func verifyManifest(manifestPath, workPath string) error {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return err
	}
	for name, entry := range manifest.Files {
		path := filepath.Join(workPath, name)
		fileEntry, err := buildManifestEntry(path)
		if err != nil {
			return err
		}
		if entry.SHA256 != fileEntry.SHA256 || entry.Lines != fileEntry.Lines {
			return fmt.Errorf("manifest mismatch for %s", name)
		}
	}
	return nil
}

func buildManifestEntry(path string) (ManifestEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return ManifestEntry{}, err
	}
	defer file.Close()

	hasher := sha256.New()
	lines := 0
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			lines++
			if _, err := hasher.Write(line); err != nil {
				return ManifestEntry{}, err
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ManifestEntry{}, err
		}
	}

	return ManifestEntry{SHA256: hex.EncodeToString(hasher.Sum(nil)), Lines: lines}, nil
}

func parseZipName(base string) (string, string, error) {
	parts := strings.Split(base, "_")
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid zip name: %s", base)
	}
	return parts[0], parts[1], nil
}

func ingestEvents(db *sql.DB, path, siteID, deviceID, ingestFile string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stmt, err := db.Prepare(`
		INSERT OR IGNORE INTO hourly_metrics
		(site_id, device_id, work_field, hour, payload_json, ingest_file, ingested_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			continue
		}
		workField, _ := payload["work_field"].(string)
		hour, _ := payload["hour"].(string)
		ingestedAt := time.Now().Format(time.RFC3339Nano)
		if _, err := stmt.Exec(siteID, deviceID, workField, hour, line, ingestFile, ingestedAt); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func ingestSnapshots(db *sql.DB, path, siteID, deviceID, ingestFile string) ([]SnapshotEnvelope, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stmt, err := db.Prepare(`
		INSERT OR IGNORE INTO sensor_data_snapshots
		(site_id, device_id, work_field, publish_at, payload_json, ingest_file, ingested_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var snapshots []SnapshotEnvelope
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var snapshot SnapshotEnvelope
		if err := json.Unmarshal([]byte(line), &snapshot); err != nil {
			continue
		}
		publishAt := extractPublishAt(snapshot.Payload)
		ingestedAt := time.Now().Format(time.RFC3339Nano)
		if _, err := stmt.Exec(siteID, deviceID, snapshot.WorkField, publishAt, string(snapshot.Payload), ingestFile, ingestedAt); err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return snapshots, nil
}

func extractPublishAt(payload json.RawMessage) string {
	var data SensorPayload
	if err := json.Unmarshal(payload, &data); err != nil {
		return ""
	}
	if data.PublishAt != "" {
		return data.PublishAt
	}
	return data.Time
}

func loadRawObservations(dir string, mapping map[string]SensorMapping) (map[string][]RawObservation, error) {
	observations := map[string][]RawObservation{}
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		return observations, nil
	}

	return observations, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		sensorID := matchSensorID(path, mapping)
		if sensorID == "" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			timestamp, value, ok := parseRawLine(mapping[sensorID].Type, line)
			if !ok {
				continue
			}
			evidence := clipEvidence(line)
			observations[sensorID] = append(observations[sensorID], RawObservation{Timestamp: timestamp, Value: value, Evidence: evidence})
		}
		return scanner.Err()
	})
}

func matchSensorID(path string, mapping map[string]SensorMapping) string {
	lower := strings.ToLower(path)
	for _, entry := range mapping {
		if entry.SensorID == "" {
			continue
		}
		if strings.Contains(lower, strings.ToLower(entry.SensorID)) {
			return entry.SensorID
		}
	}
	return ""
}

func parseRawLine(sensorType, line string) (time.Time, string, bool) {
	if len(line) < len("2006-01-02 15:04:05.000") {
		return time.Time{}, "", false
	}
	stamp := strings.TrimSpace(line[:23])
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000", stamp, time.Local)
	if err != nil {
		return time.Time{}, "", false
	}
	value := extractRawValue(sensorType, line)
	if value == "" {
		return time.Time{}, "", false
	}
	return parsed, value, true
}

func extractRawValue(sensorType, line string) string {
	lower := strings.ToLower(line)
	if idx := strings.Index(lower, "rcv:"); idx != -1 {
		return strings.TrimSpace(line[idx+4:])
	}
	if idx := strings.Index(lower, "status"); idx != -1 {
		return strings.TrimSpace(line[idx:])
	}
	if idx := strings.Index(lower, "snd:"); idx != -1 {
		return strings.TrimSpace(line[idx+4:])
	}
	return ""
}

func clipEvidence(line string) string {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) > 200 {
		return trimmed[:200]
	}
	return trimmed
}

func compareSnapshots(db *sql.DB, snapshots []SnapshotEnvelope, rawObservations map[string][]RawObservation, mapping map[string]SensorMapping, window time.Duration, ingestFile, siteID, deviceID string) error {
	stmt, err := db.Prepare(`
		INSERT OR IGNORE INTO comparison_results
		(site_id, device_id, work_field, publish_at, sensor_id, sensor_type, field_name, sent_value, raw_value, result, raw_evidence, ingest_file, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, snapshot := range snapshots {
		payload, publishAt, err := parsePayload(snapshot.Payload)
		if err != nil {
			continue
		}
		workField := payload.WorkField
		if workField == "" {
			workField = snapshot.WorkField
		}
		publishTime := publishAt
		for id, entry := range mapping {
			sentValue, ok := findSentValue(payload, id, entry)
			rawValue, rawEvidence, rawFound := findRawValue(entry.SensorID, rawObservations, publishTime, window)
			result := compareValues(sentValue, rawValue, ok, rawFound, entry)
			createdAt := time.Now().Format(time.RFC3339Nano)
			if _, err := stmt.Exec(siteID, deviceID, workField, publishTime.Format(time.RFC3339Nano), entry.SensorID, entry.Type, entry.Field, sentValue, rawValue, result, rawEvidence, ingestFile, createdAt); err != nil {
				return err
			}
		}
	}
	return nil
}

func parsePayload(payloadRaw json.RawMessage) (SensorPayloadContext, time.Time, error) {
	var payload SensorPayload
	if err := json.Unmarshal(payloadRaw, &payload); err != nil {
		return SensorPayloadContext{}, time.Time{}, err
	}
	publishAt := payload.PublishAt
	if publishAt == "" {
		publishAt = payload.Time
	}
	timestamp, err := parseTimestamp(publishAt)
	if err != nil {
		return SensorPayloadContext{}, time.Time{}, err
	}

	return SensorPayloadContext{
		WorkField: payload.WorkField,
		Data:      payload.Data,
	}, timestamp, nil
}

type SensorPayloadContext struct {
	WorkField string
	Data      []SensorDataItem
}

func parseTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, errors.New("missing timestamp")
	}
	layouts := []string{time.RFC3339Nano, "2006-01-02 15:04:05.000"}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, value, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp: %s", value)
}

func findSentValue(payload SensorPayloadContext, id string, entry SensorMapping) (string, bool) {
	idInt, err := strconv.Atoi(id)
	if err != nil {
		return "", false
	}
	for _, item := range payload.Data {
		if item.ID != idInt {
			continue
		}
		if entry.JSONType != "" && !strings.EqualFold(entry.JSONType, item.Type) {
			continue
		}
		switch entry.Field {
		case "ping":
			return normalizeValue(item.Ping), true
		case "position":
			return normalizeValue(item.Position), true
		default:
			return normalizeValue(item.Value), true
		}
	}
	return "", false
}

func normalizeValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return strings.TrimSpace(string(raw))
	}
	switch v := value.(type) {
	case string:
		return strings.ToLower(strings.TrimSpace(v))
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", v), "0"), ".")
	case bool:
		return strings.ToLower(strconv.FormatBool(v))
	default:
		return strings.ToLower(strings.TrimSpace(string(raw)))
	}
}

func findRawValue(sensorID string, observations map[string][]RawObservation, target time.Time, window time.Duration) (string, string, bool) {
	obs := observations[sensorID]
	if len(obs) == 0 {
		return "", "", false
	}
	start := target.Add(-window)
	end := target.Add(window)
	var selected RawObservation
	found := false
	for _, item := range obs {
		if item.Timestamp.Before(start) || item.Timestamp.After(end) {
			continue
		}
		selected = item
		found = true
	}
	if !found {
		return "", "", false
	}
	return normalizeText(selected.Value), selected.Evidence, true
}

func normalizeText(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.ToLower(trimmed)
	trimmed = strings.ReplaceAll(trimmed, " ", "")
	return trimmed
}

func compareValues(sentValue, rawValue string, sentFound, rawFound bool, entry SensorMapping) string {
	if !sentFound {
		return "MISSING_SENT"
	}
	if sentFound && !rawFound {
		return "MISSING_RAW"
	}
	if entry.Tolerance > 0 {
		sentNum, sentErr := strconv.ParseFloat(sentValue, 64)
		rawNum, rawErr := strconv.ParseFloat(rawValue, 64)
		if sentErr == nil && rawErr == nil {
			if absFloat(sentNum-rawNum) <= entry.Tolerance {
				return "MATCH"
			}
			return "MISMATCH"
		}
	}
	if normalizeText(sentValue) == normalizeText(rawValue) {
		return "MATCH"
	}
	return "MISMATCH"
}

func absFloat(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}

func initSchema(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS hourly_metrics (
		id INTEGER PRIMARY KEY,
		site_id TEXT,
		device_id TEXT,
		work_field TEXT,
		hour TEXT,
		payload_json TEXT,
		ingest_file TEXT,
		ingested_at TEXT,
		UNIQUE(site_id, device_id, work_field, hour, ingest_file)
	);
	CREATE TABLE IF NOT EXISTS sensor_data_snapshots (
		id INTEGER PRIMARY KEY,
		site_id TEXT,
		device_id TEXT,
		work_field TEXT,
		publish_at TEXT,
		payload_json TEXT,
		ingest_file TEXT,
		ingested_at TEXT,
		UNIQUE(site_id, device_id, publish_at, work_field)
	);
	CREATE TABLE IF NOT EXISTS comparison_results (
		id INTEGER PRIMARY KEY,
		site_id TEXT,
		device_id TEXT,
		work_field TEXT,
		publish_at TEXT,
		sensor_id TEXT,
		sensor_type TEXT,
		field_name TEXT,
		sent_value TEXT,
		raw_value TEXT,
		result TEXT,
		raw_evidence TEXT,
		ingest_file TEXT,
		created_at TEXT,
		UNIQUE(site_id, device_id, work_field, publish_at, sensor_id, field_name)
	);
	`
	_, err := db.Exec(schema)
	return err
}

func loadMapping(path string) (map[string]SensorMapping, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	mapping := map[string]SensorMapping{}
	if err := json.Unmarshal(data, &mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
