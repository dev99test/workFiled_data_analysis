package analyzer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDuplicateCounting(t *testing.T) {
	cfg := Config{DuplicateRunThreshold: 3}
	metrics, _ := analyzeLines([]string{
		"2026-01-19 00:00:01.000 rcv: (01)",
		"2026-01-19 00:00:02.000 rcv: (01)",
		"2026-01-19 00:00:03.000 rcv: (01)",
		"2026-01-19 00:00:04.000 rcv: (01)",
		"2026-01-19 00:00:05.000 rcv: (02)",
	}, "2026-01-19", cfg)

	if metrics.Duplicates != 2 {
		t.Fatalf("expected duplicates 2, got %d", metrics.Duplicates)
	}
}

func TestZeroDataAndTimeout(t *testing.T) {
	cfg := Config{DuplicateRunThreshold: 3}
	metrics, examples := analyzeLines([]string{
		"2026-01-19 00:00:01.000 timeout while reading",
		"2026-01-19 00:00:02.000 rcv: (00)",
		"2026-01-19 00:00:03.000 rcv: (00, 00, 00)",
	}, "2026-01-19", cfg)

	if metrics.Timeout != 1 {
		t.Fatalf("expected timeout 1, got %d", metrics.Timeout)
	}
	if metrics.ZeroData != 2 {
		t.Fatalf("expected zero_data 2, got %d", metrics.ZeroData)
	}
	if examples.FirstZeroDataLine == "" {
		t.Fatalf("expected first_zero_data_line to be set")
	}
}

func TestUniqueRatioNilWhenNoPayloads(t *testing.T) {
	cfg := Config{DuplicateRunThreshold: 3}
	metrics, examples := analyzeLines([]string{
		"2026-01-19 00:00:01.000 timeout while reading",
		"2026-01-19 00:00:02.000 STATUS OK",
	}, "2026-01-19", cfg)

	if metrics.UniqueRatioPct != nil {
		t.Fatalf("expected unique_ratio_pct to be nil when no payloads")
	}
	if examples.Note == "" {
		t.Fatalf("expected note when no payloads")
	}
}

func TestSelectFilesByDate(t *testing.T) {
	root := t.TempDir()
	sensorDir := filepath.Join(root, "WLS1")
	if err := os.MkdirAll(sensorDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	files := []string{
		filepath.Join(sensorDir, "2026-01-18.log"),
		filepath.Join(sensorDir, "2026-01-19.log"),
	}
	for _, path := range files {
		if err := os.WriteFile(path, []byte("2026-01-19 00:00:01.000 rcv: (01)\n"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	entries, err := os.ReadDir(sensorDir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	selected, _, err := selectFiles(entries, sensorDir, "2026-01-19", true)
	if err != nil {
		t.Fatalf("selectFiles: %v", err)
	}
	if len(selected) != 1 || !strings.Contains(selected[0], "2026-01-19") {
		t.Fatalf("expected only 2026-01-19 file, got %v", selected)
	}
}

func TestAnalyzeSensorDirFiltersByDate(t *testing.T) {
	root := t.TempDir()
	sensorDir := filepath.Join(root, "GATE1")
	if err := os.MkdirAll(sensorDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	path := filepath.Join(sensorDir, "2026-01-19.log")
	content := strings.Join([]string{
		"2026-01-19 00:00:01.000 rcv: (01)",
		"2026-01-18 23:59:59.000 rcv: (02)",
		"2026-01-19 00:00:02.000 timeout",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	result, err := analyzeSensorDir(sensorDir, "2026-01-19", 100, Config{FallbackToLatestFile: true, DuplicateRunThreshold: 3})
	if err != nil {
		t.Fatalf("analyzeSensorDir: %v", err)
	}
	if result.Metrics.Lines != 2 {
		t.Fatalf("expected 2 lines for date, got %d", result.Metrics.Lines)
	}
}
