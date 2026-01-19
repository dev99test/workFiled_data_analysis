package analyzer

import "testing"

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
