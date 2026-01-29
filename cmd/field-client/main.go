package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"workfield/internal/analyzer"
)

type Config struct {
	SiteID                string   `json:"site_id"`
	DeviceID              string   `json:"device_id"`
	OutboxDir             string   `json:"outbox_dir"`
	LogRoot               string   `json:"log_root"`
	IncludeGlobs          []string `json:"include_globs"`
	ExcludeDirs           []string `json:"exclude_dirs"`
	DuplicateRunThreshold int      `json:"duplicate_run_threshold"`
	FallbackToLatestFile  *bool    `json:"fallback_to_latest_file"`
	Debug                 bool     `json:"debug"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "expected subcommand: analyze-daily")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "analyze-daily":
		runAnalyzeDaily(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand")
		os.Exit(2)
	}
}

func runAnalyzeDaily(args []string) {
	fs := flag.NewFlagSet("analyze-daily", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "config file path")
	dateStr := fs.String("date", "", "date in YYYYMMDD")
	logRoot := fs.String("log-root", "", "log root directory")
	maxLines := fs.Int("max-lines", 5000, "max lines per sensor")
	fs.Parse(args)

	if *dateStr == "" {
		fatal(errors.New("--date is required (YYYYMMDD)"))
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}
	if *logRoot != "" {
		cfg.LogRoot = *logRoot
	}
	if cfg.LogRoot == "" {
		fatal(errors.New("log_root is required"))
	}
	if cfg.OutboxDir == "" {
		fatal(errors.New("outbox_dir is required"))
	}

	fallback := true
	if cfg.FallbackToLatestFile != nil {
		fallback = *cfg.FallbackToLatestFile
	}

	analysisConfig := analyzer.Config{
		SiteID:                cfg.SiteID,
		DeviceID:              cfg.DeviceID,
		OutboxDir:             cfg.OutboxDir,
		LogRoot:               cfg.LogRoot,
		IncludeGlobs:          cfg.IncludeGlobs,
		ExcludeDirs:           cfg.ExcludeDirs,
		DuplicateRunThreshold: cfg.DuplicateRunThreshold,
		FallbackToLatestFile:  fallback,
		Debug:                 cfg.Debug,
	}

	summary, err := analyzer.AnalyzeDaily(analysisConfig, *dateStr, *maxLines)
	if err != nil {
		fatal(err)
	}

	outDir := filepath.Join(cfg.OutboxDir, "daily", *dateStr)
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fatal(err)
	}
	outputPath := filepath.Join(outDir, "analysis.json")
	if err := writeJSON(outputPath, summary); err != nil {
		fatal(err)
	}

	fmt.Printf("wrote %s\n", outputPath)
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func writeJSON(path string, data any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
