package main

import (
	"archive/zip"
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	SiteID            string `json:"site_id"`
	DeviceID          string `json:"device_id"`
	WorkField         string `json:"work_field"`
	OutboxDir         string `json:"outbox_dir"`
	SensorDataLogPath string `json:"sensor_data_log_path"`
	RemoteHost        string `json:"remote_host"`
	RemoteUser        string `json:"remote_user"`
	RemotePath        string `json:"remote_path"`
}

type SensorDataRecord struct {
	CapturedAt string          `json:"captured_at"`
	WorkField  string          `json:"work_field"`
	Payload    json.RawMessage `json:"payload"`
}

type ManifestEntry struct {
	SHA256 string `json:"sha256"`
	Lines  int    `json:"lines"`
}

type Manifest struct {
	Files map[string]ManifestEntry `json:"files"`
}

type Meta struct {
	Version   string `json:"version"`
	CreatedAt string `json:"created_at"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "expected subcommand: collect-sensor-data, package-daily, upload-daily")
		os.Exit(2)
	}

	switch os.Args[1] {
	case "collect-sensor-data":
		runCollectSensorData(os.Args[2:])
	case "package-daily":
		runPackageDaily(os.Args[2:])
	case "upload-daily":
		runUploadDaily(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand")
		os.Exit(2)
	}
}

func runCollectSensorData(args []string) {
	fs := flag.NewFlagSet("collect-sensor-data", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "config file path")
	inputPath := fs.String("input", "", "input sensor data log path")
	dateStr := fs.String("date", "", "date in YYYYMMDD")
	outPath := fs.String("out", "", "output sensor_data.jsonl path")
	fs.Parse(args)

	if *dateStr == "" {
		fatal(errors.New("--date is required"))
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}
	if *inputPath == "" {
		*inputPath = cfg.SensorDataLogPath
	}
	if *inputPath == "" {
		fatal(errors.New("--input or config sensor_data_log_path is required"))
	}
	if *outPath == "" {
		*outPath = filepath.Join(cfg.OutboxDir, "snapshots", *dateStr, "sensor_data.jsonl")
	}

	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		fatal(err)
	}

	inFile, err := os.Open(*inputPath)
	if err != nil {
		fatal(err)
	}
	defer inFile.Close()

	outFile, err := os.OpenFile(*outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fatal(err)
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	scanner := bufio.NewScanner(inFile)
	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		capturedAt, payload, err := parseSensorDataLine(line)
		if err != nil {
			continue
		}
		if capturedAt.Format("20060102") != *dateStr {
			continue
		}
		record := SensorDataRecord{
			CapturedAt: capturedAt.Format(time.RFC3339Nano),
			WorkField:  cfg.WorkField,
			Payload:    payload,
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			continue
		}
		if _, err := writer.Write(append(encoded, '\n')); err != nil {
			fatal(err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		fatal(err)
	}

	fmt.Printf("wrote %d records to %s\n", count, *outPath)
}

func runPackageDaily(args []string) {
	fs := flag.NewFlagSet("package-daily", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "config file path")
	dateStr := fs.String("date", "", "date in YYYYMMDD")
	outZip := fs.String("out", "", "output zip path")
	rawDir := fs.String("raw-dir", "", "optional raw session directory to include")
	fs.Parse(args)

	if *dateStr == "" {
		fatal(errors.New("--date is required"))
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}

	snapshotDir := filepath.Join(cfg.OutboxDir, "snapshots", *dateStr)
	eventsPath := filepath.Join(snapshotDir, "events.jsonl")
	sensorPath := filepath.Join(snapshotDir, "sensor_data.jsonl")

	if *outZip == "" {
		zipName := fmt.Sprintf("%s_%s_%s.zip", cfg.SiteID, cfg.DeviceID, *dateStr)
		*outZip = filepath.Join(cfg.OutboxDir, "outgoing", zipName)
	}

	if err := os.MkdirAll(filepath.Dir(*outZip), 0o755); err != nil {
		fatal(err)
	}

	manifest := Manifest{Files: map[string]ManifestEntry{}}
	manifestEntry, err := buildManifestEntry(eventsPath)
	if err != nil {
		fatal(err)
	}
	manifest.Files["events.jsonl"] = manifestEntry
	manifestEntry, err = buildManifestEntry(sensorPath)
	if err != nil {
		fatal(err)
	}
	manifest.Files["sensor_data.jsonl"] = manifestEntry

	manifestPath := filepath.Join(snapshotDir, "manifest.json")
	if err := writeJSONFile(manifestPath, manifest); err != nil {
		fatal(err)
	}

	meta := Meta{Version: "1.0", CreatedAt: time.Now().Format(time.RFC3339Nano)}
	metaPath := filepath.Join(snapshotDir, "meta.json")
	if err := writeJSONFile(metaPath, meta); err != nil {
		fatal(err)
	}

	files := []string{eventsPath, sensorPath, manifestPath, metaPath}
	if err := createZip(*outZip, files, snapshotDir, *rawDir); err != nil {
		fatal(err)
	}

	fmt.Printf("created %s\n", *outZip)
}

func runUploadDaily(args []string) {
	fs := flag.NewFlagSet("upload-daily", flag.ExitOnError)
	configPath := fs.String("config", "config.json", "config file path")
	zipPath := fs.String("zip", "", "zip file to upload")
	fs.Parse(args)

	if *zipPath == "" {
		fatal(errors.New("--zip is required"))
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}
	if cfg.RemoteHost == "" || cfg.RemoteUser == "" || cfg.RemotePath == "" {
		fatal(errors.New("remote_host, remote_user, and remote_path are required in config"))
	}

	base := filepath.Base(*zipPath)
	partial := base + ".partial"
	remotePartial := fmt.Sprintf("%s@%s:%s/%s", cfg.RemoteUser, cfg.RemoteHost, cfg.RemotePath, partial)
	remoteFinal := fmt.Sprintf("%s/%s", cfg.RemotePath, base)

	if err := runCommand("scp", *zipPath, remotePartial); err != nil {
		fatal(err)
	}
	if err := runCommand("ssh", fmt.Sprintf("%s@%s", cfg.RemoteUser, cfg.RemoteHost), "mv", remoteFinal+".partial", remoteFinal); err != nil {
		fatal(err)
	}

	fmt.Printf("uploaded %s to %s\n", *zipPath, remoteFinal)
}

func parseSensorDataLine(line string) (time.Time, json.RawMessage, error) {
	idx := strings.Index(line, "{")
	if idx == -1 {
		return time.Time{}, nil, errors.New("missing json payload")
	}
	stamp := strings.TrimSpace(line[:idx])
	payload := strings.TrimSpace(line[idx:])
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000", stamp, time.Local)
	if err != nil {
		return time.Time{}, nil, err
	}
	if !json.Valid([]byte(payload)) {
		return time.Time{}, nil, errors.New("invalid json payload")
	}
	return parsed, json.RawMessage(payload), nil
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

func createZip(zipPath string, files []string, baseDir string, rawDir string) error {
	if err := os.RemoveAll(zipPath); err != nil {
		return err
	}

	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	writer := zip.NewWriter(zipFile)
	defer writer.Close()

	for _, path := range files {
		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}
		if err := addFileToZip(writer, rel, path); err != nil {
			return err
		}
	}
	if rawDir != "" {
		if err := addDirToZip(writer, rawDir, "raw_session"); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONFile(path string, data any) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
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

func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func addFileToZip(writer *zip.Writer, name, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	zipEntry, err := writer.Create(name)
	if err != nil {
		return err
	}

	_, err = io.Copy(zipEntry, file)
	return err
}

func addDirToZip(writer *zip.Writer, dir string, zipRoot string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		zipPath := filepath.Join(zipRoot, rel)
		return addFileToZip(writer, zipPath, path)
	})
}
