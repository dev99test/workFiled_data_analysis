# Field Sensor Data Ingest

This repository provides a field PC raw-log analyzer and a server-side ingest/comparison worker.

## Field PC (Go analyzer)

### Build

```bash
go build -o field-client ./cmd/field-client
```

### Analyze daily logs

```bash
./field-client analyze-daily \
  --config /etc/field-client/config.json \
  --date 20260119 \
  --log-root /home/eumit/Downloads/underware202408-main/log
```

This reads the configured `log_root` and writes:

```
$outbox/daily/YYYYMMDD/analysis.json
```

The output JSON includes per-sensor metrics, examples, and a top-issues summary.

### Cron example

```
10 0 * * * /usr/local/bin/field-client analyze-daily --config /etc/field-client/config.json --date $(date -d 'yesterday' +\%Y\%m\%d) --log-root /home/eumit/Downloads/underware202408-main/log
```

## Server ingest worker

### Build

```bash
go build -o field-ingest-worker ./cmd/field-ingest-worker
```

### Run

```bash
./field-ingest-worker \
  --incoming /srv/field-ingest/incoming \
  --work /srv/field-ingest/work \
  --done /srv/field-ingest/done \
  --db /srv/field-ingest/db/field_metrics.sqlite3 \
  --mapping /etc/field-ingest/mapping.json
```

The worker:

1. Unzips incoming daily ZIP files.
2. Verifies `manifest.json`.
3. Inserts `events.jsonl` into `hourly_metrics`.
4. Inserts `sensor_data.jsonl` into `sensor_data_snapshots`.
5. Compares sensor snapshots with raw session logs stored in `raw_session/` within the ZIP.
6. Writes comparison results to `comparison_results` and moves the ZIP to `done`.

### Systemd example

```
[Unit]
Description=Field Ingest Worker

[Service]
ExecStart=/usr/local/bin/field-ingest-worker --incoming /srv/field-ingest/incoming --work /srv/field-ingest/work --done /srv/field-ingest/done --db /srv/field-ingest/db/field_metrics.sqlite3 --mapping /etc/field-ingest/mapping.json
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

## Sample configuration

- `config/field_client_config.sample.json`
- `config/mapping.sample.json`

## Notes

- The analyzer scans sensor folders matching include globs (default: GATE*, WLS*, PUMP*, TEMP*) and skips excluded directories.
- It prefers log files whose names include the target date (YYYY-MM-DD) and falls back to the latest file when none match.
- The comparison window is ±3 seconds by default and configurable with `--window`.
- SQLite uses `modernc.org/sqlite` for pure Go compatibility.
