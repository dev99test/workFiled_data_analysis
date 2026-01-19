# Field Sensor Data Ingest

This repository adds daily SENSOR_DATA snapshot collection on the field PC and server-side ingestion/comparison against raw session logs.

## Field PC (Go client)

### Build

```bash
go build -o field-client ./cmd/field-client
```

### Collect SENSOR_DATA snapshots

```bash
./field-client collect-sensor-data \
  --config /etc/field-client/config.json \
  --date 20260119
```

This reads the configured `sensor_data_log_path` and writes:

```
$outbox/snapshots/YYYYMMDD/sensor_data.jsonl
```

Each line is a JSON envelope that stores `captured_at`, `work_field`, and the raw payload.

### Package daily ZIP

```bash
./field-client package-daily \
  --config /etc/field-client/config.json \
  --date 20260119 \
  --raw-dir /var/log/underware/raw_session
```

The ZIP includes:

- `events.jsonl`
- `sensor_data.jsonl`
- `manifest.json`
- `meta.json`
- `raw_session/` (optional, when `--raw-dir` is provided)

### Upload daily ZIP (atomic)

```bash
./field-client upload-daily \
  --config /etc/field-client/config.json \
  --zip /var/lib/field-client/outbox/outgoing/siteA_device01_20260119.zip
```

The upload uses `scp` to `.partial` then renames with `ssh`.

### Cron example

```
10 0 * * * /usr/local/bin/field-client collect-sensor-data --config /etc/field-client/config.json --date $(date -d 'yesterday' +\%Y\%m\%d)
15 0 * * * /usr/local/bin/field-client package-daily --config /etc/field-client/config.json --date $(date -d 'yesterday' +\%Y\%m\%d)
20 0 * * * /usr/local/bin/field-client upload-daily --config /etc/field-client/config.json --zip /var/lib/field-client/outbox/outgoing/siteA_device01_$(date -d 'yesterday' +\%Y\%m\%d).zip
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

- Raw session logs are expected in `raw_session/` within the ZIP for comparison.
- The comparison window is ±3 seconds by default and configurable with `--window`.
- SQLite uses `modernc.org/sqlite` for pure Go compatibility.
