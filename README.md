# Field Sensor Data Ingest

이 저장소는 현장 PC에서 센서 로그를 일 단위로 분석하는 도구와, 서버에서 ZIP 적재/비교를 수행하는 워커를 제공합니다.

## 현장 PC 로그 분석기 (Go)

### 빌드


```bash
go build -o field-client ./cmd/field-client
```

### 일일 로그 분석 실행


```bash
./field-client analyze-daily \
  --config /etc/field-client/config.json \
  --date 20260119 \
  --log-root /home/eumit/Downloads/underware202408-main/log
```


`log_root` 아래의 센서 디렉터리(GATE*/WLS*/PUMP*/TEMP*)를 날짜 기준으로 분석하고 결과를 생성합니다.


```
$outbox/daily/YYYYMMDD/analysis.json
```

결과 JSON에는 센서별 시간 범위, snd/rcv 개수, no_response, zero_data, duplicates, WLS 수위(min/max/last) 등이 포함됩니다.

### 크론 예시


```
10 0 * * * /usr/local/bin/field-client analyze-daily --config /etc/field-client/config.json --date $(date -d 'yesterday' +\%Y\%m\%d) --log-root /home/eumit/Downloads/underware202408-main/log
```


## 서버 ingest 워커

### 빌드


```bash
go build -o field-ingest-worker ./cmd/field-ingest-worker
```


### 실행


```bash
./field-ingest-worker \
  --incoming /srv/field-ingest/incoming \
  --work /srv/field-ingest/work \
  --done /srv/field-ingest/done \
  --db /srv/field-ingest/db/field_metrics.sqlite3 \
  --mapping /etc/field-ingest/mapping.json
```


워커 처리 순서:


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


## 샘플 설정


- `config/field_client_config.sample.json`
- `config/mapping.sample.json`


## 분석 규칙 요약

- 기본 포함 디렉터리: GATE*, WLS*, PUMP*, TEMP*
- 기본 제외 디렉터리: ALL, PING, SERVER
- 파일명에 날짜(YYYY-MM-DD)가 포함된 로그 파일을 우선 분석하며, 없으면 최신 파일로 fallback합니다.
- WLS는 11바이트 프레임(FA...76)만 유효로 판단하고, 유효하지 않으면 zero_data로 집계합니다.
- 서버 워커는 ZIP을 풀어 manifest 검증 후 SQLite에 적재합니다.

