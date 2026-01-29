# Field Client (현장 PC 일일 로그 분석기)

이 저장소는 **현장 PC에서 센서 로그를 일 단위로 분석**하는 `field-client`만을 기준으로 정리했습니다.

## 빌드

```bash
go build -o field-client ./cmd/field-client
```

## 일일 로그 분석 실행 (필수 예시)

아래 명령을 그대로 복붙해서 실행할 수 있습니다.

```bash
./field-client analyze-daily -config ./config/field_client_config.sample.json -date 20260120
```

- `-config` 는 단일 대시(short) 옵션이며 `--config`도 동일하게 동작합니다.
- `log_root`는 기본적으로 **config에서 읽습니다**. 필요 시 `-log-root`로 덮어쓸 수 있습니다.
- 분석 결과는 `$outbox_dir/daily/YYYYMMDD/analysis.json`에 저장됩니다.

### 선택 옵션

```bash
./field-client analyze-daily \
  -config ./config/field_client_config.sample.json \
  -date 20260120 \
  -log-root /var/log/field-logs \
  -max-lines 5000
```

- `-log-root`: config의 `log_root`를 임시로 덮어쓰기.
- `-max-lines`: 센서별 최대 처리 라인 수(기본 5000). 로그가 매우 큰 경우 분석 시간을 제한하기 위한 안전장치입니다.

## 샘플 설정 상세

### `config/field_client_config.sample.json`

현장 PC 환경에 맞게 **반드시 수정**해서 사용합니다.

- `site_id`: 현장 식별자
- `device_id`: 장치 식별자
- `work_field`: (필요 시) 현장 구역 이름
- `outbox_dir`: 결과 저장 경로
- `log_root`: 로그 루트 디렉터리
  - 하위에 `GATE*`, `WLS*`, `PUMP*`, `TEMP*` 디렉터리가 있어야 합니다.
- `exclude_dirs`: 분석에서 제외할 디렉터리
  - 기본값: `ALL`, `PING`, `SERVER`
- (옵션) `duplicate_run_threshold`, `fallback_to_latest_file`, `debug`
- (옵션) `-max-lines` 옵션으로 센서당 최대 라인 수를 조절할 수 있습니다.

### `config/mapping.sample.json`

이 파일은 **서버로 보내는 sensor_data(JSON)의 id를 논리 센서명으로 매핑**하는 용도입니다.

- 현장별로 id가 달라질 수 있으므로 코드가 아니라 **mapping으로 관리**합니다.
- 사용자가 현장 구성에 맞게 **직접 수정**해야 하는 설정 파일입니다.
- **현재 `analyze-daily`만 사용한다면 mapping은 필요 없습니다** (서버 ingest 단계에서만 사용).

## 결과 JSON (`analysis.json`) 상세

결과 파일은 다음 위치에 생성됩니다:

```
$outbox_dir/daily/YYYYMMDD/analysis.json
```

센서별 `metrics`는 다음을 포함합니다:

- `time_range.from` / `time_range.to`
  - 분석 대상 로그가 실제로 커버하는 시간 범위입니다.
  - 로그 라인 타임스탬프가 있으면 그 범위를 사용하고, 없으면 파일명 기반으로 추정합니다.
- `snd_count`
  - 해당 날짜에 관측된 요청(`snd`) 라인 수
- `rcv_count`
  - 해당 날짜에 관측된 응답(`rcv`) 라인 수
- `no_response`
  - `snd`는 있지만 대응 `rcv`가 끝내 나오지 않은 횟수
  - 정의: 로그에 `snd`만 존재하고 해당 요청에 대한 `rcv`가 끝내 나오지 않으면 카운트
  - **SERVER 디렉터리는 분석 제외** (정책)
- `zero_data`
  - WLS 프로토콜 프레임이 무효인 건수
  - 정의(확정): WLS `rcv` payload는 반드시 **11바이트 프레임(FA … 76)** 이어야 유효
  - 유효 프레임 뒤에 바이트가 추가되면(예: `… 76, FA, FF …`) **무조건 zero_data**로 카운트
  - `zero_data`로 판정된 `rcv`는 수위 통계(`min/max/last`) 업데이트에서 **제외**
- `duplicates`
  - **정상(유효) `rcv` 프레임**의 동일 payload 연속 반복 횟수
  - `zero_data`는 duplicates 비교 대상에서 제외되어 **체인이 끊깁니다**

### WLS 수위(`wls_min_value_cm`, `wls_max_value_cm`, `wls_last_value_cm`)

- 현재 프로토콜 기준으로 **0~96cm 범위만 유효**한 값으로 처리합니다.
- 구현상 0~96cm를 벗어나는 값은 통계 업데이트에서 제외됩니다.
- 따라서 64255 같은 잘못된 값이 결과에 포함되지 않습니다.

## 기본 분석 규칙

- 포함 디렉터리: `GATE*`, `WLS*`, `PUMP*`, `TEMP*`
- 제외 디렉터리: `ALL`, `PING`, `SERVER`
- 파일 선택 규칙:
  - 날짜(`YYYY-MM-DD`)가 포함된 로그 파일을 우선 분석
  - 해당 날짜 파일이 없으면 **최신 파일로 fallback** (config의 `fallback_to_latest_file` 기준)
- `-max-lines`는 센서별 처리 라인 수를 제한하여 과도한 로그로 인한 분석 지연을 방지합니다.

## field-client 자동 실행 (systemd timer)

아래 예시는 **“오늘이 2026-01-29이면, 다음날 2026-01-30 00:05에 2026-01-29 하루치 분석”**을 수행합니다.

### `/etc/systemd/system/field-client-analyze.service`
```bash
```ini
[Unit]
Description=Field Client Daily Log Analysis (Yesterday)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=user
Group=user
WorkingDirectory=/user/user/Documents/workFiled_data_analysis
ExecStart=/home/user/Documents/workFiled_data_analysis/run_analyze_yesterday.sh

```

### `/etc/systemd/system/field-client-analyze.timer`
```bash
```ini
[Unit]
Description=Run field-client analyze-daily at 00:05

[Timer]
OnCalendar=*-*-* 00:05:00
Persistent=true

[Install]
WantedBy=timers.target
```

### `run_analyze_yesterday.sh`
```bash
```ini
#!/usr/bin/env bash
set -euo pipefail

BASE="/home/user/Documents/workFiled_data_analysis"
BIN="${BASE}/field-client"
CFG="${BASE}/config/config.json"
LOG_ROOT="/home/user/Downloads/underware202408-main/log"

# 어제 날짜 (YYYYMMDD)
TARGET_DATE="$(date -d 'yesterday' +%Y%m%d)"

echo "[INFO] Analyzing date: ${TARGET_DATE}"

exec "${BIN}" analyze-daily \
  -config "${CFG}" \
  -date "${TARGET_DATE}" \
  -log-root "${LOG_ROOT}"
```


### 확인 포인트

- `date -d 'yesterday' +%Y%m%d`가 **항상 “어제 날짜”를 계산**하는지 확인하세요.
- `/etc/field-client/config.json` 경로가 실제 config 위치와 일치해야 합니다.
- `outbox_dir`에 대한 **쓰기 권한**이 `User=field` 계정에 있어야 합니다.

### 데이터 전송
- `데이터는 tar.gz 로 압축되어 전송.
- `접속용 SSH키 설정을 해야 전송이 됩니다.
