# Trading System (Docker Ready 🐳)

한국투자증권(KIS) Open API를 활용한 시장 데이터 수집 및 API 서버 시스템입니다.
Docker Compose를 통해 누구나 쉽고 빠르게 개발 환경을 구축할 수 있도록 설계되었습니다.

---

## 🚀 빠른 시작 (Docker)

```bash
cp .env.example .env   # API 키 입력 필요
docker compose up -d --build
```

---

## 프로젝트 구조

```
src/
├── kis_auth_handler.py          # 토큰 발급 및 갱신
├── kis_config.py                # API endpoint, 경로 설정
├── kis_market_handler.py        # 코어 API 메서드 모음
└── market_context_collector.py  # 시장 컨텍스트 데이터 수집 스크립트
```

---

## 수집 데이터

| 항목 | 심볼 | 메서드 |
|------|------|--------|
| S&P500 일봉 | `.SPX` | `fetch_overseas_index_daily` |
| NASDAQ 일봉 | `.IXIC` | `fetch_overseas_index_daily` |
| SOX 일봉 | `.SOX` | `fetch_overseas_index_daily` |
| VIX 일봉 | `.VIX` | `fetch_overseas_index_daily` |
| KOSPI 일봉 (시가/종가) | `0001` | `fetch_domestic_index_daily` |
| 외국인/기관 순매수 | 종목코드 | `fetch_investor_flow` |
| USD/KRW 환율 | `FX@KRW` | `fetch_fx_rate_daily` |

---

## 환경 설정

### 1. API 키 설정
아래 두 가지 방법 중 하나를 선택하여 KIS API 키를 설정합니다.

- **Docker/추천**: 프로젝트 루트에 `.env` 파일을 생성하고 내용을 채웁니다. (참고: `.env.example`)
  ```bash
  cp .env.example .env
  # .env 파일을 열어 KIS_APP_KEY, KIS_APP_SECRET 등을 수정
  ```
- **로컬 레거시**: `~/.ssh/kis` 파일에 직접 저장합니다.
  ```
  KIS_APP_KEY=your_app_key
  KIS_APP_SECRET=your_app_secret
  ```

---

## 실행 방법

### 1. Docker Compose (추천)
모든 서비스(API 서버 + 데이터 수집 데몬)를 컨테이너로 실행합니다.
```bash
docker compose up -d --build
```
- **API 서버**: `http://localhost:8000`
- **수집 데몬**: 백그라운드에서 실시간 데이터 수집 (로그는 `logs/` 폴더에서 확인 가능)

### 2. 로컬 실행
의존성 설치 후 직접 스크립트를 실행합니다.
```bash
# 의존성 설치 (uv 사용 시)
uv sync

# 데이터 수집 실행
python src/market_context_collector.py

# API 서버 실행
PYTHONPATH=src uvicorn api:app --reload
```

---

## 📈 KOSPI 200 데이터 파이프라인

### 파이프라인 구성

| 스크립트 | 용도 | 데이터 소스 |
|---|---|---|
| `src/daily_intraday_orchestrator.py` | 매일 장 마감 후 KOSPI 200 전 종목 1분봉 수집 | KIS API |
| `src/backfill_orchestrator.py` | 특정 종목의 일봉(5년) + 1분봉(30일) 소급 수집 | KIS API + Yahoo Finance |
| `src/intraday_backfill_stock.py` | 단일 종목 1분봉 N일 소급 수집 | Yahoo Finance |
| `src/collect_daily_stock.py` | 단일 종목 일봉 N일 수집 | KIS API |

수집된 데이터는 `data/market_data/kr/` 이하에 Parquet 형식으로 저장됩니다.

```
data/market_data/kr/
├── stock/
│   ├── daily/          # 종목별 일봉 (e.g. 005930.parquet)
│   └── 1min/           # 날짜별 1분봉 (e.g. 2026-04-03.parquet)
    ├── 1min/           # 날짜별 KOSPI200 전 종목 1분봉
    └── components/     # KOSPI200 구성종목 목록
```

---

## 💬 커뮤니티 데이터 (네이버 종목 토론방)

네이버 증권 종목 토론방의 실시간 게시물 데이터를 수집하여 여론의 흐름을 파악합니다.

### 동작 방식
- **증분 수집 (Incremental)**: 데이터베이스(`trading_data.db`)에 이미 저장된 최신 게시물 번호(nid)를 파악해, 새로운 글만 중복 없이 가져옵니다.
- **이중화 스크래핑**: `httpx`를 이용한 고속 수집(Plan A)을 기본으로 하며, 차단될 경우 `playwright`를 통해 브라우저 기반(Plan B)으로 로드합니다.
- **인코딩 처리**: 네이버 증권의 EUC-KR / UTF-8 동적 인코딩을 자동 감지하여 안정적으로 한글을 수집합니다.
- **DB 저장 및 알림**: 수집한 데이터는 SQLite `stock_board_posts` 테이블에 영구 저장되며, 실행 직후 Slack과 Discord로 📈 요약 리포트(신규 건수 및 인기글)를 발송합니다.

### ▶️ 수동 실행

```bash
# 특정 종목 대상 수집 (예: SK하이닉스, 삼성전자)
# 기본적으로 5페이지까지 탐색하며, 기존 DB 데이터와 만나면 자동 중단됩니다.
uv run python3 src/collectors/naver_board/collector.py --symbols 000660,005930 --max_pages 5
```

---

### 🔔 알림 설정 (Slack / Discord)

수집 파이프라인은 시작·완료·오류 시 Slack과 Discord로 동시에 알림을 전송합니다.
`config.yaml` 또는 `~/.ssh/kis`에 아래 환경변수를 추가하세요.

```yaml
# Slack
SLACK_BOT_TOKEN: xoxb-...
SLACK_CHANNEL_ID: C0XXXXXX

# Discord - 봇 토큰 방식 (추천)
DISCORD_TOKEN: your-discord-bot-token
DISCORD_CHANNEL_ID: your-channel-id-number

# Discord - 웹훅 방식 (선택)
# DISCORD_WEBHOOK_URL: https://discord.com/api/webhooks/...
```

> **Discord 채널 ID 확인**: 디스코드 → 설정 → 고급 → 개발자 모드 ON → 채널 우클릭 → "채널 ID 복사"

---

### ▶️ 수동 실행

```bash
# 특정 종목 소급 수집 (일봉 5년 + 1분봉 30일)
uv run src/backfill_orchestrator.py --symbol 005930
uv run src/backfill_orchestrator.py --symbol 005930 --years 10 --days 30

# 오늘 KOSPI 200 전 종목 1분봉 수집 (1회 실행)
uv run src/daily_intraday_orchestrator.py
```

---

### ⏰ Prefect를 이용한 매일 자동 수집

#### 1단계: Prefect 서버 실행

Prefect 스케줄러가 동작하려면 먼저 Prefect API 서버가 떠 있어야 합니다.

```bash
# 로컬 Prefect 서버 실행
uv run prefect server start
```

#### 2단계: Prefect API URL 설정

별도 터미널에서 Worker가 로컬 Prefect 서버에 연결할 수 있도록 API URL을 설정합니다.

```bash
uv run prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# 현재 설정 확인
uv run prefect config view
```

`prefect worker start` 실행 시 `PREFECT_API_URL must be set` 오류가 나면 이 단계가 누락된 것입니다.

#### 3단계: Worker 실행

별도 터미널에서 작업을 처리할 Worker를 실행합니다.

```bash
uv run prefect worker start --pool "default-agent-pool"
```

#### 4단계: 스케줄 배포

매일 오후 4시(KST, 월~금)에 KOSPI 200 데이터를 자동 수집하도록 배포합니다.

```bash
uv run prefect deploy src/daily_intraday_orchestrator.py:daily_intraday_flow \
  --name "KOSPI-Intraday-Daily" \
  --cron "0 16 * * 1-5" \
  --timezone "Asia/Seoul" \
  --pool "default-agent-pool"

# 네이버 종목 토론방 수집 (하루 2번, 12시간 간격)
uv run prefect deploy src/collectors/naver_board/orchestrator.py:naver_board_flow \
  --name "Naver-Board-Sync" \
  --cron "0 */12 * * *" \
  --timezone "Asia/Seoul" \
  --pool "default-agent-pool"
```

#### 배포 확인

```bash
# 배포 목록 확인
uv run prefect deployment ls

# 수동으로 즉시 실행 (테스트용)
uv run prefect deployment run 'Daily-KOSPI200-Intraday-Flow/KOSPI-Intraday-Daily'

# 네이버 종목 토론방 플로우 즉시 실행 (테스트용)
uv run prefect deployment run 'Naver-Board-Collection-Flow/Naver-Board-Sync'
```

#### Docker 환경에서 자동화

로컬 SQLite 기반 `prefect server start`는 worker를 붙이면 `database is locked`가 발생할 수 있습니다. 운영용으로는 `docker compose`로 PostgreSQL + Prefect Server + Prefect Worker를 함께 올리는 구성을 사용합니다.

```yaml
postgres:
  image: postgres:16-alpine

prefect-server:
  build: .
  command: prefect server start --host 0.0.0.0

prefect-worker:
  build: .
  command: prefect worker start --pool "default-agent-pool"
```

실행:

```bash
docker compose up -d postgres prefect-server prefect-worker
```

대시보드:

```text
http://127.0.0.1:4200
```

운영 메모:
- `prefect-server`는 PostgreSQL을 메타데이터 DB로 사용합니다.
- `prefect-worker`는 `http://prefect-server:4200/api`로 연결됩니다.
- `prefect-worker`는 호스트의 [`trading_data.db`](/Users/giwooklee/Workspace/trading_system/trading_data.db)를 그대로 마운트하므로, 수집 결과가 로컬 DB에 바로 반영됩니다.
- `prefect-worker`는 [`config.yaml`](/Users/giwooklee/Workspace/trading_system/config.yaml)도 함께 마운트하므로 Slack/Discord 알림 설정을 그대로 사용합니다.
- `.env`를 쓸 경우 프로젝트 루트에 두고 컨테이너를 다시 띄워야 반영됩니다.

배포 재등록:

```bash
# 로컬 호스트에서 재배포
PREFECT_API_URL=http://127.0.0.1:4200/api uv run prefect deploy --name Naver-Board-Sync

# 경로를 /app 기준으로 맞추려면 worker 컨테이너 내부에서 재배포
docker compose exec -T \
  -e PREFECT_API_URL=http://prefect-server:4200/api \
  prefect-worker \
  /app/.venv/bin/prefect deploy --name Naver-Board-Sync --prefect-file /app/prefect.yaml
```

수동 실행:

```bash
PREFECT_API_URL=http://127.0.0.1:4200/api \
  uv run prefect deployment run 'Naver-Board-Collection-Flow/Naver-Board-Sync'
```

상태 확인:

```bash
docker compose ps
docker compose logs -f prefect-worker
docker compose logs -f prefect-server
```

#### 트러블슈팅: `sqlite3.OperationalError: database is locked`

로컬 `prefect server start`는 기본적으로 SQLite(`~/.prefect/prefect.db`)를 사용합니다. 개발 환경에서는 간단하지만, Worker가 붙어 동시에 상태를 읽고 쓰기 시작하면 `database is locked`가 발생할 수 있습니다.

먼저 아래 순서로 확인합니다.

```bash
# 1) Prefect 서버/워커를 모두 중지
# 실행 중인 터미널에서 Ctrl+C

# 2) 필요하면 DB timeout을 늘림
uv run prefect config set PREFECT_API_DATABASE_TIMEOUT=60

# 3) 서버 재시작
uv run prefect server start

# 4) 다른 터미널에서 워커 재시작
uv run prefect worker start --pool "default-agent-pool"
```

락이 계속 발생하면 SQLite 대신 PostgreSQL을 사용하는 것이 안전합니다.

```bash
uv run prefect config set \
  PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://postgres:password@localhost:5432/prefect"

uv run prefect server start
```

Prefect 공식 문서 기준으로 로컬 기본 DB는 SQLite이며, 운영 또는 동시성 있는 환경에서는 PostgreSQL 사용이 권장됩니다.

---

## 테스트

### 검증 필요 항목

아래 항목은 실계좌 환경에서 응답을 직접 확인해야 합니다.

| 항목 | 심볼 | 확인 포인트 |
|------|------|-------------|
| SOX | `.SOX` | KIS API가 해당 심볼을 지원하는지 |
| VIX | `.VIX` | 동일 |
| USD/KRW | `FX@KRW` | 환율 심볼 및 TR(`HHDFS76240000`) 정상 동작 여부 |

### 수동 테스트 방법

각 메서드를 개별적으로 호출해 응답 구조를 확인합니다.

```python
from kis_market_handler import MarketHandler
from datetime import datetime, timedelta

handler = MarketHandler()
end = datetime.today()
start = end - timedelta(days=7)

# 해외지수
df = handler.fetch_overseas_index_daily(".SPX", start, end)
print(df.columns.tolist())   # 응답 컬럼 확인
print(df.tail(3))

# 국내지수
df = handler.fetch_domestic_index_daily("0001", start, end)
print(df.columns.tolist())
print(df.tail(3))

# 외국인/기관 순매수 (삼성전자)
data = handler.fetch_investor_flow("005930")
print(data.get("output", {}))  # 응답 키 확인

# 환율
df = handler.fetch_fx_rate_daily("FX@KRW", start, end)
print(df.columns.tolist())
print(df.tail(3))
```

### 실패 시 체크리스트

- **SOX / VIX 응답 없음**: `.SOX` → `$SOX`, `.VIX` → `$VIX` 등 심볼 변형 시도
- **환율 응답 없음**:
  - 심볼 `FX@KRW` → `USDKRW` 변경 시도
  - TR ID `HHDFS76240000` → KIS API 문서에서 환율 전용 TR 재확인
- **국내지수 컬럼명 다름**: `bstp_nmix_clpr` 대신 실제 응답의 `output2` 키 목록 출력 후 `kis_market_handler.py` 내 numeric 변환 컬럼 목록 수정
- **메신저 봇 (Slack / Discord)**:
   - Slack: `python src/slack_handler.py`
   - Discord: `python src/discord_handler.py` (DISCORD_TOKEN 설정 필요)

## 상세 가이드 (Local)

### 응답 컬럼 기준

| 메서드 | 날짜 컬럼 | 종가 컬럼 | 시가 컬럼 |
|--------|-----------|-----------|-----------|
| `fetch_overseas_index_daily` | `bsop_date` | `clpr` | `oprc` |
| `fetch_domestic_index_daily` | `bsop_date` | `bstp_nmix_clpr` | `bstp_nmix_oprc` |
| `fetch_fx_rate_daily` | `bsop_date` | `clos` | `open` |
