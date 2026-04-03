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
└── kospi200/
    ├── 1min/           # 날짜별 KOSPI200 전 종목 1분봉
    └── components/     # KOSPI200 구성종목 목록
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

Prefect 스케줄러가 동작하려면 Prefect 서버 또는 Worker가 실행되어야 합니다.

```bash
# 로컬 Prefect 서버 실행 (백그라운드)
uv run prefect server start
```

#### 2단계: Worker 실행

별도 터미널에서 작업을 처리할 Worker를 실행합니다.

```bash
uv run prefect worker start --pool "default-agent-pool"
```

#### 3단계: 스케줄 배포

매일 오후 4시(KST, 월~금)에 KOSPI 200 데이터를 자동 수집하도록 배포합니다.

```bash
uv run prefect deploy src/daily_intraday_orchestrator.py:daily_intraday_flow \
  --name "KOSPI-Intraday-Daily" \
  --cron "0 16 * * 1-5" \
  --timezone "Asia/Seoul" \
  --pool "default-agent-pool"
```

#### 배포 확인

```bash
# 배포 목록 확인
uv run prefect deployment ls

# 수동으로 즉시 실행 (테스트용)
uv run prefect deployment run 'Daily-KOSPI200-Intraday-Flow/KOSPI-Intraday-Daily'
```

#### Docker 환경에서 자동화

`docker-compose.yml`에 Prefect Worker 서비스를 추가하면 컨테이너 기반으로 스케줄을 운영할 수 있습니다.

```yaml
prefect-worker:
  build: .
  command: uv run prefect worker start --pool "default-agent-pool"
  env_file: .env
  volumes:
    - ./data:/app/data
```

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
