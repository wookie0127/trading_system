# Trading System

한국투자증권(KIS) Open API를 활용한 시장 데이터 수집 시스템.

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

KIS API 키를 `~/.ssh/kis` 파일에 저장:

```
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
```

---

## 실행

```bash
cd src
python market_context_collector.py
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

### 응답 컬럼 기준

| 메서드 | 날짜 컬럼 | 종가 컬럼 | 시가 컬럼 |
|--------|-----------|-----------|-----------|
| `fetch_overseas_index_daily` | `bsop_date` | `clpr` | `oprc` |
| `fetch_domestic_index_daily` | `bsop_date` | `bstp_nmix_clpr` | `bstp_nmix_oprc` |
| `fetch_fx_rate_daily` | `bsop_date` | `clos` | `open` |
