# Gemini 기반 시스템 트레이딩 설계 및 검증 계획

## 1. 문서 목적

본 문서는 Gemini를 활용한 BTCUSDT 시스템 트레이딩을 설계하고, Antigravity에 전달하여 구현 가능성, 구조적 결함, 데이터 누수, 리스크 통제, 검증 가능성을 점검하기 위한 기준 문서다.

본 시스템은 Gemini가 모든 매매 결정을 자유롭게 수행하는 완전 자율 트레이딩 시스템을 목표로 하지 않는다.

핵심 원칙은 다음과 같다.

> Gemini는 시장 상태를 해석하고 제한된 행동 중 하나를 선택한다.  
> 포지션 크기, 레버리지, 손절, 최대 손실, 주문 실행은 결정론적 코드가 통제한다.

초기 목표는 실거래가 아니라 **shadow trading**과 **paper trading**을 통해 Gemini가 기존 규칙 기반 전략보다 추가적인 가치를 제공하는지 검증하는 것이다.

---

# 2. 목표와 비목표

## 2.1 목표

1. BTCUSDT 4시간봉을 기준으로 시장 상태를 분석한다.
2. 가격, 기술지표, 파생시장 지표, 제한된 뉴스 및 거시 정보를 결합한다.
3. `LONG`, `SHORT`, `HOLD`, `REDUCE`, `CLOSE`, `NO_TRADE` 중 하나를 Gemini가 선택한다.
4. 모든 입력, 추론 결과, 검증 결과, 가상 주문, 이후 성과를 저장한다.
5. Gemini 판단이 규칙 기반 baseline보다 유효한지 비교한다.
6. LLM 응답의 일관성, 재현성, 비용, 지연, 오류율을 측정한다.
7. 실거래 전 충분한 shadow/paper trading 기간을 확보한다.

## 2.2 비목표

초기 버전에서는 다음 기능을 구현하지 않는다.

- Gemini가 자유롭게 Python 전략 코드를 생성하거나 수정하는 기능
- Gemini가 레버리지와 포지션 수량을 직접 결정하는 기능
- Gemini가 손절가를 임의로 제거하거나 변경하는 기능
- 다중 거래소 동시 운영
- 다중 종목 포트폴리오 최적화
- 초단타 또는 1분 이하 빈도 매매
- 멀티 에이전트 구조
- LangChain, LangGraph 등 별도 프레임워크 도입
- 뉴스 전체 인터넷 검색 결과를 무제한으로 사용하는 구조
- 초기 단계의 자동 실거래

---

# 3. 핵심 설계 원칙

## 3.1 LLM과 결정론적 코드의 역할 분리

### Gemini가 담당하는 영역

- 시장 국면 분류
- 여러 신호 간 충돌 해석
- 전략 유형 선택
- 기존 포지션 유지 여부 판단
- 가설, 지지 근거, 반대 근거, 무효화 조건 생성
- 제한된 행동 집합 중 하나 선택

### 결정론적 코드가 담당하는 영역

- 데이터 수집
- 지표 계산
- 데이터 시점 정렬
- 미래 데이터 차단
- 포지션 크기 계산
- 최대 레버리지 제한
- 손절 및 익절 가격 계산
- 주문 중복 방지
- 일일 최대 손실 제한
- 최대 낙폭 제한
- 거래소 주문 실행
- kill switch
- 장애 대응

## 3.2 Fail-closed 원칙

다음 상황에서는 신규 주문을 실행하지 않는다.

- Gemini API 실패
- 응답 timeout
- 구조화 출력 파싱 실패
- 스키마 검증 실패
- 시장 데이터 누락
- 데이터 freshness 초과
- 허용되지 않은 행동 반환
- 위험 한도 초과
- 계정 상태 조회 실패
- 거래소 상태 확인 실패
- 현재 포지션과 요청 행동의 불일치

기본 동작은 항상 다음이어야 한다.

```text
UNKNOWN / INVALID / ERROR
→ NO_TRADE
```

## 3.3 설명과 예측력 분리

LLM이 생성한 자연어 근거는 감사 및 분석 목적의 로그다.

다음과 같은 해석을 금지한다.

```text
설명이 논리적이다
→ 예측이 정확하다
```

자연어 근거의 품질과 실제 수익성은 별도로 평가한다.

## 3.4 기존 판단과의 일관성보다 반증 가능성 우선

직전 판단을 다음 입력에 포함하되, Gemini가 이전 결정을 방어하도록 유도하지 않는다.

모든 판단에는 반드시 다음이 포함되어야 한다.

- 현재 가설
- 가설을 지지하는 증거
- 가설에 반대하는 증거
- 가설이 무효화되는 조건
- 반대 시나리오
- 불확실성 요소

---

# 4. 대상 시장과 운영 주기

## 4.1 초기 대상

```yaml
exchange: configurable
symbol: BTCUSDT
market: perpetual_futures
decision_timeframe: 4h
execution_mode: shadow
```

## 4.2 실행 주기

Gemini 판단은 4시간봉 종가가 확정된 이후 실행한다.

예시:

```text
4시간봉 종료
→ 데이터 확정 여부 확인
→ 지표 계산
→ 외부 데이터 시점 정렬
→ 시장 스냅샷 생성
→ Gemini 호출
→ 정책 검증
→ 가상 주문 생성
→ 결과 저장
```

### 주의

4시간마다 Gemini를 호출하더라도 위험관리는 4시간마다 실행해서는 안 된다.

```text
Gemini Decision Loop
- 4시간 주기

Risk Monitoring Loop
- 실시간 또는 짧은 주기
- 손절, 청산 위험, 계정 상태, 주문 상태 감시
```

---

# 5. 전체 아키텍처

```text
┌───────────────────────────────────────────┐
│ 1. Market Data Collector                  │
│ OHLCV, funding, OI, liquidation           │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 2. External Context Collector             │
│ Fear & Greed, macro, curated news         │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 3. Snapshot Builder                       │
│ 시점 정렬, freshness, validation          │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 4. Deterministic Feature Engine           │
│ EMA, RSI, MACD, ATR, Bollinger, returns   │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 5. Gemini Decision Layer                  │
│ regime, strategy, action, evidence        │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 6. Policy Validator                       │
│ schema, action, state, freshness checks   │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 7. Risk Engine                            │
│ leverage, size, SL, TP, loss limits       │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 8. Execution Simulator / Exchange Adapter │
│ shadow → paper → live                     │
└────────────────────┬──────────────────────┘
                     ↓
┌───────────────────────────────────────────┐
│ 9. Decision & Outcome Store               │
│ inputs, outputs, orders, outcomes         │
└───────────────────────────────────────────┘
```

---

# 6. 권장 디렉터리 구조

```text
trading_system/
├── configs/
│   ├── system.yaml
│   ├── risk.yaml
│   ├── llm.yaml
│   ├── data_sources.yaml
│   └── strategies/
│       └── btc_4h_adaptive.md
│
├── data/
│   ├── raw/
│   ├── snapshots/
│   └── features/
│
├── src/
│   └── trading_system/
│       ├── collectors/
│       │   ├── market.py
│       │   ├── derivatives.py
│       │   ├── sentiment.py
│       │   ├── macro.py
│       │   └── news.py
│       │
│       ├── snapshots/
│       │   ├── builder.py
│       │   ├── validator.py
│       │   └── schemas.py
│       │
│       ├── features/
│       │   ├── technical.py
│       │   ├── volatility.py
│       │   ├── derivatives.py
│       │   └── regime.py
│       │
│       ├── llm/
│       │   ├── client.py
│       │   ├── prompts.py
│       │   ├── schemas.py
│       │   ├── parser.py
│       │   └── decision_service.py
│       │
│       ├── policy/
│       │   ├── validator.py
│       │   ├── state_machine.py
│       │   └── rules.py
│       │
│       ├── risk/
│       │   ├── sizing.py
│       │   ├── stop_loss.py
│       │   ├── limits.py
│       │   └── kill_switch.py
│       │
│       ├── execution/
│       │   ├── base.py
│       │   ├── shadow.py
│       │   ├── paper.py
│       │   └── exchange.py
│       │
│       ├── evaluation/
│       │   ├── baseline.py
│       │   ├── outcomes.py
│       │   ├── consistency.py
│       │   └── reports.py
│       │
│       ├── storage/
│       │   ├── repository.py
│       │   ├── models.py
│       │   └── migrations/
│       │
│       └── orchestration/
│           ├── decision_job.py
│           └── risk_monitor.py
│
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── replay/
│   └── failure/
│
├── artifacts/
│   ├── prompts/
│   ├── decisions/
│   ├── reports/
│   └── replay_runs/
│
└── docs/
    ├── architecture.md
    ├── validation_plan.md
    └── operating_runbook.md
```

---

# 7. 시장 입력 데이터 설계

## 7.1 필수 가격 데이터

```yaml
ohlcv:
  - timestamp
  - open
  - high
  - low
  - close
  - volume
```

필수 타임프레임:

- 4시간봉: 핵심 판단 주기
- 1시간봉: 하위 시간대 확인
- 1일봉: 상위 추세 확인

## 7.2 기술지표

초기에는 제한된 지표만 사용한다.

```yaml
technical_indicators:
  - ema_20
  - ema_50
  - ema_100
  - ema_200
  - rsi_14
  - macd
  - macd_signal
  - macd_histogram
  - atr_14
  - bollinger_upper
  - bollinger_middle
  - bollinger_lower
  - volume_ma
  - volume_zscore
```

### 주의

지표 수를 무분별하게 늘리지 않는다.

지표가 많아질수록 다음 문제가 발생한다.

- 서로 중복되는 신호 증가
- LLM이 임의로 근거를 선택
- 프롬프트 길이 증가
- 사후적 정당화 가능성 증가
- 어떤 지표가 기여했는지 분석 어려움

## 7.3 파생시장 지표

```yaml
derivatives:
  - funding_rate
  - open_interest
  - open_interest_change
  - long_short_ratio
  - long_liquidation
  - short_liquidation
  - basis
```

각 데이터는 다음 메타데이터를 가져야 한다.

```yaml
metadata:
  source:
  observed_at:
  effective_at:
  fetched_at:
  freshness_seconds:
  is_stale:
```

## 7.4 심리 지표

초기에는 소수만 사용한다.

```yaml
sentiment:
  - fear_greed_index
```

심리 지표는 독립적인 진입 신호가 아니라 보조 정보로만 사용한다.

## 7.5 뉴스 및 거시 정보

뉴스는 직접적인 진입 트리거로 사용하지 않는다.

뉴스 입력은 다음 구조로 제한한다.

```json
{
  "headline": "...",
  "published_at": "...",
  "source": "...",
  "category": "macro|regulation|exchange|security|geopolitics",
  "summary": "...",
  "relevance": "high|medium|low"
}
```

### 뉴스 관련 주의점

- 최신 시점 기준으로 수집됐는지 검증한다.
- 중복 기사 제거가 필요하다.
- 출처별 신뢰도를 구분한다.
- 동일 사건의 재전송 기사를 여러 사건으로 계산하지 않는다.
- LLM이 "가격에 선반영됐다"고 단정하지 못하게 한다.
- 기사 게시 시각과 시장 데이터 시각을 반드시 정렬한다.
- 사후 백테스트에서 미래 뉴스가 포함되지 않도록 한다.

---

# 8. 시장 스냅샷 스키마

Gemini에는 원시 캔들 전체보다 구조화된 스냅샷을 전달한다.

```json
{
  "run_at": "2026-01-01T04:05:00Z",
  "symbol": "BTCUSDT",
  "decision_timeframe": "4h",
  "price": {
    "close": 73000.0,
    "return_4h": -0.02,
    "return_24h": -0.06,
    "range_4h": 0.04
  },
  "higher_timeframe": {
    "daily_trend": "DOWN",
    "price_vs_ema_200": -0.08
  },
  "technical": {
    "ema_20": 75200.0,
    "ema_50": 76300.0,
    "ema_200": 77100.0,
    "rsi_14": 24.0,
    "macd_histogram": -420.0,
    "atr_ratio": 0.035,
    "bollinger_position": -0.12,
    "volume_zscore": 1.8
  },
  "derivatives": {
    "funding_rate": -0.0002,
    "open_interest_change_4h": -0.08,
    "long_liquidation_4h": 280000000
  },
  "sentiment": {
    "fear_greed_index": 22
  },
  "position": {
    "status": "FLAT",
    "side": null,
    "entry_price": null,
    "unrealized_pnl_ratio": 0.0
  },
  "previous_decision": {
    "action": "NO_TRADE",
    "thesis": "...",
    "invalidation_conditions": []
  },
  "data_quality": {
    "is_complete": true,
    "stale_sources": [],
    "warnings": []
  }
}
```

---

# 9. 자연어 전략 파일 설계

`strategy.md`는 시스템의 철학과 의사결정 원칙을 담는다.

강제 위험 제한은 이 파일이 아니라 코드로 구현한다.

```markdown
# BTC 4H Adaptive Strategy

## Objective

BTCUSDT 4시간봉을 기준으로 추세 추종, 평균 회귀, 변동성 돌파 가능성을 평가한다.

수익 극대화보다 자본 보존과 큰 손실 회피를 우선한다.

## Allowed Strategies

- TREND_FOLLOWING
- MEAN_REVERSION
- BREAKOUT
- NONE

## Decision Principles

1. 상위 시간대 추세를 우선 확인한다.
2. 단일 기술지표만으로 진입하지 않는다.
3. 기술지표와 파생시장 지표가 크게 충돌하면 NO_TRADE를 우선한다.
4. 뉴스는 독립적인 진입 근거로 사용하지 않는다.
5. 직전 판단은 참고하되 현재 증거로 반증될 수 있다.
6. 지지 근거와 반대 근거를 모두 제시한다.
7. 가설의 무효화 조건을 구체적으로 명시한다.
8. 손실 중인 포지션을 본전 회복 기대만으로 유지하지 않는다.
9. 불확실성이 높은 상황에서는 방향을 강제로 선택하지 않는다.

## Mean Reversion

다음 조건이 복합적으로 관찰될 때 고려한다.

- 가격의 단기 과도 이동
- 모멘텀 지표의 극단 상태
- 투매 또는 과열 징후
- 가격 이동 속도의 둔화
- 초기 반전 가능성을 지지하는 추가 증거

강한 추세가 지속되는 상황에서는 평균 회귀 진입을 피한다.

## Trend Following

다음 조건이 복합적으로 관찰될 때 고려한다.

- 상위 및 하위 시간대 방향 일치
- 이동평균 배열과 기울기가 추세를 지지
- 거래량 또는 파생시장 지표가 방향을 지지
- 진입 시점이 과도하게 추격 매수 또는 추격 매도가 아님

## Invalidation

각 판단에는 다음을 포함한다.

- 가설 무효화 조건
- 반대 시나리오
- 포지션 축소 또는 종료 조건

## Prohibited Behavior

- 손실 만회를 위한 포지션 확대
- 강제 손절 제거
- 허용 범위를 넘는 레버리지 요청
- 데이터 누락 상태에서 신규 진입
- 과거 판단을 유지하기 위한 선택적 증거 사용
- 뉴스만으로 방향 결정
```

---

# 10. Gemini 출력 스키마

```python
from typing import Literal

from pydantic import BaseModel, Field


class Evidence(BaseModel):
    category: Literal[
        "technical",
        "derivatives",
        "macro",
        "news",
        "position",
    ]
    description: str


class TradingDecision(BaseModel):
    regime: Literal[
        "TREND_UP",
        "TREND_DOWN",
        "RANGE",
        "HIGH_VOLATILITY",
        "UNCERTAIN",
    ]

    strategy: Literal[
        "TREND_FOLLOWING",
        "MEAN_REVERSION",
        "BREAKOUT",
        "NONE",
    ]

    action: Literal[
        "OPEN_LONG",
        "OPEN_SHORT",
        "HOLD",
        "REDUCE",
        "CLOSE",
        "NO_TRADE",
    ]

    confidence: float = Field(ge=0.0, le=1.0)

    thesis: str
    supporting_evidence: list[Evidence]
    contradicting_evidence: list[Evidence]
    invalidation_conditions: list[str]
    alternative_scenario: str
    risk_flags: list[str]

    risk_profile: Literal[
        "CONSERVATIVE",
        "BALANCED",
        "AGGRESSIVE",
    ]
```

## 10.1 출력 관련 주의점

- `confidence`는 실제 확률로 해석하지 않는다.
- 모델의 자기평가 값으로만 저장한다.
- 별도 calibration 전에는 포지션 크기에 직접 사용하지 않는다.
- 자연어 응답을 직접 파싱하지 않는다.
- 스키마 검증에 실패하면 `NO_TRADE` 처리한다.
- 허용되지 않은 전략명 또는 행동은 즉시 거부한다.

---

# 11. 포지션 상태 머신

Gemini의 행동은 현재 포지션과 일치해야 한다.

```text
FLAT
├── OPEN_LONG  → LONG
├── OPEN_SHORT → SHORT
└── NO_TRADE   → FLAT

LONG
├── HOLD       → LONG
├── REDUCE     → LONG
├── CLOSE      → FLAT
└── OPEN_SHORT → INVALID

SHORT
├── HOLD       → SHORT
├── REDUCE     → SHORT
├── CLOSE      → FLAT
└── OPEN_LONG  → INVALID
```

반대 포지션으로 전환하려면 다음 두 단계를 거친다.

```text
LONG
→ CLOSE
→ 다음 의사결정 주기에서 OPEN_SHORT
```

초기 버전에서는 동일 주기 내 즉시 반전 진입을 허용하지 않는다.

---

# 12. 위험관리 정책

## 12.1 코드 기반 강제 제한

예시 초기 설정:

```yaml
risk:
  max_leverage: 2.0
  max_risk_per_trade_ratio: 0.005
  max_position_ratio: 0.15
  max_daily_loss_ratio: 0.02
  max_account_drawdown_ratio: 0.08
  max_consecutive_losses: 4
  max_open_positions: 1
  min_reward_risk_ratio: 1.5
```

이 값은 예시이며 paper trading 결과를 통해 조정한다.

## 12.2 포지션 크기

포지션 크기는 Gemini가 아닌 Risk Engine이 계산한다.

```text
허용 손실 금액
= 계정 자산 × 거래당 최대 위험 비율

포지션 크기
= 허용 손실 금액 ÷ 손절 거리
```

## 12.3 손절가

손절은 다음 중 하나의 결정론적 방법으로 계산한다.

- ATR 배수
- 최근 swing high/low
- 구조적 invalidation level
- 위 방법 중 더 보수적인 값

Gemini가 손절 제거를 요청해도 무시한다.

## 12.4 Kill switch

다음 조건에서 신규 주문을 중단하고 기존 포지션을 정책에 따라 종료한다.

- 일일 최대 손실 초과
- 계정 최대 낙폭 초과
- 연속 손실 횟수 초과
- 거래소 API 상태 불안정
- 시장 데이터 중단
- 주문 상태 불일치
- 포지션 상태 불일치
- 데이터 시간 역전
- 중복 주문 감지
- 시스템 시계 이상
- DB 저장 실패로 감사 로그 보장 불가

---

# 13. 저장해야 할 데이터

## 13.1 decision_runs

```text
id
run_at
symbol
model_name
model_version
prompt_version
strategy_version
market_snapshot_id
position_snapshot_id
raw_response
parsed_response
validation_status
validation_reasons
latency_ms
input_token_count
output_token_count
estimated_cost
```

## 13.2 market_snapshots

```text
timestamp
symbol
timeframe
raw_market_data
technical_features
derivatives_features
sentiment_features
macro_context
news_context
data_quality
source_versions
```

## 13.3 orders

```text
decision_id
mode
side
order_type
requested_price
requested_quantity
filled_price
filled_quantity
fee
slippage
status
created_at
filled_at
```

## 13.4 position_outcomes

```text
decision_id
return_4h
return_8h
return_24h
return_72h
maximum_favorable_excursion
maximum_adverse_excursion
hit_stop_loss
hit_take_profit
closed_reason
realized_pnl
```

## 13.5 counterfactual_outcomes

`NO_TRADE`, `HOLD` 판단도 평가하기 위해 저장한다.

```text
decision_id
hypothetical_long_return_4h
hypothetical_short_return_4h
hypothetical_long_return_24h
hypothetical_short_return_24h
best_action_4h
best_action_24h
```

---

# 14. Baseline 전략

Gemini 성과는 반드시 단순 기준 전략과 비교한다.

## 14.1 Buy and Hold

동일 기간 BTC 보유 성과.

## 14.2 No Trade

수익률 0, 낙폭 0 기준.

## 14.3 RSI Mean Reversion

예시:

```text
RSI 과매도 + Bollinger 하단 이탈
→ LONG 후보

RSI 과매수 + Bollinger 상단 이탈
→ SHORT 후보
```

## 14.4 EMA Trend Following

예시:

```text
가격 > EMA 200
EMA 20 > EMA 50
→ LONG 후보

가격 < EMA 200
EMA 20 < EMA 50
→ SHORT 후보
```

## 14.5 Rule-based Regime Selector

시장 국면을 코드로 분류하고 전략을 선택한다.

Gemini가 유효하려면 최소한 이 기준보다 개선된 결과를 보여야 한다.

---

# 15. 검증 단계

## Phase 0. 정적 설계 검토

Antigravity가 다음을 확인한다.

- 역할 경계가 명확한가
- LLM이 위험 제한을 우회할 수 있는가
- 데이터 누수 가능성이 있는가
- 상태 머신이 완전한가
- 장애 시 기본 동작이 안전한가
- 저장 구조가 재현성을 지원하는가

## Phase 1. 단위 테스트

필수 테스트:

- 기술지표 계산 정확성
- 4시간봉 종료 시점 계산
- time zone 처리
- freshness 판정
- 스키마 검증
- 상태 머신 전이
- 포지션 크기 계산
- 손절 계산
- 최대 손실 계산
- 중복 주문 차단
- kill switch

## Phase 2. Historical Replay

과거 데이터를 순차적으로 재생한다.

주의:

- 각 시점에서 이용 가능했던 정보만 사용
- 미래 캔들, 미래 뉴스, 수정된 데이터 사용 금지
- 프롬프트와 모델 버전 기록
- 동일 입력 반복 호출을 통한 응답 분산 측정

## Phase 3. Shadow Trading

실시간 판단만 수행하고 주문하지 않는다.

최소 저장 항목:

- 판단 시점
- 입력 스냅샷
- Gemini 출력
- 검증 결과
- 가상 진입 가격
- 이후 4h, 8h, 24h, 72h 성과

## Phase 4. Paper Trading

다음 요소를 반영한다.

- 수수료
- 슬리피지
- 주문 지연
- 부분 체결
- 미체결
- funding fee
- stop gap
- 거래소 오류

## Phase 5. 제한적 실거래

다음 조건이 모두 충족된 경우에만 진행한다.

- shadow 기간 충족
- paper 기간 충족
- baseline 대비 개선 확인
- 최대 낙폭 허용 범위 내
- 손실 제한 및 kill switch 테스트 완료
- 장애 시나리오 테스트 완료
- 주문 중복 방지 검증 완료
- 수동 비상 종료 기능 확인
- 극소액 시작

---

# 16. 핵심 검증 질문

Antigravity는 아래 질문에 대해 코드, 테스트, 문서 수준으로 검증해야 한다.

## 16.1 데이터 검증

- 4시간봉은 종가 확정 후에만 사용되는가
- 거래소별 timestamp 기준이 명확한가
- UTC와 로컬 시간 변환에 오류가 없는가
- 지표 계산에 미래 데이터가 포함되지 않는가
- 뉴스 게시 시각이 시장 판단 시각보다 이후가 아닌가
- 수정되거나 재배포된 거시 데이터가 과거 재생에 섞이지 않는가
- 결측치가 자동으로 0으로 대체되지 않는가
- stale data에서 신규 진입이 차단되는가
- 데이터 공급원 간 값 불일치가 탐지되는가
- 데이터 수집 실패 시 이전 값을 재사용하지 않는가

## 16.2 LLM 검증

- 동일 입력을 여러 번 호출했을 때 행동 분산은 어느 정도인가
- temperature와 sampling 설정이 고정되어 있는가
- 모델 버전이 기록되는가
- 프롬프트 버전이 기록되는가
- 구조화 출력 검증 실패율은 얼마인가
- 뉴스 유무에 따라 판단이 지나치게 변하는가
- 직전 판단을 입력할 때 확증 편향이 증가하는가
- 반대 근거가 형식적으로만 생성되는가
- confidence와 실제 성과 사이 상관관계가 있는가
- 자연어 근거와 action이 모순되지 않는가
- 동일한 action에 대해 매번 다른 논리가 생성되는가
- 특정 키워드에 과민 반응하는가
- prompt injection이 뉴스 본문을 통해 유입될 수 있는가

## 16.3 전략 검증

- Gemini가 실제로 baseline보다 성과가 좋은가
- 수익 개선이 거래 횟수 증가 때문은 아닌가
- 수수료 및 슬리피지 이후에도 우위가 남는가
- 특정 시장 국면에서만 성과가 좋은가
- 상승장에만 맞고 하락장에서는 붕괴하지 않는가
- 평균 회귀 진입이 추세 하락에서 반복 손실을 내지 않는가
- 손실 거래를 오래 유지하는 경향이 있는가
- 이전 판단과의 일관성을 이유로 청산을 늦추는가
- 뉴스가 성과 개선에 실제 기여하는가
- 뉴스 제거 버전과 비교했는가
- 파생시장 지표 제거 버전과 비교했는가
- 기술지표만 사용한 Gemini와 비교했는가

## 16.4 위험관리 검증

- Gemini가 레버리지 한도를 우회할 수 있는가
- Gemini 응답과 무관하게 손절이 설정되는가
- 손절 주문 생성 실패 시 포지션이 차단되는가
- 주문 직후 급격한 가격 변동에서 최대 손실이 제한되는가
- gap과 slippage로 예상 손실을 초과할 수 있는가
- 펀딩비가 장기 보유 손익에 반영되는가
- 포지션 상태와 거래소 실제 상태가 불일치할 때 차단되는가
- 일일 손실 한도 도달 후 신규 진입이 가능한가
- kill switch 발동 후 재시작 시 상태가 복구되는가
- DB 저장 실패 상태에서 주문이 실행되는가
- API retry가 중복 주문을 만들 수 있는가

## 16.5 실행 검증

- 주문 요청에 idempotency key가 있는가
- 동일 decision ID로 주문이 두 번 실행되지 않는가
- timeout 발생 시 주문 성공 여부를 재조회하는가
- 부분 체결을 정상 처리하는가
- stop loss와 take profit 주문 간 충돌이 없는가
- 포지션 종료 후 잔여 주문이 취소되는가
- 거래소 연결 재시작 후 상태를 복구하는가
- shadow, paper, live adapter의 인터페이스가 동일한가
- live mode 전환이 단순 환경변수 하나로 우발적으로 가능한가
- live mode에는 별도 명시적 승인 절차가 있는가

## 16.6 평가 검증

- 손익만이 아니라 MDD, Sharpe, Sortino, Calmar를 보는가
- 거래 수가 충분한가
- 국면별 성과를 분리하는가
- confidence 구간별 성과를 비교하는가
- `NO_TRADE`의 기회비용을 평가하는가
- LONG, SHORT의 성과를 분리하는가
- 기대수익과 tail risk를 함께 보는가
- 단일 최고 성과가 아니라 여러 기간의 안정성을 보는가
- prompt 변경 전후를 동일 데이터로 비교하는가
- 모델 변경 전후 성과를 비교하는가
- 반복 추론 결과의 분산을 반영하는가

---

# 17. 필수 실험 설계

## Experiment 1. 기술지표만 사용

```text
Input:
- OHLCV
- 기술지표
- 현재 포지션

제외:
- 뉴스
- 거시정보
- 심리지표
```

목적:

Gemini가 단순 정량 데이터만으로도 baseline보다 나은지 확인한다.

## Experiment 2. 파생시장 지표 추가

```text
Experiment 1
+ funding
+ open interest
+ liquidation
```

목적:

파생시장 정보의 추가 가치 확인.

## Experiment 3. 심리 지표 추가

```text
Experiment 2
+ Fear & Greed
```

목적:

심리 지표가 판단 품질을 개선하는지 확인.

## Experiment 4. 뉴스 추가

```text
Experiment 3
+ curated news
```

목적:

뉴스가 실제 성과를 개선하는지, 단순히 설명만 풍부하게 만드는지 확인.

## Experiment 5. 직전 판단 포함 여부

```text
A: previous_decision 포함
B: previous_decision 제외
```

목적:

일관성 향상과 확증 편향 증가 사이의 trade-off 확인.

## Experiment 6. 반대 근거 강제 여부

```text
A: contradicting_evidence 필수
B: 지지 근거만 요구
```

목적:

반대 근거 강제가 과도한 자신감과 손실 유지 경향을 줄이는지 확인.

## Experiment 7. 반복 추론 안정성

같은 입력을 여러 번 실행한다.

측정 항목:

- action agreement rate
- regime agreement rate
- strategy agreement rate
- confidence variance
- thesis semantic similarity

## Experiment 8. LLM 사용 여부 비교

```text
A: Gemini adaptive selector
B: deterministic regime selector
C: fixed strategy
D: no trade
E: buy and hold
```

---

# 18. 평가 지표

## 18.1 트레이딩 성과

```text
Total Return
CAGR
Maximum Drawdown
Sharpe Ratio
Sortino Ratio
Calmar Ratio
Profit Factor
Win Rate
Average Win
Average Loss
Expected Value per Trade
Turnover
Trade Count
Fee Contribution
Slippage Contribution
Funding Fee Contribution
```

## 18.2 LLM 품질

```text
Schema Validation Rate
Action Agreement Rate
Regime Agreement Rate
Decision Flip Rate
Average Latency
Timeout Rate
Estimated Cost per Decision
Confidence Calibration Error
Contradiction Rate
Invalid Action Rate
```

## 18.3 운영 품질

```text
Data Freshness Failure Rate
Collector Failure Rate
Duplicate Order Rate
State Reconciliation Failure Rate
Risk Rule Violation Count
Kill Switch Trigger Count
Recovery Time
Missing Audit Log Count
```

---

# 19. 주의해야 할 대표 실패 사례

## 19.1 과매도 상태에서 반복 물타기

```text
RSI 과매도
→ LONG
→ 추가 하락
→ 더 강한 과매도
→ 추가 LONG
```

대응:

- 동일 방향 추가 진입 제한
- 손실 중 포지션 확대 금지
- cooldown
- 연속 평균 회귀 손실 후 전략 비활성화

## 19.2 설명 기반 손실 유지

```text
기존 가설의 일관성
→ HOLD
→ 손실 확대
```

대응:

- 일관성보다 invalidation 우선
- 코드 기반 손절
- 손실 포지션 유지 횟수 제한

## 19.3 뉴스 기반 과잉 반응

```text
부정 뉴스
→ 이미 선반영됐다고 판단
→ 역추세 진입
→ 추가 급락
```

대응:

- 뉴스 단독 진입 금지
- 가격 및 파생시장 확인 필수
- 뉴스 제거 실험과 성과 비교

## 19.4 stale data 재사용

```text
외부 API 실패
→ 이전 funding 값 재사용
→ 최신 정보로 오인
```

대응:

- stale flag
- freshness threshold
- 신규 진입 차단

## 19.5 주문 timeout 중복 실행

```text
주문 API timeout
→ 실패로 간주
→ retry
→ 실제로는 첫 주문 성공
→ 중복 포지션
```

대응:

- client order ID
- 주문 상태 재조회
- idempotency
- retry 전 reconciliation

## 19.6 LLM의 손절 제거 제안

```text
반등 가능성이 높으므로 SL 제거
```

대응:

- LLM은 손절 변경 권한 없음
- risk engine만 손절 관리
- protective order가 없으면 신규 진입 불가

## 19.7 과거 데이터 누수

```text
과거 시점 replay
+ 현재 수정된 뉴스 요약
+ 이후 확정된 거시 수치
```

대응:

- point-in-time 데이터 저장
- 수집 당시 원문 및 timestamp 보존
- 수정 이력 관리

---

# 20. Antigravity에 요청할 산출물

Antigravity는 다음 결과물을 작성해야 한다.

## 20.1 아키텍처 리뷰

```text
docs/reviews/architecture_review.md
```

포함 내용:

- 구조적 위험
- 책임 경계 문제
- 누락된 컴포넌트
- 과도한 복잡성
- 단순화 가능한 부분
- 구현 우선순위

## 20.2 데이터 누수 리뷰

```text
docs/reviews/data_leakage_review.md
```

포함 내용:

- 시간 정렬 위험
- 뉴스 및 거시 데이터 누수
- 지표 계산 누수
- replay 설계 문제
- point-in-time 데이터 요구사항

## 20.3 위험관리 리뷰

```text
docs/reviews/risk_review.md
```

포함 내용:

- LLM이 우회 가능한 규칙
- 손절 및 포지션 제한
- kill switch
- 주문 장애
- 상태 불일치
- 최대 손실 시나리오

## 20.4 검증 계획 리뷰

```text
docs/reviews/validation_review.md
```

포함 내용:

- baseline 적절성
- 실험군과 대조군
- 평가 지표
- 표본 크기
- shadow/paper 기간
- 실거래 전 통과 기준

## 20.5 테스트 매트릭스

```text
docs/reviews/test_matrix.md
```

표 형식:

| 영역 | 테스트 | 입력 | 기대 결과 | 실패 시 위험도 |
|---|---|---|---|---|
| 데이터 | stale OHLCV | 4h 이상 지연 | NO_TRADE | Critical |
| LLM | invalid action | `BUY_MORE` | 거부 | High |
| 주문 | timeout 후 retry | 첫 주문 성공 | 중복 없음 | Critical |
| 리스크 | leverage 초과 | 5x 요청 | 2x 이하 제한 | Critical |

## 20.6 구현 Gap 분석

```text
docs/reviews/implementation_gap.md
```

포함 내용:

- 현재 구현된 부분
- 설계와 불일치한 부분
- 미구현 항목
- 즉시 수정할 항목
- 실거래 전 필수 항목
- 선택적 개선 항목

---

# 21. Antigravity 검증 프롬프트

다음 프롬프트와 본 문서를 함께 전달한다.

```text
당신은 시스템 트레이딩, 소프트웨어 아키텍처, LLM 시스템 평가,
데이터 엔지니어링, 리스크 관리 관점의 검증자다.

첨부된 Gemini 기반 시스템 트레이딩 설계 문서를 검토하라.

목표는 구현을 칭찬하는 것이 아니라,
실거래에서 손실이나 시스템 장애를 일으킬 수 있는 결함을 찾는 것이다.

다음 순서로 검증하라.

1. 설계의 책임 경계를 분석하라.
2. Gemini가 위험관리 규칙을 우회할 수 있는 모든 경로를 찾아라.
3. 데이터 시점 정렬과 미래 데이터 누수 가능성을 분석하라.
4. 뉴스, 거시정보, 기술지표 입력의 신뢰성 문제를 찾아라.
5. 상태 머신과 주문 실행에서 발생할 수 있는 race condition,
   duplicate order, partial fill, timeout 문제를 찾아라.
6. shadow, paper, live 단계의 전환 조건이 충분한지 평가하라.
7. baseline과 실험 설계가 Gemini의 실제 추가 가치를 판별할 수 있는지 검토하라.
8. 테스트되지 않은 가정을 목록화하라.
9. Critical, High, Medium, Low로 위험도를 분류하라.
10. 과도한 설계는 단순화하고, 누락된 안전장치는 추가하라.

각 문제에는 반드시 다음을 포함하라.

- 문제 설명
- 발생 가능한 실제 시나리오
- 영향
- 탐지 방법
- 수정 방안
- 필요한 테스트
- 실거래 차단 여부

다음 산출물을 생성하라.

- architecture_review.md
- data_leakage_review.md
- risk_review.md
- validation_review.md
- test_matrix.md
- implementation_gap.md

불명확한 부분을 임의로 안전하다고 가정하지 말고,
검증되지 않은 항목으로 명시하라.
```

---

# 22. 구현 우선순위

## P0: 실거래 이전 필수

- 데이터 timestamp 및 freshness 검증
- LLM 구조화 출력
- 상태 머신
- 코드 기반 위험 제한
- 손절 강제
- idempotent 주문
- decision log
- market snapshot 저장
- shadow execution
- baseline 구현
- historical replay
- failure test
- kill switch

## P1: Paper trading 이전 필수

- 수수료
- 슬리피지
- funding fee
- 부분 체결
- 주문 지연
- 상태 reconciliation
- counterfactual outcome
- repeated inference
- confidence calibration 분석

## P2: 실거래 전 개선

- 뉴스 deduplication
- 출처 신뢰도
- 국면별 전략 비활성화
- drift monitoring
- prompt/model A/B test
- 운영 대시보드
- 자동 리포트
- 수동 승인 모드

---

# 23. 실거래 진입 차단 조건

다음 중 하나라도 충족하지 못하면 live trading으로 전환하지 않는다.

```text
[ ] 데이터 누수 검토 완료
[ ] shadow trading 기간 충족
[ ] paper trading 기간 충족
[ ] baseline 대비 성과 우위 확인
[ ] 수수료와 슬리피지 반영 후 수익성 유지
[ ] 최대 낙폭 허용 범위 내
[ ] 반복 추론 안정성 확인
[ ] 구조화 출력 실패율 허용 범위 내
[ ] 주문 중복 테스트 통과
[ ] 부분 체결 테스트 통과
[ ] timeout 복구 테스트 통과
[ ] kill switch 테스트 통과
[ ] 거래소 상태 reconciliation 통과
[ ] 손절 주문 실패 시나리오 통과
[ ] live mode 명시적 승인 절차 존재
[ ] 수동 비상 종료 절차 문서화
[ ] 운영 로그와 감사 로그 누락 없음
```

---

# 24. 최종 구현 방향

첫 번째 완성 목표는 다음과 같다.

```text
BTCUSDT 4시간봉 종료
→ 정량 데이터와 제한된 외부 정보 수집
→ 구조화된 시장 스냅샷 생성
→ Gemini가 제한된 행동 선택
→ 정책 및 위험 검증
→ shadow 주문 생성
→ 이후 성과와 반사실 성과 저장
→ baseline과 비교
```

초기 성공 기준은 수익이 아니라 다음이다.

1. 재현 가능한 입력과 로그가 남는다.
2. 데이터 누수 없이 replay가 가능하다.
3. Gemini 실패 시 거래가 발생하지 않는다.
4. 위험 한도가 자연어 프롬프트가 아닌 코드로 강제된다.
5. Gemini의 추가 가치가 baseline과 비교 가능하다.
6. 실거래 전 결함을 탐지할 테스트 체계가 존재한다.

실거래는 이 시스템의 첫 번째 목표가 아니라, 검증 완료 후 선택 가능한 마지막 단계다.
