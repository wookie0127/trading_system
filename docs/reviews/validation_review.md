# Validation Review

## 1. 개요
Gemini 기반 트레이딩 의사결정의 유효성과 우위를 정량적으로 평가하기 위한 검증 계획 검토서입니다.

## 2. Baseline 전략군
1. **Buy and Hold**: 동일 기간 BTC 단순 보유
2. **No Trade**: 현금 100% (수익률 0%, MDD 0%)
3. **RSI Mean Reversion Rule**: 과매수/과매도 기반 기계적 매매
4. **EMA Trend Following Rule**: 이동평균선 배열 기반 추세 추종
5. **Deterministic Rule Selector**: 정량 지표 기반 국면 선택 규칙 엔진

## 3. 평가 지표
- **수익/리스크**: Sharpe Ratio, Sortino Ratio, Calmar Ratio, MDD, Win Rate, Profit Factor
- **LLM 품질**: Schema Validation Pass Rate, Action Consistency Rate, Confidence Calibration Error
- **Counterfactual Outcomes**: `HOLD` 또는 `NO_TRADE` 선택 시 4h, 24h, 72h 후의 가상 수익률 추적
