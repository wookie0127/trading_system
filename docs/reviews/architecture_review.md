# Architecture Review

## 1. 개요
본 문서는 `gemini_system_trading_design_validation_plan.md` 설계서에 기술된 아키텍처에 대한 구조적 검토 결과를 포함합니다.

## 2. 역할 경계 분석
- **Gemini**: 정성적 해석, 시장 국면 분류(Regime), 전략 선택(Strategy), 6가지 단일 액션(`LONG`, `SHORT`, `HOLD`, `REDUCE`, `CLOSE`, `NO_TRADE`) 중 선택.
- **결정론적 코드(Deterministic Engine)**: 포지션 사이즈, 레버리지, 손절가(SL), 익절가(TP), 일일 손실 한도, 주문 체결 및 Kill-Switch 제어.
- **평가**: LLM에 포지션 크기나 손절가를 맡기지 않음으로써 환각(Hallucination)에 의한 파멸적 손실 위험을 원천 차단함.

## 3. 구조적 위험 및 극복 방안
- **Race Condition / Concurrent Execution**: 4시간봉 종가 생성 시점과 실시간 리스크 감시 루프 간 포지션 상태 동기화 문제.
  - *해결책*: DB level locking 또는 Single State Manager 패턴 도입.
- **Fail-Closed 안전망**: Gemini API 타임아웃, Schema 파싱 에러, Stale Data 수집 시 무조건 `NO_TRADE` 처리하여 오동작 방지.

## 4. 모듈화 설계
- Collector, Feature Engine, Snapshot Builder, LLM Client, Policy Validator, Risk Engine, Execution Adapter, Storage, Evaluation 독립 패키지화 (`src/trading_system/`).
