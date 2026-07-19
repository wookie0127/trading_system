# Risk Review

## 1. 개요
리스크 관리 엔진 및 안전장치(Safety Guardrails)에 대한 검토 결과입니다.

## 2. LLM 우회 시도 차단
- LLM 응답에 레버리지, 손절가, 포지션 수량이 포함되거나 변경 요청이 오더라도 완전히 무시합니다.
- 포지션 수량은 `Size = AccountBalance * MaxRiskRatio / StopDistance` 공식으로만 계산됩니다.
- 레버리지는 `system.yaml` 및 `risk.yaml`에 정의된 최대값(`max_leverage: 2.0`)으로 하드 통제됩니다.

## 3. Kill-Switch 및 하드 스톱
- 일일 손실 비율 `max_daily_loss_ratio` (2.0%) 초과 시 자동 발동.
- 연속 손실 횟수 `max_consecutive_losses` (4회) 초과 시 자동 발동.
- Stale Data 지연 시 신규 주문 차단.
- 거래소 API 타임아웃 및 reconciliation 실패 시 비상 셧다운.
