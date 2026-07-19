# Test Matrix

| 영역 | 테스트 항목 | 입력조건 | 기대 결과 | 실패시 위험도 |
|---|---|---|---|---|
| Data | Stale Candle Check | 4h 이상 캔들 수집 지연 | `is_complete=False` & `NO_TRADE` | Critical |
| Data | Look-ahead Bias | 03:59:59 미완성 캔들 지표 계산 | 예외 발생 또는 차단 | Critical |
| LLM | Schema Parsing Fail | Malformed JSON 반환 | `NO_TRADE` (Fail-closed) | High |
| LLM | Invalid Action | `OPEN_LONG_2X` 등 스키마 외 문자열 | Pydantic ValidationError & `NO_TRADE` | High |
| Policy | Position State Violation | `LONG` 상태에서 `OPEN_SHORT` | State Machine Error (`INVALID`) -> Rejected | High |
| Risk | Max Leverage Violation | Gemini 프롬프트에서 10x 요청 | 결정론적 2.0x 하드 제한 | Critical |
| Risk | Stop Loss Generation | Entry 진입 시 | SL 주문 생성 및 리스크 비율(0.5%) 준수 | Critical |
| Execution | Duplicate Order Check | 동일 Decision ID 재전송 | Client Order ID 중복 차단 | Critical |
| Execution | Timeout Recovery | API 타임아웃 발생 | State reconciliation 후 재조회 | High |
| Storage | Audit Log Integrity | DB 저장 실패 시 | 신규 거래 완전 차단 (Fail-closed) | Critical |
