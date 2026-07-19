# Implementation Gap Analysis

## 1. 개요
설계 문서(`gemini_system_trading_design_validation_plan.md`) 대비 기존 코드베이스의 구현 갭 분석입니다.

## 2. 주요 구현 갭 분석 및 대응 방안
1. **Gemini 3.5 Structured Output 통합**
   - Gap: 기존 단순 텍스트 프롬프트 사용.
   - 해결: `google-genai` 최신 SDK 및 Pydantic Structured Output (`TradingDecision`)으로 리팩토링.
2. **Deterministic Risk & Policy Layer 분리**
   - Gap: 일부 주문 제어에서 LLM 의존 가능성.
   - 해결: `src/trading_system/policy/` 및 `risk/` 패키지를 통해 완전 하드코드 제어 구조 구축.
3. **Point-in-Time Snapshot Builder 및 Historical Replay**
   - Gap: 과거 `trading_data.db` 캔들의 Replay 시 미래 데이터 누수 위험.
   - 해결: `src/trading_system/snapshots/builder.py` 및 `scripts/run_replay.py`를 신규 작성하여 엄격한 Time-window slicing 적용.
4. **Counterfactual Outcome & Baseline Evaluation**
   - Gap: LLM 판단 가치 검증 지표 부족.
   - 해결: `HOLD`/`NO_TRADE` 가상 수익률 추적 모델 및 5개 Baseline 비교 엔진 추가.
