"""
collectors/kr/ — 한국 시장 데이터 수집기
  - kospi200_component_collector: KOSPI200 구성종목
  - kospi200_intraday_collector:  KOSPI200 1분봉 (KIS API)
  - investor_flow_collector:      투자자별 매매 동향
  - market_context_collector:     시장 컨텍스트
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[2])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
