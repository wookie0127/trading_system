"""
sync/ — 종목 reference 데이터 갱신
  - kospi200_symbols_sync: KOSPI200 종목 목록 JSON 갱신
  - us_symbols_sync:       S&P500 / NASDAQ100 종목 목록 JSON 갱신
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
