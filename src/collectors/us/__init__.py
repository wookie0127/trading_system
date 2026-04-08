"""
collectors/us/ — 미국 시장 데이터 수집기
  - nasdaq_collector:        NASDAQ 지수 1분봉 (yfinance)
  - yahoo_finance_collector: US/KR 종목 일봉·1분봉 (yfinance)
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[2])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
