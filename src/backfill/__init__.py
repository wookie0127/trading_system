"""
backfill/ — 과거 데이터 백필 스크립트
  - backfill_manager:        KIS API 페이지 역순 백필
  - intraday_backfill_stock: 특정 종목 1분봉 백필 (yfinance)
  - kospi200_bulk_backfill:  KOSPI200 전 종목 일괄 백필
  - market_bulk_backfill:    다중 마켓 일괄 백필
  - one_month_backfill:      최근 1개월 KOSPI200 백필
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
