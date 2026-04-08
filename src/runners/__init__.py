"""
runners/ — CLI 실행 스크립트 및 데몬
  - collect_daily:            일별 전체 수집 파이프라인
  - collect_daily_stock:      국내 주식 일봉 수집
  - collect_stock_data:       단일 종목 수집 (KR/US)
  - intraday_collector_daemon: 분봉 수집 백그라운드 데몬
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
