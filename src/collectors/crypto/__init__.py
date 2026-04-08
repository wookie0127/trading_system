"""
collectors/crypto/ — 암호화폐 데이터 수집기
  - bitcoin: Binance 1분봉
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[2])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
