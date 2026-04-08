"""
collectors/ — 시장 데이터 수집기 (KR, US, Crypto)
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
