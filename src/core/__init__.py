"""
core/ — KIS 인증, 설정, 시장 API 핸들러
"""
import sys
from pathlib import Path

# src/ 를 sys.path에 등록하여 패키지 간 절대 임포트 보장
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
