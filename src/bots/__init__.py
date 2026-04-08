"""
bots/ — 알림 봇 (Slack, Discord) 및 공통 Notifier
"""
import sys
from pathlib import Path
_SRC = str(Path(__file__).parents[1])
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
