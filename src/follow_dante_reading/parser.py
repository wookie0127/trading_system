from __future__ import annotations

import re

from follow_dante_reading.signal_schema import ReadingMessage, ReadingSignal


STOP_LOSS_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*%\s*손절")
COMPANY_LINE_PATTERN = re.compile(r"^[A-Za-z0-9가-힣&\-\.\(\)]+$")

BUY_PHRASES = (
    "들어가셔도",
    "매수",
    "매수 가능",
    "매수 관점",
    "비중",
    "담아",
)
SELL_PHRASES = (
    "매도",
    "익절",
    "정리",
    "비중 축소",
)
WATCH_PHRASES = (
    "지지",
    "괜찮을",
    "좋아보",
    "좋네요",
    "관심",
)


def parse_reading_signal(message: ReadingMessage) -> ReadingSignal | None:
    text = (message.text or "").strip()
    if not text:
        return None

    company_name = _extract_company_name(text)
    stop_loss_pct = _extract_stop_loss_pct(text)
    action = _infer_action(text, stop_loss_pct)
    confidence = _infer_confidence(text, company_name, stop_loss_pct, action)

    if not company_name and action == "ignore":
        return None

    return ReadingSignal(
        source=message.source,
        message_id=message.message_id,
        posted_at=message.posted_at,
        chat_id=message.chat_id,
        chat_title=message.chat_title,
        company_name=company_name,
        action=action,
        confidence=confidence,
        stop_loss_pct=stop_loss_pct,
        entry_hint=_infer_entry_hint(text),
        rationale_text=text,
        raw_text=message.raw_text,
        media_path=message.media_path,
    )


def _extract_company_name(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    first_line = lines[0].lstrip("#-• ").strip()
    if len(first_line) <= 20 and COMPANY_LINE_PATTERN.match(first_line):
        return first_line
    return None


def _extract_stop_loss_pct(text: str) -> float | None:
    normalized = text.replace("손절라인", "손절")
    match = STOP_LOSS_PATTERN.search(normalized)
    if not match:
        return None

    value = float(match.group(1))
    return value / 100.0


def _infer_action(text: str, stop_loss_pct: float | None) -> str:
    if any(phrase in text for phrase in SELL_PHRASES):
        return "sell"
    if any(phrase in text for phrase in BUY_PHRASES):
        return "buy_candidate" if stop_loss_pct is not None else "watch"
    if any(phrase in text for phrase in WATCH_PHRASES):
        return "watch"
    return "ignore"


def _infer_entry_hint(text: str) -> str | None:
    if "돌파" in text:
        return "breakout"
    if "지지" in text or "눌림" in text:
        return "pullback"
    if "들어가셔도" in text or "매수" in text:
        return "current_price"
    return None


def _infer_confidence(
    text: str,
    company_name: str | None,
    stop_loss_pct: float | None,
    action: str,
) -> float:
    score = 0.15
    if company_name:
        score += 0.3
    if stop_loss_pct is not None:
        score += 0.25
    if action in {"buy_candidate", "sell"}:
        score += 0.2
    if "펀더멘탈" in text or "지지" in text:
        score += 0.05
    return min(score, 0.95)

