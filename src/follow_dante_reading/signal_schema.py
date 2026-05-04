from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class TelegramDialog:
    chat_id: int
    title: str | None
    username: str | None
    entity_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ReadingMessage:
    source: str
    chat_id: int | None
    chat_title: str | None
    message_id: int
    posted_at: datetime
    text: str
    raw_text: str
    has_media: bool
    media_path: str | None = None
    view_count: int | None = None
    forward_count: int | None = None
    reactions: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["posted_at"] = self.posted_at.isoformat()
        return payload


@dataclass(slots=True)
class ReadingSignal:
    source: str
    message_id: int
    posted_at: datetime
    chat_id: int | None
    chat_title: str | None
    company_name: str | None
    action: str
    confidence: float
    stop_loss_pct: float | None
    entry_hint: str | None
    rationale_text: str
    summary: str
    raw_text: str
    media_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["posted_at"] = self.posted_at.isoformat()
        return payload


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]
