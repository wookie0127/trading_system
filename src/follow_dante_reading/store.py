from __future__ import annotations

import json
from pathlib import Path

from follow_dante_reading.signal_schema import ReadingMessage, ReadingSignal, project_root


class ReadingStore:
    def __init__(self, base_dir: str | Path | None = None):
        root = Path(base_dir) if base_dir else project_root() / "data" / "follow_dante_reading"
        self.base_dir = root
        self.attachments_dir = root / "attachments"
        self.messages_path = root / "telegram_messages.jsonl"
        self.signals_path = root / "reading_signals.jsonl"
        self.attachments_dir.mkdir(parents=True, exist_ok=True)

    def save_message(self, message: ReadingMessage) -> None:
        self._append_jsonl(self.messages_path, message.to_dict())

    def save_signal(self, signal: ReadingSignal) -> None:
        self._append_jsonl(self.signals_path, signal.to_dict())

    @staticmethod
    def _append_jsonl(path: Path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

