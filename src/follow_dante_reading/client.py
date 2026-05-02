from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from dotenv import load_dotenv
from loguru import logger

from follow_dante_reading.signal_schema import ReadingMessage, TelegramDialog

try:
    from telethon import TelegramClient, events
    from telethon.tl.custom.dialog import Dialog
    from telethon.tl.custom.message import Message
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "Telethon is required for follow_dante_reading. Install project dependencies first."
    ) from exc


CURRENT_DIR = Path(__file__).resolve().parent
TELEGRAM_ENV_PATH = Path.home() / ".ssh" / "telegram"
load_dotenv(TELEGRAM_ENV_PATH)
load_dotenv(CURRENT_DIR.parents[2] / ".env")

SESSION_BASENAME = CURRENT_DIR / "test_check"


class TelegramReadingClient:
    def __init__(
        self,
        api_id: int | None = None,
        api_hash: str | None = None,
        session_path: str | Path = SESSION_BASENAME,
    ):
        resolved_api_id = api_id or _read_api_id_from_env()
        resolved_api_hash = api_hash or _read_api_hash_from_env()

        if resolved_api_id is None or resolved_api_hash is None:
            raise ValueError(
                "Telegram API credentials are missing. "
                "Set api_id/api_hash or TELEGRAM_API_ID/TELEGRAM_API_HASH in ~/.ssh/telegram."
            )

        self.api_id = resolved_api_id
        self.api_hash = resolved_api_hash
        self.session_path = Path(session_path)
        self.client = TelegramClient(str(self.session_path), self.api_id, self.api_hash)

    async def interactive_login(self) -> None:
        await self.client.start()
        me = await self.client.get_me()
        logger.info(f"Signed in successfully as {getattr(me, 'first_name', None)}")

    async def ensure_authorized(self) -> None:
        await self.client.connect()
        is_authorized = await self.client.is_user_authorized()
        if is_authorized:
            return

        raise RuntimeError(
            "Telegram session is not authorized. "
            "Run login mode once in an interactive terminal to create or refresh the session."
        )

    async def check_session(self) -> bool:
        await self.client.connect()
        return await self.client.is_user_authorized()

    async def get_me_summary(self) -> dict[str, str | int | None]:
        await self.ensure_authorized()
        me = await self.client.get_me()
        return {
            "id": getattr(me, "id", None),
            "first_name": getattr(me, "first_name", None),
            "last_name": getattr(me, "last_name", None),
            "username": getattr(me, "username", None),
            "phone": getattr(me, "phone", None),
        }

    async def list_dialogs(
        self,
        limit: int = 100,
        query: str | None = None,
    ) -> list[TelegramDialog]:
        await self.ensure_authorized()
        dialogs: list[TelegramDialog] = []

        async for dialog in self.client.iter_dialogs(limit=limit):
            item = self._to_dialog_summary(dialog)
            if query and not self._matches_dialog_query(item, query):
                continue
            dialogs.append(item)

        return dialogs

    async def resolve_entity(
        self,
        chat: str | int,
    ):
        await self.ensure_authorized()
        return await self.client.get_entity(chat)

    async def fetch_chat_history(
        self,
        chat: str | int,
        limit: int = 100,
        download_media: bool = True,
        media_dir: str | Path | None = None,
    ) -> list[ReadingMessage]:
        await self.ensure_authorized()
        entity = await self.resolve_entity(chat)
        results: list[ReadingMessage] = []

        async for msg in self.client.iter_messages(entity, limit=limit):
            parsed = await self._to_reading_message(msg, download_media, media_dir)
            if parsed:
                results.append(parsed)

        results.sort(key=lambda item: item.posted_at)
        return results

    async def run_realtime(
        self,
        chat: str | int,
        on_message: Callable[[ReadingMessage], Awaitable[None]],
        download_media: bool = True,
        media_dir: str | Path | None = None,
    ) -> None:
        await self.ensure_authorized()
        entity = await self.resolve_entity(chat)
        logger.info(f"Listening for new Telegram messages from: {chat}")

        @self.client.on(events.NewMessage(chats=entity))
        async def _handler(event):
            parsed = await self._to_reading_message(event.message, download_media, media_dir)
            if parsed:
                await on_message(parsed)

        await self.client.run_until_disconnected()

    async def close(self) -> None:
        await self.client.disconnect()

    async def _to_reading_message(
        self,
        message: Message,
        download_media: bool,
        media_dir: str | Path | None,
    ) -> ReadingMessage | None:
        text = (message.message or "").strip()
        if not text and not message.media:
            return None

        media_path = None
        if message.media and download_media:
            media_path = await self._download_media(message, media_dir)

        reactions = {}
        if message.reactions and getattr(message.reactions, "results", None):
            reactions = {
                reaction.reaction.emoticon: reaction.count
                for reaction in message.reactions.results
                if getattr(reaction.reaction, "emoticon", None)
            }

        chat = await message.get_chat()
        posted_at = message.date
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)

        return ReadingMessage(
            source="telegram:dante",
            chat_id=getattr(chat, "id", None),
            chat_title=getattr(chat, "title", None),
            message_id=message.id,
            posted_at=posted_at.astimezone(),
            text=text,
            raw_text=text,
            has_media=bool(message.media),
            media_path=media_path,
            view_count=getattr(message, "views", None),
            forward_count=getattr(message, "forwards", None),
            reactions=reactions,
        )

    async def _download_media(self, message: Message, media_dir: str | Path | None) -> str | None:
        directory = Path(media_dir) if media_dir else CURRENT_DIR / "downloads"
        directory.mkdir(parents=True, exist_ok=True)
        file_stem = f"{message.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        path = await self.client.download_media(message, file=directory / file_stem)
        return str(path) if path else None

    @staticmethod
    def _to_dialog_summary(dialog: Dialog) -> TelegramDialog:
        entity = dialog.entity
        return TelegramDialog(
            chat_id=getattr(entity, "id", 0),
            title=getattr(dialog, "title", None),
            username=getattr(entity, "username", None),
            entity_type=entity.__class__.__name__,
        )

    @staticmethod
    def _matches_dialog_query(dialog: TelegramDialog, query: str) -> bool:
        needle = query.casefold()
        fields = [
            str(dialog.chat_id),
            dialog.title or "",
            dialog.username or "",
        ]
        return any(needle in field.casefold() for field in fields)


def _read_api_id_from_env() -> int | None:
    raw = os.getenv("api_id") or os.getenv("TELEGRAM_API_ID")
    return int(raw) if raw else None


def _read_api_hash_from_env() -> str | None:
    return os.getenv("api_hash") or os.getenv("TELEGRAM_API_HASH")
