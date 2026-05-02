from __future__ import annotations

import asyncio
from pathlib import Path
import sys

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[0]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

import fire
from loguru import logger

from bots.notifier import Notifier
from follow_dante_reading.client import TelegramReadingClient
from follow_dante_reading.config import CHAT_CONFIG_PATH, load_chat_aliases, resolve_chat_reference
from follow_dante_reading.parser import parse_reading_signal
from follow_dante_reading.store import ReadingStore


class DanteReadingOrchestrator:
    def __init__(self, base_dir: str | Path | None = None):
        self.store = ReadingStore(base_dir=base_dir)
        self.notifier = Notifier()
        self.client = TelegramReadingClient()
        self.chat_aliases = load_chat_aliases()
        self.logs_dir = CURRENT_DIR.parents[2] / "logs" / "follow_dante_reading"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self._configure_logging()

    async def list_dialogs(
        self,
        limit: int = 100,
        query: str | None = None,
    ) -> None:
        dialogs = await self.client.list_dialogs(limit=limit, query=query)
        for dialog in dialogs:
            print(
                f"chat_id={dialog.chat_id} "
                f"title={dialog.title or '-'} "
                f"username={dialog.username or '-'} "
                f"type={dialog.entity_type}"
            )

    async def login(self) -> None:
        await self.client.interactive_login()
        print(f"session={self.client.session_path}.session")

    async def check_session(self) -> None:
        is_valid = await self.client.check_session()
        print(f"authorized={is_valid}")
        if not is_valid:
            raise RuntimeError("Session is not authorized. Run login mode first.")

    async def whoami(self) -> None:
        me = await self.client.get_me_summary()
        for key, value in me.items():
            print(f"{key}={value}")

    async def fetch_history(
        self,
        chat: str | int,
        limit: int = 100,
        download_media: bool = True,
        notify: bool = False,
    ) -> None:
        resolved_chat = self._resolve_chat(chat)
        messages = await self.client.fetch_chat_history(
            chat=resolved_chat,
            limit=limit,
            download_media=download_media,
            media_dir=self.store.attachments_dir,
        )
        logger.info(f"Fetched {len(messages)} recent messages from {resolved_chat}")
        for message in messages:
            signal = await self._process_message(message, notify)
            print(self._format_history_line(message, signal))

    async def listen(
        self,
        chat: str | int,
        download_media: bool = True,
        notify: bool = True,
    ) -> None:
        resolved_chat = self._resolve_chat(chat)
        await self.client.run_realtime(
            chat=resolved_chat,
            on_message=lambda message: self._process_message(message, notify),
            download_media=download_media,
            media_dir=self.store.attachments_dir,
        )

    async def listen_forever(
        self,
        chat: str | int,
        download_media: bool = True,
        notify: bool = True,
        retry_delay_seconds: int = 5,
    ) -> None:
        resolved_chat = self._resolve_chat(chat)
        while True:
            try:
                logger.info(f"Starting listener for chat={resolved_chat}")
                await self.listen(
                    chat=resolved_chat,
                    download_media=download_media,
                    notify=notify,
                )
            except KeyboardInterrupt:
                logger.info("Listener interrupted by user")
                raise
            except Exception as exc:
                logger.exception(f"Listener crashed for chat={resolved_chat}: {exc}")
                await asyncio.sleep(retry_delay_seconds)

    async def _process_message(self, message, notify: bool):
        self.store.save_message(message)
        signal = parse_reading_signal(message)
        logger.info(
            f"Captured message id={message.message_id} chat_id={message.chat_id} "
            f"title={message.chat_title} has_media={message.has_media}"
        )
        if not signal or signal.action == "ignore":
            return None

        self.store.save_signal(signal)
        logger.info(
            f"Parsed signal: action={signal.action} company={signal.company_name} "
            f"stop_loss={signal.stop_loss_pct}"
        )
        if notify:
            await self.notifier.notify_all(self._format_signal(signal))
        return signal

    @staticmethod
    def _format_signal(signal) -> str:
        company = signal.company_name or "Unknown"
        stop_loss = "n/a"
        if signal.stop_loss_pct is not None:
            stop_loss = f"{signal.stop_loss_pct * 100:.1f}%"

        return (
            f"📨 *Dante Reading Signal*\n"
            f"• 종목: {company}\n"
            f"• 액션: {signal.action}\n"
            f"• 손절: {stop_loss}\n"
            f"• 신뢰도: {signal.confidence:.2f}\n"
            f"• 원문: {signal.rationale_text[:120]}"
        )

    @staticmethod
    def _format_history_line(message, signal) -> str:
        text = (message.text or "").replace("\n", " ").strip()
        text = text[:100]
        parsed = signal.action if signal else "ignore"
        company = signal.company_name if signal else "-"
        return (
            f"{message.message_id} "
            f"{message.posted_at.isoformat()} "
            f"chat_id={message.chat_id} "
            f"action={parsed} "
            f"company={company} "
            f"text={text}"
        )

    def _resolve_chat(self, chat: str | int) -> str | int:
        return resolve_chat_reference(chat, self.chat_aliases)

    def _configure_logging(self) -> None:
        logger.remove()
        logger.add(sys.stderr, level="INFO")
        logger.add(
            self.logs_dir / "listener.log",
            level="INFO",
            rotation="10 MB",
            retention=10,
            enqueue=True,
        )
        logger.info(
            f"Logging configured. file={self.logs_dir / 'listener.log'} "
            f"chat_config={CHAT_CONFIG_PATH}"
        )


def main(
    mode: str,
    chat: str | int | None = None,
    limit: int = 20,
    download_media: bool = True,
    notify: bool = False,
    query: str | None = None,
    retry_delay_seconds: int = 5,
):
    orchestrator = DanteReadingOrchestrator()

    async def _runner():
        try:
            if mode == "login":
                await orchestrator.login()
            elif mode == "check-session":
                await orchestrator.check_session()
            elif mode == "whoami":
                await orchestrator.whoami()
            elif mode == "dialogs":
                await orchestrator.list_dialogs(limit=limit, query=query)
            elif mode == "history":
                if chat is None:
                    raise ValueError("chat is required for history mode")
                await orchestrator.fetch_history(
                    chat=chat,
                    limit=limit,
                    download_media=download_media,
                    notify=notify,
                )
            elif mode == "listen":
                if chat is None:
                    raise ValueError("chat is required for listen mode")
                await orchestrator.listen(
                    chat=chat,
                    download_media=download_media,
                    notify=notify,
                )
            elif mode == "serve":
                if chat is None:
                    raise ValueError("chat is required for serve mode")
                await orchestrator.listen_forever(
                    chat=chat,
                    download_media=download_media,
                    notify=notify,
                    retry_delay_seconds=retry_delay_seconds,
                )
            else:
                raise ValueError(
                    "mode must be one of: login, check-session, whoami, dialogs, history, listen, serve"
                )
        finally:
            await orchestrator.client.close()

    asyncio.run(_runner())


if __name__ == "__main__":
    fire.Fire(main)
