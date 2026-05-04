from __future__ import annotations

import anyio
from pathlib import Path
import sys
import csv
from datetime import datetime
from tenacity import AsyncRetrying, wait_exponential, stop_never, retry_if_exception_type
from telethon import events

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[0]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

import fire
from loguru import logger

from bots.notifier import Notifier
from follow_dante_reading.client import TelegramReadingClient
from follow_dante_reading.config import CHAT_CONFIG_PATH, load_chat_aliases, resolve_chat_reference
from follow_dante_reading.parser import parse_reading_signal, parse_reading_signal_with_llm
from follow_dante_reading.store import ReadingStore
from follow_dante_reading.trader import DanteTrader


class DanteReadingOrchestrator:
    def __init__(self, base_dir: str | Path | None = None, is_mock: bool = False, use_llm: bool = False):
        self.store = ReadingStore(base_dir=base_dir)
        self.notifier = Notifier()
        self.client = TelegramReadingClient()
        self.trader = DanteTrader(notifier=self.notifier, is_mock=is_mock)
        self.use_llm = use_llm
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

    async def dump_to_csv(
        self,
        chat: str | int,
        limit: int = 500,
        output_file: str | Path | None = None,
    ) -> None:
        """메시지 이력을 CSV 파일로 덤프합니다."""
        resolved_chat = self._resolve_chat(chat)
        messages = await self.client.fetch_chat_history(
            chat=resolved_chat,
            limit=limit,
            download_media=True,
            media_dir=self.store.attachments_dir,
        )

        if not output_file:
            dump_dir = self.store.base_dir / "dumps"
            dump_dir.mkdir(parents=True, exist_ok=True)
            output_file = dump_dir / f"dump_{resolved_chat}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        output_path = Path(output_file)

        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Date", "ChatID", "ChatTitle", "Text", "Signal_Action", "Company", "Media_Path"])

            for msg in messages:
                signal = parse_reading_signal(msg) # 덤프 시에는 빠른 규칙 기반 분석 결과 포함
                writer.writerow([
                    msg.message_id,
                    msg.posted_at.isoformat(),
                    msg.chat_id,
                    msg.chat_title,
                    msg.text,
                    signal.action if signal else "n/a",
                    signal.company_name if signal else "n/a",
                    msg.media_path or ""
                ])

        logger.info(f"Successfully dumped {len(messages)} messages to {output_path}")
        print(f"Dump completed: {output_path}")

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
        start_msg = f"🚀 **[Dante Bot]** 리스너를 시작합니다. (대상: {resolved_chat})"
        logger.info(start_msg)
        if notify:
            await self.notifier.notify_all(start_msg)

        async with anyio.create_task_group() as tg:
            # 수익률 트래킹 루프 시작
            tg.start_soon(self.trader.track_holdings_loop, tg)

            # tenacity를 이용한 리스너 재시도 로직
            async for attempt in AsyncRetrying(
                wait=wait_exponential(multiplier=1, min=retry_delay_seconds, max=60),
                stop=stop_never,
                retry=retry_if_exception_type(Exception),
                before_sleep=lambda retry_state: self._before_retry_callback(retry_state, notify)
            ):
                with attempt:
                    try:
                        await self.client.ensure_authorized()
                        entity = await self.client.resolve_entity(resolved_chat)

                        @self.client.client.on(events.NewMessage(chats=entity))
                        async def _handler(event):
                            parsed = await self.client._to_reading_message(event.message, download_media, self.store.attachments_dir)
                            if parsed:
                                tg.start_soon(self._process_message, parsed, notify, tg)

                        logger.info(f"Listening for new Telegram messages from: {resolved_chat}")
                        await self.client.client.run_until_disconnected()

                    except KeyboardInterrupt:
                        stop_msg = "⏹ **[Dante Bot]** 사용자에 의해 리스너가 중단되었습니다."
                        logger.info(stop_msg)
                        if notify:
                            await self.notifier.notify_all(stop_msg)
                        tg.cancel_scope.cancel()
                        raise
                    except Exception as exc:
                        # 세션/연결 문제일 경우에만 재인증 시도 로직 수행
                        if any(k in str(exc).lower() for k in ["authorized", "session", "connection"]):
                            await self._handle_session_error(exc, notify)
                        raise # tenacity가 재시도를 결정하도록 예외를 다시 던짐

    async def _handle_session_error(self, exc, notify):
        """세션 에러 시 Discord 인증 등을 처리합니다."""
        disconnect_msg = f"❗ **[Dante Bot]** 접속 문제 발생: {exc}\n재인증을 시도합니다."
        logger.warning(disconnect_msg)
        if notify:
            await self.notifier.notify_all(disconnect_msg)
        try:
            await self.client.ensure_authorized(interactive=True, via_discord=True)
        except Exception as re_auth_exc:
            logger.error(f"Re-authorization failed: {re_auth_exc}")

    def _before_retry_callback(self, retry_state, notify):
        """재시도 직전 호출되는 콜백"""
        exc = retry_state.outcome.exception()
        wait_time = retry_state.next_action.sleep
        log_msg = f"⚠️ 리스너 오류 발생 ({exc}). {wait_time:.1f}초 후 재시도합니다. (시도 횟수: {retry_state.attempt_number})"
        logger.warning(log_msg)
        # Discord 알림은 너무 빈번할 수 있으므로 로그만 남기거나 선택적으로 전송

    async def _process_message(self, message, notify: bool, tg: anyio.abc.TaskGroup | None = None):
        self.store.save_message(message)

        if self.use_llm:
            signal = parse_reading_signal_with_llm(message)
        else:
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

        # 매매 신호 처리 (트레이더 호출, TaskGroup 전달)
        await self.trader.handle_signal(signal, tg=tg)

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
    via_discord: bool = False,
    mock: bool = False,
    use_llm: bool = False,
):
    orchestrator = DanteReadingOrchestrator(is_mock=mock, use_llm=use_llm)

    async def _runner():
        try:
            if mode == "login":
                await orchestrator.client.interactive_login(via_discord=via_discord)
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
                async with anyio.create_task_group() as tg:
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
            elif mode == "dump":
                if chat is None:
                    raise ValueError("chat is required for dump mode")
                await orchestrator.dump_to_csv(chat=chat, limit=limit)
            else:
                raise ValueError(
                    "mode must be one of: login, check-session, whoami, dialogs, history, listen, serve, dump"
                )
        finally:
            await orchestrator.client.close()

    anyio.run(_runner)


if __name__ == "__main__":
    fire.Fire(main)
