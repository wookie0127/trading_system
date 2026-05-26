import anyio
import os
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
import discord
from follow_dante_reading.client import TelegramReadingClient
from follow_dante_reading.config import CHAT_CONFIG_PATH, load_chat_aliases, resolve_chat_reference
from follow_dante_reading.parser import parse_reading_signal, parse_reading_signal_with_llm
from follow_dante_reading.store import ReadingStore
from follow_dante_reading.trader import DanteTrader


class DanteReadingOrchestrator:
    def __init__(
        self,
        base_dir: str | Path | None = None,
        is_mock: bool = False,
        use_llm: bool = False,
        use_temp_session: bool = False,
    ):
        self.store = ReadingStore(base_dir=base_dir)
        self.notifier = Notifier()
        self.client = TelegramReadingClient(use_temp_session=use_temp_session)
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
        start_date: str | None = None,
        end_date: str | None = None,
        download_media: bool = True,
    ) -> None:
        resolved_chat = self._resolve_chat(chat)
        start_dt = _parse_date_arg(start_date, end_of_day=False)
        end_dt = _parse_date_arg(end_date, end_of_day=True)
        messages = await self.client.fetch_chat_history(
            chat=resolved_chat,
            limit=limit,
            start_date=start_dt,
            end_date=end_dt,
            download_media=download_media,
            media_dir=self.store.attachments_dir,
        )
        logger.info(
            f"Fetched {len(messages)} messages from {resolved_chat} "
            f"(start_date={start_date}, end_date={end_date}, limit={limit})"
        )
        for message in messages:
            signal = parse_reading_signal(message)
            print(self._format_history_line(message, signal))

    async def dump_to_csv(
        self,
        chat: str | int,
        limit: int = 500,
        start_date: str | None = None,
        end_date: str | None = None,
        output_file: str | Path | None = None,
    ) -> None:
        """메시지 이력을 CSV 파일로 덤프합니다."""
        resolved_chat = self._resolve_chat(chat)
        start_dt = _parse_date_arg(start_date, end_of_day=False)
        end_dt = _parse_date_arg(end_date, end_of_day=True)
        messages = await self.client.fetch_chat_history(
            chat=resolved_chat,
            limit=limit,
            start_date=start_dt,
            end_date=end_dt,
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

        logger.info(
            f"Successfully dumped {len(messages)} messages to {output_path} "
            f"(start_date={start_date}, end_date={end_date}, limit={limit})"
        )
        print(f"Dump completed: {output_path}")

    async def relay_history_to_discord(
        self,
        chat: str | int,
        discord_channel_id: str,
        limit: int = 100,
        start_date: str | None = None,
        end_date: str | None = None,
        download_media: bool = True,
    ) -> None:
        resolved_chat = self._resolve_chat(chat)
        start_dt = _parse_date_arg(start_date, end_of_day=False)
        end_dt = _parse_date_arg(end_date, end_of_day=True)
        messages = await self.client.fetch_chat_history(
            chat=resolved_chat,
            limit=limit,
            start_date=start_dt,
            end_date=end_dt,
            download_media=download_media,
            media_dir=self.store.attachments_dir,
        )

        logger.info(
            f"Relaying {len(messages)} messages from {resolved_chat} to Discord channel {discord_channel_id} "
            f"(start_date={start_date}, end_date={end_date}, limit={limit})"
        )

        for index, message in enumerate(messages, 1):
            payload = _format_discord_archive_message(message)
            chunks = _split_discord_text(payload)
            attachment_paths = [message.media_path] if message.media_path else None

            for chunk_index, chunk in enumerate(chunks, 1):
                send_attachments = attachment_paths if chunk_index == len(chunks) else None
                await self.notifier.send_discord_async(
                    chunk,
                    channel_id=discord_channel_id,
                    attachment_paths=send_attachments,
                )

            logger.info(
                f"Relayed message {index}/{len(messages)} "
                f"(telegram_message_id={message.message_id}, attachments={bool(attachment_paths)})"
            )
            await anyio.sleep(0.4)

        print(
            f"Relayed {len(messages)} messages from {resolved_chat} "
            f"to Discord channel {discord_channel_id}"
        )

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
            # 수익률 트래킹 루프 및 Discord 커맨드 루프 시작
            tg.start_soon(self.trader.track_holdings_loop, tg)
            tg.start_soon(self.trader.track_trade_prices_loop)
            tg.start_soon(self._run_discord_command_loop)

            # 텔레그램 이벤트 핸들러 정의
            async def _handler(event):
                logger.info(f"🔥 [EVENT] New Telegram message detected (ID: {event.message.id})")
                parsed = await self.client._to_reading_message(event.message, download_media, self.store.attachments_dir)
                if parsed:
                    logger.info(f"✅ [PARSED] Message {parsed.message_id} converted to ReadingMessage")
                    tg.start_soon(self._process_message, parsed, notify, tg)
                else:
                    logger.warning(f"⚠️ [SKIP] Failed to convert message {event.message.id} to ReadingMessage")

            # tenacity를 이용한 리스너 재시도 로직
            async for attempt in AsyncRetrying(
                wait=wait_exponential(multiplier=1, min=retry_delay_seconds, max=60),
                stop=stop_never,
                retry=retry_if_exception_type(Exception),
                before_sleep=lambda retry_state: self._before_retry_callback(retry_state, notify)
            ):
                with attempt:
                    try:
                        # 기존 핸들러가 있다면 제거 (중복 방지)
                        self.client.client.remove_event_handler(_handler)

                        await self.client.ensure_authorized()
                        entity = await self.client.resolve_entity(resolved_chat)
                        logger.info(f"🔍 Resolved entity for {resolved_chat}: ID={getattr(entity, 'id', 'N/A')} Type={type(entity).__name__}")

                        # 핸들러 등록
                        self.client.client.add_event_handler(_handler, events.NewMessage(chats=entity))

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
        logger.info(f"📩 New message received: id={message.message_id} from={message.chat_title}")
        self.store.save_message(message)

        # 1. 원문 수신 알림 (디버깅용)
        if notify:
            raw_msg = f"📩 **[Telegram Received]**\n• Chat: {message.chat_title}\n• Text: {message.text[:300]}"
            await self.notifier.notify_diary(raw_msg)

        if self.use_llm:
            signal = parse_reading_signal_with_llm(message)
        else:
            signal = parse_reading_signal(message)

        if not signal:
            logger.warning(f"Failed to parse signal for message {message.message_id}")
            return None

        self.store.save_signal(signal)
        logger.info(
            f"Parsed signal: action={signal.action} company={signal.company_name} "
            f"summary_len={len(signal.summary) if signal.summary else 0}"
        )

        # 2. 파싱된 신호 알림 (ignore가 아닐 때만)
        if signal.action != "ignore" and notify:
            formatted_signal = self._format_signal(signal)
            await self.notifier.notify_diary(formatted_signal)
            await self.notifier.notify_all(formatted_signal)

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

    async def _run_discord_command_loop(self):
        """Discord 명령어를 처리하는 루프입니다."""
        token = os.environ.get("DISCORD_TOKEN") or os.environ.get("DISCORD_BOT_TOKEN")
        if not token:
            logger.warning("Discord token missing. Command handler will not start.")
            return

        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            logger.info("Discord Command Handler is ready.")

        @client.event
        async def on_message(message):
            if message.author.bot:
                return

            raw_content = message.content.strip()
            content = raw_content.lower()
            if content in ["status", "!status", "현황", "계좌"]:
                logger.info(f"Received status command from {message.author}")
                try:
                    report = await self.trader.get_status_report()
                    await message.channel.send(report)
                except Exception as e:
                    logger.error(f"Error generating status report: {e}")
                    await message.channel.send(f"❌ 현황 조회 중 오류가 발생했습니다: {e}")

            elif content in ["!balance", "balance", "잔고"]:
                logger.info(f"Received balance command from {message.author}")
                try:
                    report = await self.trader.get_status_report()
                    await message.channel.send(report)
                except Exception as e:
                    logger.error(f"Error generating balance report: {e}")
                    await message.channel.send(f"❌ 잔고 조회 중 오류가 발생했습니다: {e}")

            elif content.startswith("!buy "):
                await self._handle_manual_trade_command(message, raw_content, side="buy")

            elif content.startswith("!sell "):
                await self._handle_manual_trade_command(message, raw_content, side="sell")

            elif content in ["help", "!help", "도움말", "도움"]:
                help_text = (
                    "🤖 **[Dante Bot 명령어 안내]**\n\n"
                    "• `!status` (현황, 계좌): 현재 예수금, 보유 종목 수익률, 실현 손익 요약\n"
                    "• `!balance` (잔고): 현재 계좌 잔고 및 보유 종목 조회\n"
                    "• `!buy <종목명> <수량>`: KIS 모의투자/실계좌로 시장가 매수\n"
                    "• `!sell <종목명> <수량>`: KIS 모의투자/실계좌로 시장가 매도\n"
                    "• `!help` (도움말): 현재 보고 계신 명령어 가이드 표시\n\n"
                    "💡 **매매 승인 프로세스**\n"
                    "텔레그램 신호 포착 시 승인 요청 메시지가 발송됩니다.\n"
                    "해당 메시지에 `buy`, `sell`, `skip` 또는 `y`, `네` 등으로 답장하면 실제/모의 매매가 집행됩니다."
                )
                await message.channel.send(help_text)

        try:
            await client.start(token)
        except anyio.get_cancelled_exc_class():
            raise
        except Exception as e:
            logger.error(f"Error in Discord command loop: {e}")
        finally:
            if not client.is_closed():
                await client.close()

    async def _handle_manual_trade_command(self, message, raw_content: str, side: str) -> None:
        try:
            stock_name, quantity = self._parse_trade_command(raw_content)
        except ValueError as exc:
            await message.channel.send(str(exc))
            return

        market_handler = self.trader.market_handler
        code = market_handler.get_code(stock_name)
        if not code:
            await message.channel.send(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
            return

        verb = "매수" if side == "buy" else "매도"
        try:
            if side == "buy":
                res = market_handler.create_market_buy_order(code, quantity)
            else:
                res = market_handler.create_market_sell_order(code, quantity)
        except Exception as exc:
            logger.error(f"Manual {side} command failed: {exc}")
            await message.channel.send(f"❌ {verb} 주문 중 오류가 발생했습니다: {exc}")
            return

        if res.get("rt_cd") == "0":
            price = self.trader._extract_price(res, fallback_code=code)
            if side == "buy":
                self.trader.record_executed_buy(stock_name, code, quantity, price)
            else:
                self.trader.record_executed_sell(stock_name, code, quantity, price)
            await message.channel.send(
                f"✅ {verb} 주문 성공: {stock_name}({code}) {quantity}주"
            )
        else:
            error_msg = res.get("msg1") or res.get("msg_cd") or "알 수 없는 오류"
            await message.channel.send(
                f"❌ {verb} 주문 실패: {stock_name}({code}) {quantity}주, {error_msg}"
            )

    @staticmethod
    def _parse_trade_command(raw_content: str) -> tuple[str, int]:
        parts = raw_content.strip().split()
        if len(parts) < 3:
            raise ValueError("❌ 형식이 올바르지 않습니다. 예: `!buy 삼성전자 1`")

        quantity_raw = parts[-1]
        if not quantity_raw.isdigit():
            raise ValueError("❌ 수량은 양의 정수여야 합니다. 예: `!sell 삼성전자 2`")

        quantity = int(quantity_raw)
        if quantity <= 0:
            raise ValueError("❌ 수량은 1 이상이어야 합니다.")

        stock_name = " ".join(parts[1:-1]).strip()
        if not stock_name:
            raise ValueError("❌ 종목명이 비어 있습니다. 예: `!buy 한미반도체 1`")

        return stock_name, quantity


def main(
    mode: str,
    chat: str | int | None = None,
    limit: int = 20,
    start_date: str | None = None,
    end_date: str | None = None,
    output_file: str | None = None,
    discord_channel_id: str | None = None,
    download_media: bool = True,
    notify: bool = False,
    query: str | None = None,
    retry_delay_seconds: int = 5,
    via_discord: bool = False,
    mock: bool = False,
    use_llm: bool = False,
):
    use_temp_session = mode in {"dialogs", "whoami", "history", "dump", "relay-history"}
    orchestrator = DanteReadingOrchestrator(
        is_mock=mock,
        use_llm=use_llm,
        use_temp_session=use_temp_session,
    )

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
                    start_date=start_date,
                    end_date=end_date,
                    download_media=download_media,
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
                await orchestrator.dump_to_csv(
                    chat=chat,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                    output_file=output_file,
                )
            elif mode == "relay-history":
                if chat is None:
                    raise ValueError("chat is required for relay-history mode")
                if not discord_channel_id:
                    raise ValueError("discord_channel_id is required for relay-history mode")
                await orchestrator.relay_history_to_discord(
                    chat=chat,
                    discord_channel_id=discord_channel_id,
                    limit=limit,
                    start_date=start_date,
                    end_date=end_date,
                    download_media=download_media,
                )
            else:
                raise ValueError(
                    "mode must be one of: login, check-session, whoami, dialogs, history, listen, serve, dump, relay-history"
                )
        finally:
            await orchestrator.client.close()

    anyio.run(_runner)


def _parse_date_arg(raw: str | None, end_of_day: bool) -> datetime | None:
    if not raw:
        return None

    normalized = raw.strip()
    if not normalized:
        return None

    if "T" in normalized or " " in normalized:
        return datetime.fromisoformat(normalized)

    base = datetime.fromisoformat(normalized)
    if end_of_day:
        return base.replace(hour=23, minute=59, second=59, microsecond=999999)
    return base.replace(hour=0, minute=0, second=0, microsecond=0)


def _format_discord_archive_message(message) -> str:
    lines = [
        "📚 **[Telegram Archive]**",
        f"• Chat: {message.chat_title or '-'}",
        f"• Message ID: {message.message_id}",
        f"• Posted At: {message.posted_at.isoformat()}",
    ]

    if message.view_count is not None:
        lines.append(f"• Views: {message.view_count}")
    if message.forward_count is not None:
        lines.append(f"• Forwards: {message.forward_count}")
    if message.reactions:
        reactions = ", ".join(f"{emoji} {count}" for emoji, count in message.reactions.items())
        lines.append(f"• Reactions: {reactions}")

    text = message.text.strip() if message.text else ""
    if text:
        lines.append("")
        lines.append(text)

    if message.media_path:
        lines.append("")
        lines.append(f"• Media Path: `{message.media_path}`")

    return "\n".join(lines)


def _split_discord_text(text: str, max_length: int = 1900) -> list[str]:
    if len(text) <= max_length:
        return [text]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > max_length:
        split_at = remaining.rfind("\n", 0, max_length)
        if split_at <= 0:
            split_at = max_length
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()

    if remaining:
        chunks.append(remaining)
    return chunks


if __name__ == "__main__":
    fire.Fire(main)
