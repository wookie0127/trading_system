import anyio
import os
from pathlib import Path
import sys
import csv
import re
from dataclasses import dataclass
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
from follow_telegram_leading.client import TelegramReadingClient
from follow_telegram_leading.compact import TleadingHistoryCompactor
from follow_telegram_leading.config import CHAT_CONFIG_PATH, load_chat_aliases, resolve_chat_reference
from follow_telegram_leading.parser import parse_reading_signal, parse_reading_signal_with_llm
from follow_telegram_leading.store import ReadingStore
from follow_telegram_leading.trader import TleadingTrader


@dataclass
class ManualTradeCommand:
    stock_name: str
    quantity: int | None = None
    ratio: float | None = None
    trigger_price: int | None = None
    stop_loss_price: int | None = None
    stop_loss_pct: float | None = None


class TleadingReadingOrchestrator:
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
        self.trader = TleadingTrader(notifier=self.notifier, is_mock=is_mock)
        self.use_llm = use_llm
        self.chat_aliases = load_chat_aliases()
        self.logs_dir = CURRENT_DIR.parents[2] / "logs" / "follow_telegram_leading"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.strategy_sessions: dict[int, dict] = {}
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

    def compact_history(
        self,
        target_date: str | None = None,
        output_dir: str | Path | None = None,
    ) -> None:
        result = TleadingHistoryCompactor(output_dir=output_dir).compact(target_date)
        print(
            f"compact_date={result.compact_date.isoformat()} "
            f"messages={result.message_count} "
            f"signals={result.signal_count} "
            f"journal={result.journal_count} "
            f"markdown={result.markdown_path} "
            f"json={result.json_path}"
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
        resolved_chats = self._resolve_chat_list(chat)
        start_msg = f"🚀 **[Tleading Bot]** 리스너를 시작합니다. (대상: {', '.join(map(str, resolved_chats))})"
        logger.info(start_msg)
        if notify:
            await self.notifier.notify_all(start_msg)

        async with anyio.create_task_group() as tg:
            # 수익률 트래킹 루프 및 Discord 커맨드 루프 시작
            tg.start_soon(self.trader.track_holdings_loop, tg)
            tg.start_soon(self.trader.track_trade_prices_loop)
            tg.start_soon(self.trader.track_scheduled_orders_loop)
            tg.start_soon(self.trader.daily_review_loop)
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
                        entities = []
                        for resolved_chat in resolved_chats:
                            entity = await self.client.resolve_entity(resolved_chat)
                            entities.append(entity)
                            logger.info(
                                "🔍 Resolved entity for {}: ID={} Type={}",
                                resolved_chat,
                                getattr(entity, "id", "N/A"),
                                type(entity).__name__,
                            )

                        # 핸들러 등록
                        self.client.client.add_event_handler(_handler, events.NewMessage(chats=entities))

                        logger.info(f"Listening for new Telegram messages from: {resolved_chats}")
                        await self.client.client.run_until_disconnected()

                    except KeyboardInterrupt:
                        stop_msg = "⏹ **[Tleading Bot]** 사용자에 의해 리스너가 중단되었습니다."
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
        disconnect_msg = f"❗ **[Tleading Bot]** 접속 문제 발생: {exc}\n재인증을 시도합니다."
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
            f"📨 *{signal.strategy_name or 'telegram_stock_leading'} Signal*\n"
            f"• 종목: {company}\n"
            f"• 액션: {signal.action}\n"
            f"• 스타일: {signal.trade_style}\n"
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

    def _resolve_chat_list(self, chat: str | int) -> list[str | int]:
        if isinstance(chat, int):
            return [self._resolve_chat(chat)]
        if isinstance(chat, (list, tuple)):
            parts = [str(part).strip() for part in chat if str(part).strip()]
            if not parts:
                raise ValueError("chat reference is empty")
            return [self._resolve_chat(part) for part in parts]

        parts = [part.strip() for part in str(chat).split(",") if part.strip()]
        if not parts:
            raise ValueError("chat reference is empty")

        return [self._resolve_chat(part) for part in parts]

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
                    "🤖 **[Tleading Bot 명령어 안내]**\n\n"
                    "• `!status` (현황, 계좌): 현재 예수금, 보유 종목별 수량/평단가, 예약 주문, 실현 손익 요약\n"
                    "• `!balance` (잔고): 현재 계좌 잔고 및 보유 종목 조회\n"
                    "• `!buy <종목명> <수량> [목표가격] [sl=손절가|손절률%]`: 즉시 또는 목표가 이하 예약 매수\n"
                    "• `!sell <종목명> <수량|전량|절반|30%> [목표가격]`: 즉시 또는 목표가 이상 예약 매도\n"
                    "• `!strategy`: 전략 문의를 시작하고 질문에 답한 뒤 후보를 고르고 셀렉합니다.\n"
                    "• `!help` (도움말): 현재 보고 계신 명령어 가이드 표시\n\n"
                    "💡 **매매 승인 프로세스**\n"
                    "기본 전략에서는 텔레그램 신호 포착 시 승인 요청 메시지가 발송됩니다.\n"
                    "해당 메시지에 `buy`, `sell`, `skip` 또는 `y`, `네` 등으로 답장하면 실제/모의 매매가 집행됩니다.\n"
                    "`TLEADING_SIGNAL_STRATEGY=llm_autonomous` 설정 시 신뢰도 기준을 통과한 LLM 판단은 승인 없이 집행됩니다.\n"
                    "텔레그램 신호는 `cafe_share`, `chart_master_kospi`처럼 채널별 전략명으로 기록됩니다."
                )
                await message.channel.send(help_text)

            elif await self._handle_strategy_workflow(message, raw_content):
                return

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
            command = self._parse_trade_command(raw_content, side=side)
        except ValueError as exc:
            await message.channel.send(str(exc))
            return

        market_handler = self.trader.market_handler
        code = market_handler.get_code(command.stock_name)
        if not code:
            await message.channel.send(f"❌ '{command.stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
            return

        if command.trigger_price is not None:
            order = self.trader.schedule_manual_order(
                company=command.stock_name,
                code=code,
                side=side,
                quantity=command.quantity,
                ratio=command.ratio,
                trigger_price=command.trigger_price,
                stop_loss_price=command.stop_loss_price,
                stop_loss_pct=command.stop_loss_pct,
            )
            target = (
                f"{command.quantity}주"
                if command.quantity is not None
                else f"{int((command.ratio or 0) * 100)}%"
            )
            stop_loss = self._format_stop_loss_command(command)
            await message.channel.send(
                f"🗓️ 예약 주문 등록 완료: {command.stock_name}({code}) {target} "
                f"{'매수' if side == 'buy' else '매도'} / 목표가 {order['trigger_price']:,}원"
                f"{stop_loss}"
            )
            return

        try:
            if side == "buy":
                ok, result_message = await self.trader.place_manual_buy(
                    command.stock_name,
                    code,
                    command.quantity or 0,
                    stop_loss_price=command.stop_loss_price,
                    stop_loss_pct=command.stop_loss_pct,
                )
            else:
                ok, result_message = await self.trader.place_manual_sell(
                    command.stock_name,
                    code,
                    quantity=command.quantity,
                    ratio=command.ratio,
                )
        except ValueError as exc:
            await message.channel.send(f"❌ {exc}")
            return

        if ok:
            await message.channel.send(result_message)
        else:
            await message.channel.send(result_message)

    async def _handle_strategy_workflow(self, message, raw_content: str) -> bool:
        channel_id = message.channel.id
        normalized = raw_content.strip()
        lowered = normalized.lower()
        session = self.strategy_sessions.get(channel_id)

        if lowered in {"!strategy", "strategy", "전략", "!전략"}:
            self.strategy_sessions[channel_id] = {
                "stage": "awaiting_question",
                "question": None,
                "candidates": [],
                "selected": None,
                "created_at": datetime.now().isoformat(),
            }
            await message.channel.send(
                "전략 문의를 한 줄로 입력해주세요.\n"
                "예: `현재 상태 기준 보수적으로 갈지, 공격적으로 갈지`"
            )
            return True

        if lowered.startswith(("!strategy question ", "!strategy 문의 ", "!strategy 질문 ")):
            question = self._extract_command_payload(normalized, 2)
            if not question:
                await message.channel.send("전략 문의 내용이 비어 있습니다.")
                return True
            self.strategy_sessions[channel_id] = {
                "stage": "awaiting_selection",
                "question": question,
                "candidates": await self._build_strategy_candidates(question),
                "selected": None,
                "created_at": datetime.now().isoformat(),
            }
            await self._send_strategy_candidates(message, self.strategy_sessions[channel_id])
            return True

        if lowered.startswith(("!strategy select ", "!select ")):
            index_raw = self._extract_command_payload(normalized, 1 if lowered.startswith("!select ") else 2)
            return await self._select_strategy_candidate(message, session, index_raw)

        if lowered in {"!strategy candidate", "!strategy candidates", "!candidate", "후보", "전략 후보"}:
            if not session or not session.get("question"):
                await message.channel.send("먼저 `!strategy`로 문의를 시작하고 질문을 입력해주세요.")
                return True
            session["stage"] = "awaiting_selection"
            session["candidates"] = await self._build_strategy_candidates(session["question"])
            await self._send_strategy_candidates(message, session)
            return True

        if session and session.get("stage") == "awaiting_question" and not lowered.startswith("!"):
            question = normalized
            session.update(
                {
                    "stage": "awaiting_selection",
                    "question": question,
                    "candidates": await self._build_strategy_candidates(question),
                    "selected": None,
                    "updated_at": datetime.now().isoformat(),
                }
            )
            await message.channel.send(f"전략 문의를 받았습니다: `{question}`")
            await self._send_strategy_candidates(message, session)
            return True

        if session and session.get("stage") == "awaiting_selection" and normalized.isdigit():
            return await self._select_strategy_candidate(message, session, normalized)

        return False

    @staticmethod
    def _extract_command_payload(raw_content: str, split_at: int = 2) -> str:
        parts = raw_content.strip().split(maxsplit=split_at)
        if len(parts) <= split_at:
            return ""
        return parts[split_at].strip()

    async def _send_strategy_candidates(self, message, session: dict) -> None:
        question = session.get("question") or "-"
        candidates = session.get("candidates") or []
        if not candidates:
            await message.channel.send("전략 후보를 만들지 못했습니다.")
            return

        lines = [
            "🧭 **[전략 후보]**",
            f"• 문의: {question}",
            "",
        ]
        for idx, candidate in enumerate(candidates, 1):
            lines.append(f"{idx}. {candidate['title']} - {candidate['reason']}")
        lines.append("")
        lines.append("선택하려면 `1`, `2`, `3` 또는 `!select 2`처럼 입력하세요.")
        await message.channel.send("\n".join(lines))

    async def _select_strategy_candidate(self, message, session: dict | None, index_raw: str) -> bool:
        if not session or not session.get("candidates"):
            await message.channel.send("먼저 `!strategy`로 문의를 시작하세요.")
            return True

        if not index_raw or not re.fullmatch(r"\d+", index_raw.strip()):
            await message.channel.send("선택값은 숫자로 입력하세요. 예: `2` 또는 `!select 2`")
            return True

        index = int(index_raw)
        candidates = session["candidates"]
        if index < 1 or index > len(candidates):
            await message.channel.send(f"선택 범위는 1~{len(candidates)}입니다.")
            return True

        selected = candidates[index - 1]
        session["selected"] = {
            "index": index,
            "title": selected["title"],
            "reason": selected["reason"],
            "selected_at": datetime.now().isoformat(),
        }
        session["stage"] = "selected"
        await message.channel.send(
            "✅ **[전략 셀렉]**\n"
            f"• 선택: {index}. {selected['title']}\n"
            f"• 사유: {selected['reason']}"
        )
        return True

    async def _build_strategy_candidates(self, question: str) -> list[dict[str, str]]:
        active_trades = self.trader._reconcile_active_trades_with_balance()
        holdings_count = len(active_trades)
        aggregate_pnl = 0

        for code, trade in active_trades.items():
            try:
                entry_price = int(trade.get("entry_price") or 0)
                quantity = int(trade.get("quantity") or 0)
                current_price = int(self.trader.market_handler.fetch_price(code).get("output", {}).get("stck_prpr", 0))
                aggregate_pnl += (current_price - entry_price) * quantity
            except Exception as exc:
                logger.warning(f"Failed to inspect holding {code} for strategy candidates: {exc}")

        question_hint = question[:80] if question else "현재 상태"

        if holdings_count == 0:
            return [
                {
                    "title": "현금 대기",
                    "reason": f"{question_hint} 기준으로 보유 종목이 없어 관망을 우선합니다.",
                },
                {
                    "title": "신규 탐색",
                    "reason": "진입 신호가 들어올 때만 소액으로 점검합니다.",
                },
                {
                    "title": "테스트 진입",
                    "reason": "상호 의사전달 확인용으로 최소 규모만 검토합니다.",
                },
            ]

        if aggregate_pnl > 0:
            primary_title = "수익 실현 우선"
            primary_reason = f"{question_hint} 기준으로 이익 구간 포지션의 일부 정리를 먼저 검토합니다."
        elif aggregate_pnl < 0:
            primary_title = "리스크 축소 우선"
            primary_reason = f"{question_hint} 기준으로 손실 포지션의 축소 또는 정리를 먼저 봅니다."
        else:
            primary_title = "보유 유지"
            primary_reason = f"{question_hint} 기준으로 현재 보유를 유지하면서 추가 확인을 합니다."

        return [
            {
                "title": primary_title,
                "reason": primary_reason,
            },
            {
                "title": "보유 종목 점검",
                "reason": f"보유 {holdings_count}개 종목의 손절선과 평단가를 다시 확인합니다.",
            },
            {
                "title": "신규 후보 보류",
                "reason": "현재 포지션 정리가 끝난 뒤에만 새로운 후보를 검토합니다.",
            },
        ]

    @staticmethod
    def _parse_trade_command(raw_content: str, side: str) -> ManualTradeCommand:
        parts = raw_content.strip().split()
        if len(parts) < 3:
            raise ValueError("❌ 형식이 올바르지 않습니다. 예: `!buy 삼성전자 1`")

        parts, stop_loss_price, stop_loss_pct = TleadingReadingOrchestrator._extract_stop_loss_options(parts)
        if len(parts) < 3:
            raise ValueError("❌ 형식이 올바르지 않습니다. 예: `!buy 삼성전자 1 sl=3%`")
        if side != "buy" and (stop_loss_price is not None or stop_loss_pct is not None):
            raise ValueError("❌ 손절 설정은 매수 명령에서만 사용할 수 있습니다.")

        trigger_price = None
        if len(parts) >= 4 and TleadingReadingOrchestrator._looks_like_price(parts[-1]):
            trigger_price = TleadingReadingOrchestrator._parse_trigger_price(parts[-1])
            parts = parts[:-1]

        target_raw = parts[-1]
        stock_name = " ".join(parts[1:-1]).strip()
        if not stock_name:
            raise ValueError("❌ 종목명이 비어 있습니다. 예: `!buy 한미반도체 1`")

        if side == "buy":
            if not target_raw.isdigit():
                raise ValueError("❌ 매수 수량은 양의 정수여야 합니다. 예: `!buy 삼성전자 2`")
            quantity = int(target_raw)
            if quantity <= 0:
                raise ValueError("❌ 매수 수량은 1 이상이어야 합니다.")
            return ManualTradeCommand(
                stock_name=stock_name,
                quantity=quantity,
                trigger_price=trigger_price,
                stop_loss_price=stop_loss_price,
                stop_loss_pct=stop_loss_pct,
            )

        quantity, ratio = TleadingReadingOrchestrator._parse_sell_target(target_raw)
        return ManualTradeCommand(
            stock_name=stock_name,
            quantity=quantity,
            ratio=ratio,
            trigger_price=trigger_price,
        )

    @staticmethod
    def _parse_sell_target(target_raw: str) -> tuple[int | None, float | None]:
        normalized = target_raw.strip().lower()
        normalized = normalized.replace("％", "%")
        alias_map = {
            "all": (None, 1.0),
            "전량": (None, 1.0),
            "전체": (None, 1.0),
            "half": (None, 0.5),
            "절반": (None, 0.5),
            "반": (None, 0.5),
        }
        if normalized in alias_map:
            return alias_map[normalized]

        if normalized.endswith("%"):
            pct_raw = normalized[:-1]
            try:
                pct = float(pct_raw)
            except ValueError as exc:
                raise ValueError("❌ 매도 비율 형식이 올바르지 않습니다. 예: `!sell 삼성전자 30%`") from exc
            if pct <= 0 or pct > 100:
                raise ValueError("❌ 매도 비율은 0 초과 100 이하만 가능합니다.")
            return None, pct / 100

        if normalized.isdigit():
            quantity = int(normalized)
            if quantity <= 0:
                raise ValueError("❌ 매도 수량은 1 이상이어야 합니다.")
            return quantity, None

        raise ValueError("❌ 매도는 수량 또는 `전량`, `절반`, `30%` 형식으로 입력하세요.")

    @staticmethod
    def _looks_like_price(value: str) -> bool:
        normalized = value.replace(",", "").strip()
        return normalized.isdigit()

    @staticmethod
    def _extract_stop_loss_options(parts: list[str]) -> tuple[list[str], int | None, float | None]:
        prefixes = {
            "sl",
            "stop",
            "stop_loss",
            "손절",
            "손절가",
            "손절률",
        }
        remaining = []
        stop_loss_price = None
        stop_loss_pct = None

        for token in parts:
            if "=" not in token:
                remaining.append(token)
                continue

            raw_key, raw_value = token.split("=", 1)
            key = raw_key.strip().lower()
            value = raw_value.strip()
            if key not in prefixes:
                remaining.append(token)
                continue

            parsed_price, parsed_pct = TleadingReadingOrchestrator._parse_stop_loss_value(key, value)
            if parsed_price is not None:
                stop_loss_price = parsed_price
                stop_loss_pct = None
            if parsed_pct is not None:
                stop_loss_pct = parsed_pct
                stop_loss_price = None

        return remaining, stop_loss_price, stop_loss_pct

    @staticmethod
    def _parse_stop_loss_value(key: str, value: str) -> tuple[int | None, float | None]:
        normalized = value.replace(",", "").replace("％", "%").strip()
        if not normalized:
            raise ValueError("❌ 손절값이 비어 있습니다. 예: `sl=3%` 또는 `sl=65000`")

        pct_keys = {"손절률"}
        if normalized.endswith("%") or key in pct_keys:
            pct_raw = normalized[:-1] if normalized.endswith("%") else normalized
            try:
                pct = float(pct_raw)
            except ValueError as exc:
                raise ValueError("❌ 손절률 형식이 올바르지 않습니다. 예: `sl=3%`") from exc
            if pct <= 0 or pct >= 100:
                raise ValueError("❌ 손절률은 0 초과 100 미만이어야 합니다. 예: `sl=3%`")
            return None, pct / 100

        if not normalized.isdigit():
            raise ValueError("❌ 손절가는 숫자여야 합니다. 예: `sl=65000`")

        price = int(normalized)
        if key in {"sl", "stop", "stop_loss", "손절"} and price < 100:
            return None, price / 100
        if price <= 0:
            raise ValueError("❌ 손절가는 1원 이상이어야 합니다.")
        return price, None

    @staticmethod
    def _format_stop_loss_command(command: ManualTradeCommand) -> str:
        if command.stop_loss_price is not None:
            return f" / 손절가 {command.stop_loss_price:,}원"
        if command.stop_loss_pct is not None:
            return f" / 손절률 {command.stop_loss_pct * 100:.1f}%"
        return ""

    @staticmethod
    def _parse_trigger_price(value: str) -> int:
        normalized = value.replace(",", "").strip()
        if not normalized.isdigit():
            raise ValueError("❌ 목표 가격은 숫자여야 합니다. 예: `!buy 삼성전자 1 65000`")
        trigger_price = int(normalized)
        if trigger_price <= 0:
            raise ValueError("❌ 목표 가격은 1원 이상이어야 합니다.")
        return trigger_price


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
    orchestrator = TleadingReadingOrchestrator(
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
            elif mode == "compact":
                orchestrator.compact_history(
                    target_date=start_date,
                    output_dir=output_file,
                )
            else:
                raise ValueError(
                    "mode must be one of: login, check-session, whoami, dialogs, history, listen, serve, dump, relay-history, compact"
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
