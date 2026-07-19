import anyio
import anyio.abc
import json
import math
import os
import re
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger
from tenacity import (
    AsyncRetrying,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

from core.kis_market_handler import MarketHandler
from bots.notifier import Notifier
from follow_telegram_leading.client import get_discord_input
from follow_telegram_leading.signal_schema import ReadingSignal, project_root


FUTURES_QUANTITY_PATTERN = re.compile(r"(\d+)\s*계약")


class TleadingTrader:
    def __init__(self, notifier: Notifier | None = None, is_mock: bool = False):
        self.market_handler = MarketHandler()
        self.notifier = notifier or Notifier()
        self.active_trades_path = (
            project_root() / "data" / "follow_telegram_leading" / "active_trades.json"
        )
        self.trade_history_path = (
            project_root() / "data" / "follow_telegram_leading" / "trade_history.json"
        )
        self.trade_tracking_path = (
            project_root()
            / "data"
            / "follow_telegram_leading"
            / "trade_price_tracking.json"
        )
        self.scheduled_orders_path = (
            project_root()
            / "data"
            / "follow_telegram_leading"
            / "scheduled_orders.json"
        )
        self.autonomous_state_path = (
            project_root()
            / "data"
            / "follow_telegram_leading"
            / "autonomous_strategy_state.json"
        )
        self.kospi_futures_state_path = (
            project_root()
            / "data"
            / "follow_telegram_leading"
            / "kospi_futures_state.json"
        )
        self.investment_journal_path = (
            project_root()
            / "data"
            / "follow_telegram_leading"
            / "investment_journal.jsonl"
        )
        self.daily_reviews_path = (
            project_root() / "data" / "follow_telegram_leading" / "daily_reviews.json"
        )
        self.obsidian_diary_dir = Path(
            os.getenv(
                "TLEADING_OBSIDIAN_DIARY_DIR",
                "/Users/giwooklee/Documents/Obsidian Vault/TradingSystem/invest_diary",
            )
        )
        self.active_trades_path.parent.mkdir(parents=True, exist_ok=True)
        self.is_mock = is_mock
        self.market_timezone = ZoneInfo(
            os.getenv("TLEADING_MARKET_TIMEZONE", "Asia/Seoul")
        )
        self.market_open_time = time(9, 0)
        self.market_close_time = time(15, 30)
        self.default_stop_loss_pct = self._parse_stop_loss_pct(
            os.getenv("TLEADING_DEFAULT_STOP_LOSS_PCT", "5%")
        )
        self.default_stop_loss_price = self._parse_stop_loss_price(
            os.getenv("TLEADING_DEFAULT_STOP_LOSS_PRICE")
        )
        self.order_quantity = int(os.getenv("TLEADING_ORDER_QUANTITY", "1"))
        self.holdings_poll_seconds = int(
            os.getenv("TLEADING_HOLDINGS_POLL_SECONDS", "300")
        )
        self.price_tracking_minutes = int(
            os.getenv("TLEADING_PRICE_TRACKING_MINUTES", "15")
        )
        self.scheduled_orders_poll_seconds = int(
            os.getenv("TLEADING_SCHEDULED_ORDERS_POLL_SECONDS", "5")
        )
        self.auto_stop_loss_enabled = (
            os.getenv("TLEADING_AUTO_STOP_LOSS_ENABLED", "true").lower() == "true"
        )
        self.signal_strategy = (
            os.getenv("TLEADING_SIGNAL_STRATEGY", "confirm").strip().lower()
        )
        self.llm_auto_buy_min_confidence = float(
            os.getenv("TLEADING_LLM_AUTO_BUY_MIN_CONFIDENCE", "0.85")
        )
        self.llm_auto_sell_min_confidence = float(
            os.getenv("TLEADING_LLM_AUTO_SELL_MIN_CONFIDENCE", "0.75")
        )
        self.llm_daytrade_buy_min_confidence = float(
            os.getenv(
                "TLEADING_LLM_DAYTRADE_BUY_MIN_CONFIDENCE",
                str(self.llm_auto_buy_min_confidence),
            )
        )
        self.llm_swing_buy_min_confidence = float(
            os.getenv("TLEADING_LLM_SWING_BUY_MIN_CONFIDENCE", "0.90")
        )
        self.daytrade_stop_loss_pct = self._parse_stop_loss_pct(
            os.getenv("TLEADING_DAYTRADE_STOP_LOSS_PCT", "3%")
        )
        self.swing_stop_loss_pct = self._parse_stop_loss_pct(
            os.getenv("TLEADING_SWING_STOP_LOSS_PCT", "7%")
        )
        self.llm_auto_buy_requires_stop_loss = (
            os.getenv("TLEADING_LLM_AUTO_BUY_REQUIRES_STOP_LOSS", "true").lower()
            == "true"
        )
        self.llm_auto_max_buys_per_day = int(
            os.getenv("TLEADING_LLM_AUTO_MAX_BUYS_PER_DAY", "3")
        )
        self.llm_auto_max_active_positions = int(
            os.getenv("TLEADING_LLM_AUTO_MAX_ACTIVE_POSITIONS", "5")
        )
        self.llm_auto_symbol_cooldown_minutes = int(
            os.getenv("TLEADING_LLM_AUTO_SYMBOL_COOLDOWN_MINUTES", "60")
        )
        self.daily_review_poll_seconds = int(
            os.getenv("TLEADING_DAILY_REVIEW_POLL_SECONDS", "300")
        )
        self.daily_review_time = self._parse_hhmm(
            os.getenv("TLEADING_DAILY_REVIEW_TIME", "23:00")
        )
        self.kospi_futures_contract_code = os.getenv(
            "TLEADING_KOSPI_FUTURES_CONTRACT_CODE", "101W09"
        ).strip()
        self.kospi_futures_market_cls_code = os.getenv(
            "TLEADING_KOSPI_FUTURES_MARKET_CLS_CODE", "MKI"
        ).strip()
        self.kospi_futures_daily_budget_krw = int(
            os.getenv("TLEADING_KOSPI_FUTURES_DAILY_BUDGET_KRW", "1000000")
        )
        self.kospi_futures_contract_budget_krw = int(
            os.getenv(
                "TLEADING_KOSPI_FUTURES_CONTRACT_BUDGET_KRW",
                str(self.kospi_futures_daily_budget_krw),
            )
        )
        self.kospi_futures_default_quantity = int(
            os.getenv("TLEADING_KOSPI_FUTURES_DEFAULT_QUANTITY", "1")
        )
        self.kospi_futures_mode = self._resolve_kospi_futures_mode()
        self.kospi_futures_track_only = self.kospi_futures_mode == "tracking"
        self._validate_kospi_futures_mode()
        if self.is_mock:
            logger.info("TleadingTrader initialized in MOCK MODE (No real trades)")
        logger.info(
            "TleadingTrader config: "
            f"order_quantity={self.order_quantity}, "
            f"default_stop_loss_pct={self.default_stop_loss_pct}, "
            f"default_stop_loss_price={self.default_stop_loss_price}, "
            f"market_timezone={self.market_timezone.key}, "
            f"market_open_time={self.market_open_time.strftime('%H:%M')}, "
            f"market_close_time={self.market_close_time.strftime('%H:%M')}, "
            f"holdings_poll_seconds={self.holdings_poll_seconds}, "
            f"price_tracking_minutes={self.price_tracking_minutes}, "
            f"scheduled_orders_poll_seconds={self.scheduled_orders_poll_seconds}, "
            f"auto_stop_loss_enabled={self.auto_stop_loss_enabled}, "
            f"signal_strategy={self.signal_strategy}, "
            f"llm_auto_buy_min_confidence={self.llm_auto_buy_min_confidence}, "
            f"llm_auto_sell_min_confidence={self.llm_auto_sell_min_confidence}, "
            f"llm_daytrade_buy_min_confidence={self.llm_daytrade_buy_min_confidence}, "
            f"llm_swing_buy_min_confidence={self.llm_swing_buy_min_confidence}, "
            f"daytrade_stop_loss_pct={self.daytrade_stop_loss_pct}, "
            f"swing_stop_loss_pct={self.swing_stop_loss_pct}, "
            f"llm_auto_buy_requires_stop_loss={self.llm_auto_buy_requires_stop_loss}, "
            f"llm_auto_max_buys_per_day={self.llm_auto_max_buys_per_day}, "
            f"llm_auto_max_active_positions={self.llm_auto_max_active_positions}, "
            f"llm_auto_symbol_cooldown_minutes={self.llm_auto_symbol_cooldown_minutes}, "
            f"daily_review_time={self.daily_review_time.strftime('%H:%M')}, "
            f"kospi_futures_contract_code={self.kospi_futures_contract_code}, "
            f"kospi_futures_market_cls_code={self.kospi_futures_market_cls_code}, "
            f"kospi_futures_daily_budget_krw={self.kospi_futures_daily_budget_krw}, "
            f"kospi_futures_contract_budget_krw={self.kospi_futures_contract_budget_krw}, "
            f"kospi_futures_mode={self.kospi_futures_mode}, "
            f"kospi_futures_track_only={self.kospi_futures_track_only}, "
            f"obsidian_diary_dir={self.obsidian_diary_dir}, "
            f"is_mock={self.is_mock}"
        )

    def _resolve_kospi_futures_mode(self) -> str:
        raw_mode = os.getenv("TLEADING_KOSPI_FUTURES_MODE", "").strip().lower()
        if not raw_mode:
            legacy_track_only = (
                os.getenv("TLEADING_KOSPI_FUTURES_TRACK_ONLY", "false").strip().lower()
            )
            return "tracking" if legacy_track_only == "true" else "paper"

        aliases = {
            "track": "tracking",
            "tracking-only": "tracking",
            "track_only": "tracking",
            "paper-tracking": "tracking",
            "mock": "paper",
            "simulation": "paper",
            "demo": "paper",
            "real": "live",
            "production": "live",
        }
        mode = aliases.get(raw_mode, raw_mode)
        if mode not in {"tracking", "paper", "live"}:
            raise ValueError(
                "TLEADING_KOSPI_FUTURES_MODE must be one of tracking, paper, live "
                f"(got {raw_mode!r})"
            )
        return mode

    def _validate_kospi_futures_mode(self) -> None:
        if self.kospi_futures_mode == "tracking":
            return

        if self.kospi_futures_mode == "paper" and not self.market_handler.is_simulation:
            raise ValueError(
                "TLEADING_KOSPI_FUTURES_MODE=paper requires KIS_PROFILE=paper or KIS_SIMULATION=true"
            )

        if self.kospi_futures_mode == "live" and self.market_handler.is_simulation:
            raise ValueError(
                "TLEADING_KOSPI_FUTURES_MODE=live requires KIS_PROFILE=live and KIS_SIMULATION=false"
            )

    async def handle_signal(
        self, signal: ReadingSignal, tg: anyio.abc.TaskGroup | None = None
    ):
        """매매 신호를 처리하고 필요 시 Discord 컨펌을 요청합니다."""
        if self._is_weekend():
            logger.info(
                "Skipping Telegram leading signal handling on weekend (action={}, strategy_name={})",
                signal.action,
                signal.strategy_name,
            )
            return

        # 1. 시그널 요약 다이어리에 기록
        summary_msg = f"📔 **[Tleading Diary]** {signal.company_name or '시황 요약'}\n• 요약: {signal.summary}\n• 판단: {signal.action} / {signal.trade_style} (신뢰도: {signal.confidence:.2f})\n• 근거: {signal.rationale_text}"
        await self.notifier.notify_diary(summary_msg)

        # 2. 매매 액션 처리
        if signal.action == "ignore":
            self._record_investment_journal(
                signal, code=None, decision="ignored", reason="action_ignore"
            )
            return

        if signal.strategy_name == "chart_master_kospi":
            await self._handle_chart_master_kospi_signal(signal)
            return

        company = signal.company_name
        if not company:
            logger.info(
                f"Signal action is {signal.action} but company name is missing. Skipping trade."
            )
            self._record_investment_journal(
                signal, code=None, decision="skipped", reason="missing_company"
            )
            return

        code = self.market_handler.get_code(company)
        if not code:
            logger.warning(f"Could not find code for {company}. Skipping trade.")
            self._record_investment_journal(
                signal, code=None, decision="skipped", reason="missing_code"
            )
            return

        if signal.action == "buy_candidate":
            active_trades = self._load_trades()
            if code in active_trades:
                logger.info(
                    f"{company}({code}) is already in active trades. Skipping duplicate buy."
                )
                self._record_investment_journal(
                    signal,
                    code=code,
                    decision="skipped",
                    reason="duplicate_active_trade",
                )
                return
            if self._should_auto_execute_signal(
                signal,
                expected_action="buy",
                code=code,
                active_trades=active_trades,
            ):
                self._record_investment_journal(
                    signal,
                    code=code,
                    decision="auto_buy_attempt",
                    reason="auto_gate_passed",
                )
                await self._auto_buy(company, code, signal)
                return
            self._record_investment_journal(
                signal,
                code=code,
                decision="confirm_buy_requested",
                reason="auto_gate_not_passed",
            )
            await self._confirm_and_buy(company, code, signal)
        elif signal.action == "sell":
            if self._should_auto_execute_signal(signal, expected_action="sell"):
                self._record_investment_journal(
                    signal,
                    code=code,
                    decision="auto_sell_attempt",
                    reason="auto_gate_passed",
                )
                await self._auto_sell(company, code, signal)
                return
            self._record_investment_journal(
                signal,
                code=code,
                decision="confirm_sell_requested",
                reason="auto_gate_not_passed",
            )
            await self._confirm_and_sell(company, code, signal)

    async def _auto_buy(self, company: str, code: str, signal: ReadingSignal):
        quantity = self.order_quantity
        ok, result_message = await self.place_manual_buy(
            company,
            code,
            quantity,
            stop_loss_pct=self._resolve_signal_stop_loss_pct(signal),
            trade_style=signal.trade_style,
        )
        prefix = (
            f"🤖 **[LLM Auto Buy]** {company}({code})\n"
            f"• 신뢰도: {signal.confidence:.2f}\n"
            f"• 판단 근거: {signal.rationale_text[:180]}\n"
        )
        await self.notifier.notify_all(prefix + result_message)
        await self.notifier.notify_diary(prefix + result_message)
        if ok:
            self._record_autonomous_action("buy", code, company)
        else:
            logger.warning("LLM auto buy failed: {}", result_message)

    async def _auto_sell(self, company: str, code: str, signal: ReadingSignal):
        active_trades = self._load_trades()
        if code not in active_trades:
            logger.info(
                f"LLM auto sell signal for {company}, but not in active trades."
            )
            return

        ok, result_message = await self.place_manual_sell(
            company,
            code,
            quantity=active_trades[code]["quantity"],
        )
        prefix = (
            f"🤖 **[LLM Auto Sell]** {company}({code})\n"
            f"• 신뢰도: {signal.confidence:.2f}\n"
            f"• 판단 근거: {signal.rationale_text[:180]}\n"
        )
        await self.notifier.notify_all(prefix + result_message)
        await self.notifier.notify_diary(prefix + result_message)
        if ok:
            self._record_autonomous_action("sell", code, company)
        else:
            logger.warning("LLM auto sell failed: {}", result_message)

    async def _confirm_and_buy(self, company: str, code: str, signal: ReadingSignal):
        """매수 컨펌 및 실행"""
        prompt = (
            f"'{company}'({code})를 매수할까요? "
            f"(신뢰도: {signal.confidence:.2f})\n"
            f"• 응답: `buy` 또는 `skip` (`y`/`yes`도 가능)\n"
            f"• 원문: {signal.rationale_text[:100]}..."
        )
        answer = await get_discord_input(
            prompt,
            request_label="📢 **[Trade Confirm]**",
        )

        if self._is_trade_confirmed(answer, expected_action="buy"):
            quantity = self.order_quantity
            price = 0

            if self.is_mock:
                # 모의 투자: 현재가 조회 후 성공 처리
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                sl_price, sl_label = self._resolve_stop_loss(
                    entry_price=price,
                    stop_loss_pct=self._resolve_signal_stop_loss_pct(signal),
                )

                success_msg = (
                    f"🍦 **[Mock Buy]** {company} {quantity}주 매수 완료\n"
                    f"• 체결가: {price:,}원\n"
                    f"• 손절라인: {self._format_stop_loss_price(sl_price)} ({sl_label})"
                )
                await self.notifier.notify_all(success_msg)
                await self.notifier.notify_diary(
                    f"✅ [Mock Buy Success] {company} @ {price:,}원 "
                    f"(SL: {self._format_stop_loss_price(sl_price)})"
                )
                self.record_executed_buy(
                    company,
                    code,
                    quantity,
                    price,
                    sl_price,
                    trade_style=signal.trade_style,
                )
            else:
                # 실제 투자
                res = self.market_handler.create_market_buy_order(code, quantity)
                if res.get("rt_cd") == "0":
                    price = self._extract_price(res, fallback_code=code)
                    sl_price, sl_label = self._resolve_stop_loss(
                        entry_price=price,
                        stop_loss_pct=self._resolve_signal_stop_loss_pct(signal),
                    )

                    success_msg = (
                        f"✅ **[Buy Success]** {company} {quantity}주 매수 완료\n"
                        f"• 평균가: {price:,}원\n"
                        f"• 손절 예약: {self._format_stop_loss_price(sl_price)} ({sl_label}) 설정 완료"
                    )
                    await self.notifier.notify_all(success_msg)
                    await self.notifier.notify_diary(
                        f"✅ [Buy Success] {company} @ {price:,}원 "
                        f"(SL: {self._format_stop_loss_price(sl_price)})"
                    )

                    # 실제 예약 매도 주문 로직 (KIS API에 따라 구현 필요, 여기서는 기록 후 감시)
                    self.record_executed_buy(
                        company,
                        code,
                        quantity,
                        price,
                        sl_price,
                        trade_style=signal.trade_style,
                    )
                else:
                    fail_msg = (
                        f"❌ **[Buy Fail]** {company} 매수 실패: {res.get('msg1')}"
                    )
                    await self.notifier.notify_all(fail_msg)
        else:
            await self.notifier.notify_all(f"🚫 {company} 매수를 거절하셨습니다.")

    async def _confirm_and_sell(self, company: str, code: str, signal: ReadingSignal):
        """매도 컨펌 및 실행"""
        # 보유 중인지 확인
        active_trades = self._load_trades()
        if code not in active_trades:
            logger.info(f"Signal to sell {company}, but not in active trades.")
            return

        prompt = (
            f"보유 중인 '{company}'({code})를 매도할까요?\n"
            f"• 응답: `sell` 또는 `skip` (`y`/`yes`도 가능)\n"
            f"• 원문: {signal.rationale_text[:100]}..."
        )
        answer = await get_discord_input(
            prompt,
            request_label="📢 **[Trade Confirm]**",
        )

        if self._is_trade_confirmed(answer, expected_action="sell"):
            quantity = active_trades[code]["quantity"]
            price = 0

            if self.is_mock:
                # 모의 투자: 현재가 조회 후 성공 처리
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                success_msg = f"🍦 **[Mock Sell]** {company} {quantity}주 매도 시뮬레이션 완료 (매도가: {price:,}원)"
                await self.notifier.notify_all(success_msg)
                self.record_executed_sell(
                    company, code, quantity, price, active_trade=active_trades[code]
                )
            else:
                # 실제 투자
                res = self.market_handler.create_market_sell_order(code, quantity)
                if res.get("rt_cd") == "0":
                    price = self._extract_price(res, fallback_code=code)
                    success_msg = f"✅ **[Sell Success]** {company} {quantity}주 전량 매도 완료 (매도가: {price:,}원)"
                    await self.notifier.notify_all(success_msg)
                    self.record_executed_sell(
                        company, code, quantity, price, active_trade=active_trades[code]
                    )
                else:
                    fail_msg = (
                        f"❌ **[Sell Fail]** {company} 매도 실패: {res.get('msg1')}"
                    )
                    await self.notifier.notify_all(fail_msg)
        else:
            await self.notifier.notify_all(f"🚫 {company} 매도를 거절하셨습니다.")

    async def _handle_chart_master_kospi_signal(self, signal: ReadingSignal) -> None:
        action = signal.action
        if action not in {"buy_candidate", "sell"}:
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="skipped",
                reason="non_actionable_futures_signal",
            )
            return

        requested_qty = (
            self._extract_futures_quantity(signal.rationale_text)
            or self.kospi_futures_default_quantity
        )
        if requested_qty <= 0:
            requested_qty = self.kospi_futures_default_quantity

        state = self._load_kospi_futures_state()
        open_quantity = int(state.get("open_quantity", 0))
        if self.kospi_futures_track_only:
            await self._track_chart_master_kospi_paper_position(
                signal,
                state=state,
                requested_qty=requested_qty,
                open_quantity=open_quantity,
            )
            return

        if action == "buy_candidate":
            blocked_reason = state.get("buy_blocked_reason")
            blocked_date = state.get("buy_blocked_date")
            if blocked_reason and blocked_date == self._today_market_date():
                await self.notifier.notify_kospi_futures(
                    "⚠️ **[KOSPI Futures Buy Blocked]** "
                    f"{self.kospi_futures_contract_code} 신규 진입이 차단되어 있습니다: {blocked_reason}"
                )
                self._record_investment_journal(
                    signal,
                    code=self.kospi_futures_contract_code,
                    decision="skipped",
                    reason="futures_buy_blocked_for_day",
                )
                return

            budget_remaining = max(
                self.kospi_futures_daily_budget_krw
                - (open_quantity * self.kospi_futures_contract_budget_krw),
                0,
            )
            max_allowed_qty = (
                budget_remaining // self.kospi_futures_contract_budget_krw
                if self.kospi_futures_contract_budget_krw > 0
                else 0
            )
            buy_quantity = min(requested_qty, max_allowed_qty)
            if buy_quantity <= 0:
                await self.notifier.notify_kospi_futures(
                    "⚠️ **[KOSPI Futures Budget]** "
                    f"{self.kospi_futures_contract_code} 신규 진입 불가: 예산 {self.kospi_futures_daily_budget_krw:,}원 내에서 "
                    f"가용 수량이 없습니다."
                )
                self._record_investment_journal(
                    signal,
                    code=self.kospi_futures_contract_code,
                    decision="skipped",
                    reason="futures_budget_exhausted",
                )
                return

            price = self.market_handler.fetch_domestic_future_price(
                self.kospi_futures_contract_code,
                market_cls_code=self.kospi_futures_market_cls_code,
            )
            res = self.market_handler.create_futureoption_buy_order(
                self.kospi_futures_contract_code,
                buy_quantity,
                account_product_code="03",
            )
            if res.get("rt_cd") != "0":
                fail_reason = str(
                    res.get("msg1") or res.get("msg_cd") or "unknown_error"
                )
                fail_msg = f"❌ **[KOSPI Futures Buy Fail]** {self.kospi_futures_contract_code} {buy_quantity}계약 진입 실패: {fail_reason}"
                await self.notifier.notify_kospi_futures(fail_msg)
                if self._is_futures_insufficient_orderable_amount(res):
                    broker_budget_msg = (
                        "브로커 응답 기준 모의/실계좌 주문가능금액이 계약 증거금보다 부족합니다. "
                        f"현재 전략 내부 계약예산은 {self.kospi_futures_contract_budget_krw:,}원으로 설정되어 있으므로, "
                        "실제 선물 증거금 수준에 맞게 `TLEADING_KOSPI_FUTURES_CONTRACT_BUDGET_KRW`를 상향하거나 "
                        "더 작은 계약 종목으로 변경해야 합니다."
                    )
                    self._save_kospi_futures_state(
                        {
                            "buy_blocked_date": self._today_market_date(),
                            "buy_blocked_reason": fail_reason,
                            "last_action": "buy_rejected",
                            "last_updated": self._now_market_tz().isoformat(),
                        }
                    )
                    await self.notifier.notify_kospi_futures(
                        "⚠️ **[KOSPI Futures Budget Mismatch]** "
                        f"{broker_budget_msg} ({self.kospi_futures_contract_code})"
                    )
                self._record_investment_journal(
                    signal,
                    code=self.kospi_futures_contract_code,
                    decision="futures_buy_failed",
                    reason=fail_reason,
                )
                return

            executed_price = price or self._extract_price(res)
            self._save_kospi_futures_state(
                {
                    "open_quantity": open_quantity + buy_quantity,
                    "buy_blocked_date": None,
                    "buy_blocked_reason": None,
                    "last_action": "buy",
                    "last_price": executed_price,
                    "last_updated": self._now_market_tz().isoformat(),
                }
            )
            await self.notifier.notify_kospi_futures(
                "✅ **[KOSPI Futures Buy]** "
                f"{self.kospi_futures_contract_code} {buy_quantity}계약 진입 완료 "
                f"(체결가: {executed_price:,}원, 예산 {self.kospi_futures_daily_budget_krw:,}원)"
            )
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="futures_buy_executed",
                reason=f"requested={requested_qty}, executed={buy_quantity}",
            )
            return

        sell_quantity = min(requested_qty, open_quantity)
        if sell_quantity <= 0:
            await self.notifier.notify_kospi_futures(
                f"ℹ️ **[KOSPI Futures Sell]** {self.kospi_futures_contract_code} 청산 대상이 없습니다."
            )
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="skipped",
                reason="no_open_futures_position",
            )
            return

        price = self.market_handler.fetch_domestic_future_price(
            self.kospi_futures_contract_code,
            market_cls_code=self.kospi_futures_market_cls_code,
        )
        res = self.market_handler.create_futureoption_sell_order(
            self.kospi_futures_contract_code,
            sell_quantity,
            account_product_code="03",
        )
        if res.get("rt_cd") != "0":
            fail_msg = f"❌ **[KOSPI Futures Sell Fail]** {self.kospi_futures_contract_code} {sell_quantity}계약 청산 실패: {res.get('msg1')}"
            await self.notifier.notify_kospi_futures(fail_msg)
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="futures_sell_failed",
                reason=str(res.get("msg1") or res.get("msg_cd") or "unknown_error"),
            )
            return

        executed_price = price or self._extract_price(res)
        remaining_quantity = max(open_quantity - sell_quantity, 0)
        self._save_kospi_futures_state(
            {
                "open_quantity": remaining_quantity,
                "last_action": "sell",
                "last_price": executed_price,
                "last_updated": self._now_market_tz().isoformat(),
            }
        )
        await self.notifier.notify_kospi_futures(
            "✅ **[KOSPI Futures Sell]** "
            f"{self.kospi_futures_contract_code} {sell_quantity}계약 청산 완료 "
            f"(체결가: {executed_price:,}원, 잔여 {remaining_quantity}계약)"
        )
        self._record_investment_journal(
            signal,
            code=self.kospi_futures_contract_code,
            decision="futures_sell_executed",
            reason=f"requested={requested_qty}, executed={sell_quantity}",
        )

    @staticmethod
    def _extract_futures_quantity(text: str) -> int:
        match = FUTURES_QUANTITY_PATTERN.search(text or "")
        if not match:
            return 0
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _is_futures_insufficient_orderable_amount(response: dict) -> bool:
        msg_cd = str(response.get("msg_cd") or "").strip()
        msg1 = str(response.get("msg1") or "").strip()
        if msg_cd == "40250000":
            return True
        return "주문가능금액이 부족" in msg1 or "증거금" in msg1

    async def _track_chart_master_kospi_paper_position(
        self,
        signal: ReadingSignal,
        *,
        state: dict,
        requested_qty: int,
        open_quantity: int,
    ) -> None:
        current_price = self.market_handler.fetch_domestic_future_price(
            self.kospi_futures_contract_code,
            market_cls_code=self.kospi_futures_market_cls_code,
        )
        action = signal.action

        if action == "buy_candidate":
            buy_quantity = requested_qty
            if buy_quantity <= 0:
                buy_quantity = self.kospi_futures_default_quantity

            avg_entry_price = float(state.get("avg_entry_price") or 0.0)
            total_quantity = open_quantity + buy_quantity
            next_avg_entry = (
                (
                    (avg_entry_price * open_quantity)
                    + (float(current_price) * buy_quantity)
                )
                / total_quantity
                if total_quantity > 0
                else 0.0
            )
            self._save_kospi_futures_state(
                {
                    "open_quantity": total_quantity,
                    "avg_entry_price": next_avg_entry,
                    "last_action": "paper_buy",
                    "last_price": current_price,
                    "last_updated": self._now_market_tz().isoformat(),
                }
            )
            unrealized_pnl_points = (
                float(current_price) - next_avg_entry
            ) * total_quantity
            await self.notifier.notify_kospi_futures(
                "📝 **[KOSPI Futures Paper Buy]** "
                f"{self.kospi_futures_contract_code} {buy_quantity}계약 가상 진입 "
                f"(기준가: {float(current_price):,.2f}, 평균가: {next_avg_entry:,.2f}, "
                f"보유 {total_quantity}계약, 평가손익 {unrealized_pnl_points:+.2f}pt)"
            )
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="futures_paper_buy_tracked",
                reason=f"requested={requested_qty}, tracked={buy_quantity}, price={float(current_price):.2f}",
            )
            return

        sell_quantity = min(requested_qty, open_quantity)
        if sell_quantity <= 0:
            await self.notifier.notify_kospi_futures(
                f"ℹ️ **[KOSPI Futures Paper Sell]** {self.kospi_futures_contract_code} 청산할 가상 포지션이 없습니다."
            )
            self._record_investment_journal(
                signal,
                code=self.kospi_futures_contract_code,
                decision="skipped",
                reason="no_open_paper_futures_position",
            )
            return

        avg_entry_price = float(state.get("avg_entry_price") or 0.0)
        realized_pnl_points = (float(current_price) - avg_entry_price) * sell_quantity
        cumulative_realized_pnl_points = (
            float(state.get("realized_pnl_points") or 0.0) + realized_pnl_points
        )
        remaining_quantity = max(open_quantity - sell_quantity, 0)
        next_avg_entry = avg_entry_price if remaining_quantity > 0 else 0.0
        self._save_kospi_futures_state(
            {
                "open_quantity": remaining_quantity,
                "avg_entry_price": next_avg_entry,
                "realized_pnl_points": cumulative_realized_pnl_points,
                "last_realized_pnl_points": realized_pnl_points,
                "last_action": "paper_sell",
                "last_price": current_price,
                "last_updated": self._now_market_tz().isoformat(),
            }
        )
        await self.notifier.notify_kospi_futures(
            "📝 **[KOSPI Futures Paper Sell]** "
            f"{self.kospi_futures_contract_code} {sell_quantity}계약 가상 청산 "
            f"(청산가: {float(current_price):,.2f}, 실현손익 {realized_pnl_points:+.2f}pt, "
            f"누적 {cumulative_realized_pnl_points:+.2f}pt, 잔여 {remaining_quantity}계약)"
        )
        self._record_investment_journal(
            signal,
            code=self.kospi_futures_contract_code,
            decision="futures_paper_sell_tracked",
            reason=f"requested={requested_qty}, tracked={sell_quantity}, price={float(current_price):.2f}, pnl_points={realized_pnl_points:+.2f}",
        )

    def _load_kospi_futures_state(self) -> dict:
        today = self._today_market_date()
        if not self.kospi_futures_state_path.exists():
            return {
                "date": today,
                "open_quantity": 0,
                "avg_entry_price": 0.0,
                "realized_pnl_points": 0.0,
            }

        try:
            with open(self.kospi_futures_state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            return {
                "date": today,
                "open_quantity": 0,
                "avg_entry_price": 0.0,
                "realized_pnl_points": 0.0,
            }

        if not isinstance(state, dict):
            return {
                "date": today,
                "open_quantity": 0,
                "avg_entry_price": 0.0,
                "realized_pnl_points": 0.0,
            }
        if state.get("date") != today:
            state = {
                "date": today,
                "open_quantity": 0,
                "avg_entry_price": 0.0,
                "realized_pnl_points": 0.0,
            }
        state["open_quantity"] = int(state.get("open_quantity", 0) or 0)
        state["avg_entry_price"] = float(state.get("avg_entry_price", 0.0) or 0.0)
        state["realized_pnl_points"] = float(
            state.get("realized_pnl_points", 0.0) or 0.0
        )
        return state

    def _save_kospi_futures_state(self, patch: dict) -> None:
        state = self._load_kospi_futures_state()
        state.update(patch)
        state["date"] = self._today_market_date()
        state["open_quantity"] = max(int(state.get("open_quantity", 0) or 0), 0)
        with open(self.kospi_futures_state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    async def track_scheduled_orders_loop(self):
        logger.info(
            "Starting scheduled order loop (Interval: {} seconds)",
            self.scheduled_orders_poll_seconds,
        )
        while True:
            try:
                await self.process_scheduled_orders()
            except Exception as e:
                logger.error(f"Error in scheduled order loop: {e}")

            await anyio.sleep(self.scheduled_orders_poll_seconds)

    async def daily_review_loop(self):
        logger.info(
            "Starting daily review loop (Time: {}, Poll: {} seconds)",
            self.daily_review_time.strftime("%H:%M"),
            self.daily_review_poll_seconds,
        )
        while True:
            try:
                if self._is_weekend():
                    await anyio.sleep(self.daily_review_poll_seconds)
                    continue
                await self.process_daily_review()
            except Exception as e:
                logger.exception("Error in daily review loop: {}", e)

            await anyio.sleep(self.daily_review_poll_seconds)

    async def process_daily_review(self) -> None:
        now = self._now_market_tz()
        if self._is_weekend(now):
            logger.info("Skipping daily review on weekend ({})", now.date().isoformat())
            return
        if now.time() < self.daily_review_time:
            return

        review_date = now.date().isoformat()
        reviews = self._load_daily_reviews()

        review_state = reviews.get(review_date, {})
        terminal_feedback_states = {"recorded", "skipped", "prompt_failed"}
        if (
            review_state.get("feedback_text")
            or review_state.get("feedback_state") in terminal_feedback_states
        ):
            return

        review = review_state.get("review_text")
        if not review:
            review = self._build_daily_review(now.date())
            review_path = self._write_daily_review_markdown(now.date(), review)
            review_state.update(
                {
                    "created_at": now.isoformat(),
                    "markdown_path": str(review_path),
                    "review_text": review,
                }
            )
        else:
            review_path = Path(
                review_state.get("markdown_path")
                or self.obsidian_diary_dir / f"{review_date}.md"
            )
            if not review_path.exists():
                self._write_daily_review_markdown(now.date(), review)
            review_state.setdefault("created_at", now.isoformat())
            review_state.setdefault("markdown_path", str(review_path))
            review_state.setdefault("review_text", review)

        reviews[review_date] = review_state
        self._save_daily_reviews(reviews)

        review_channel_name = (
            os.environ.get("TLEADING_INVEST_REVIEW_CHANNEL_NAME")
            or os.environ.get("REVIEW_CHANNEL_NAME")
            or "📊-매매-복기"
        )
        review_channel_id = (
            self.notifier.review_channel_id or self.notifier.diary_channel_id
        )
        try:
            feedback = await get_discord_input(
                review,
                preferred_channel_name=review_channel_name,
                channel_id=review_channel_id,
                request_label="📘 **[Daily Review]**",
                prompt_suffix="",
                response_builder=lambda content: self._build_daily_review_response(
                    content, review_date
                ),
                response_label="📘 **[Daily Review Ack]**",
            )
        except RuntimeError as exc:
            review_state["feedback_at"] = self._now_market_tz().isoformat()
            review_state["feedback_state"] = "prompt_failed"
            review_state["feedback_error"] = str(exc)
            reviews[review_date] = review_state
            self._save_daily_reviews(reviews)
            logger.warning("Daily review feedback prompt failed: {}", exc)
            return

        normalized_feedback = feedback.strip().lower()
        review_state["feedback_at"] = self._now_market_tz().isoformat()
        if normalized_feedback == "skip":
            review_state["feedback_state"] = "skipped"
        elif feedback.strip():
            review_state["feedback_text"] = feedback.strip()
            review_state["feedback_state"] = "recorded"
            self._append_daily_review_feedback_markdown(now.date(), feedback)
        else:
            review_state["feedback_state"] = "skipped"

        reviews[review_date] = review_state
        self._save_daily_reviews(reviews)

    @staticmethod
    def _is_trade_confirmed(answer: str, expected_action: str) -> bool:
        normalized = answer.strip().lower()
        if not normalized:
            return False

        if normalized in {"skip", "n", "no", "아니오", "거절", "패스"}:
            return False

        if expected_action == "buy":
            return normalized in {"buy", "b", "y", "yes", "네", "ㅇㅇ", "ok", "매수"}

        if expected_action == "sell":
            return normalized in {
                "sell",
                "s",
                "y",
                "yes",
                "네",
                "ㅇㅇ",
                "ok",
                "매도",
                "정리",
            }

        return False

    def _should_auto_execute_signal(
        self,
        signal: ReadingSignal,
        expected_action: str,
        code: str | None = None,
        active_trades: dict | None = None,
    ) -> bool:
        if self.signal_strategy not in {"llm_autonomous", "llm-auto", "auto"}:
            return False

        if expected_action == "buy":
            if signal.action != "buy_candidate":
                return False
            min_confidence = self._auto_buy_min_confidence(signal.trade_style)
            if signal.confidence < min_confidence:
                logger.info(
                    "LLM auto buy skipped: confidence {:.2f} below threshold {:.2f}",
                    signal.confidence,
                    min_confidence,
                )
                return False
            if self.llm_auto_buy_requires_stop_loss and signal.stop_loss_pct is None:
                logger.info(
                    "LLM auto buy skipped: stop_loss_pct is required but missing"
                )
                return False
            if not code:
                logger.info("LLM auto buy skipped: symbol code is missing")
                return False
            if not self._autonomous_buy_risk_allows(code, active_trades=active_trades):
                return False
            return True

        if expected_action == "sell":
            if signal.action != "sell":
                return False
            if signal.confidence < self.llm_auto_sell_min_confidence:
                logger.info(
                    "LLM auto sell skipped: confidence {:.2f} below threshold {:.2f}",
                    signal.confidence,
                    self.llm_auto_sell_min_confidence,
                )
                return False
            return True

        return False

    async def track_holdings_loop(self, tg: anyio.abc.TaskGroup):
        """주기적으로 보유 주식 현황과 손절 트리거를 체크합니다."""
        logger.info(
            f"Starting holdings tracking loop (Interval: {self.holdings_poll_seconds} seconds)"
        )
        while True:
            try:
                await self.report_holdings(tg)
            except Exception as e:
                logger.error(f"Error in tracking loop: {e}")

            await anyio.sleep(self.holdings_poll_seconds)

    async def track_trade_prices_loop(self):
        """체결된 보유 종목을 15분 단위로 가격 추적합니다."""
        interval_seconds = max(self.price_tracking_minutes, 1) * 60
        logger.info(
            "Starting trade price tracking loop (Interval: {} minutes)",
            self.price_tracking_minutes,
        )
        while True:
            try:
                await self.track_trade_prices()
            except Exception as e:
                logger.error(f"Error in trade price tracking loop: {e}")

            await anyio.sleep(interval_seconds)

    async def track_trade_prices(self):
        active_trades = self._reconcile_active_trades_with_balance()
        if not active_trades:
            return

        now = self._now_market_tz()
        if not self._is_regular_market_open(now):
            await self._send_post_market_briefings(active_trades, now)
            return

        for code, trade in active_trades.items():
            if not self._is_tracking_due(trade):
                continue

            price_info = self.market_handler.fetch_price(code)
            current_price = int(float(price_info.get("output", {}).get("stck_prpr", 0)))
            if current_price <= 0:
                continue

            tracked_at = datetime.now()
            self._record_trade_snapshot(
                trade, current_price, phase="interval", tracked_at=tracked_at
            )
            self._update_trade_tracking_state(code, current_price, tracked_at)

            entry_price = trade["entry_price"]
            profit_rate = (
                ((current_price - entry_price) / entry_price) * 100
                if entry_price
                else 0.0
            )
            await self.notifier.notify_diary(
                f"⏱️ [Auto Trading Log] {trade['company']}({code}) "
                f"{self.price_tracking_minutes}분 추적\n"
                f"• 기준가: {entry_price:,}원\n"
                f"• 현재가: {current_price:,}원\n"
                f"• 수익률: {profit_rate:+.2f}%\n"
                f"• 시각: {tracked_at.isoformat(timespec='seconds')}"
            )

    async def _send_post_market_briefings(
        self, active_trades: dict, now: datetime
    ) -> None:
        if not self._should_send_post_market_briefing(now):
            return

        for code, trade in active_trades.items():
            if trade.get("last_daily_briefing_date") == now.date().isoformat():
                continue

            briefing = self._build_post_market_briefing(code, trade, now.date())
            if not briefing:
                continue

            await self.notifier.notify_diary(briefing)
            self._mark_trade_daily_briefing_sent(code, now.date())

    async def report_holdings(self, tg: anyio.abc.TaskGroup):
        """현재 보유 종목의 수익률 현황을 Discord로 보고합니다."""
        if not self._is_regular_market_open():
            return

        # API 호출 안정성을 위해 tenacity 적용
        async for attempt in AsyncRetrying(
            wait=wait_exponential(multiplier=1, min=2, max=10),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        ):
            with attempt:
                active_trades = self._load_trades()
                active_trades = self._reconcile_active_trades_with_balance(
                    active_trades
                )
                if not active_trades:
                    return

                msg = "📊 **[Hourly Status]** 현재 보유 종목 현황\n"
                has_updates = False

                for code, data in active_trades.items():
                    price_info = self.market_handler.fetch_price(code)
                    current_price = float(
                        price_info.get("output", {}).get("stck_prpr", 0)
                    )
                    if current_price == 0:
                        continue

                    entry_price = data["entry_price"]
                    profit_rate = (current_price - entry_price) / entry_price
                    stop_loss_price = int(data.get("stop_loss_price") or 0)

                    status_emoji = "📈" if profit_rate > 0 else "📉"
                    stop_loss_suffix = (
                        f", SL {stop_loss_price:,}원" if stop_loss_price > 0 else ""
                    )
                    msg += (
                        f"• {data['company']}: {current_price:,}원 "
                        f"({status_emoji} {profit_rate * 100:+.2f}%{stop_loss_suffix})\n"
                    )
                    has_updates = True

                    stop_loss_pending = bool(data.get("stop_loss_pending"))
                    threshold_price = (
                        stop_loss_price
                        or self._resolve_stop_loss(entry_price=entry_price)[0]
                    )

                    if (
                        self.auto_stop_loss_enabled
                        and threshold_price
                        and current_price <= threshold_price
                    ):
                        if stop_loss_pending:
                            continue

                        alert_msg = (
                            f"⚠️ **[Stop Loss Triggered]** {data['company']} 현재가 {current_price:,}원이 "
                            f"손절 라인 {threshold_price:,}원 이하에 도달했습니다."
                        )
                        await self.notifier.notify_all(alert_msg)
                        self._mark_trade_stop_loss_pending(code, True, current_price)

                        tg.start_soon(
                            self._execute_stop_loss_sell,
                            data["company"],
                            code,
                            data["quantity"],
                            entry_price,
                            current_price,
                        )

                if has_updates:
                    await self.notifier.notify_all(msg)

    async def get_status_report(self) -> str:
        """현재 계좌 상태와 매매 현황을 요약한 리포트를 생성합니다."""
        # 1. 예수금 조회
        balance_info = self.market_handler.fetch_balance()
        cash = (
            int(balance_info.get("output2", [{}])[0].get("dnca_tot_amt", 0))
            if not self.is_mock
            else 10000000
        )  # 모의는 1천만 시작 가정

        report = f"💰 **[Account Status]**\n• **예수금**: {cash:,}원\n\n"
        report += self._build_autonomous_strategy_status()
        report += self._build_kospi_futures_status()

        # 2. 보유 종목 현황
        active_trades = self._reconcile_active_trades_with_balance()
        if active_trades:
            report += "📂 **현재 보유 종목**\n"
            total_eval = 0
            rows = []
            for code, data in active_trades.items():
                price_info = self.market_handler.fetch_price(code)
                curr_price = int(price_info.get("output", {}).get("stck_prpr", 0))
                eval_pnl = (curr_price - data["entry_price"]) * data["quantity"]
                eval_rate = (curr_price - data["entry_price"]) / data["entry_price"]
                total_eval += curr_price * data["quantity"]
                stop_loss_price = int(data.get("stop_loss_price") or 0)

                rows.append(
                    [
                        data["company"],
                        str(data["quantity"]),
                        f"{data['entry_price']:,}",
                        f"{curr_price:,}",
                        f"{eval_rate * 100:+.2f}%",
                        f"{eval_pnl:+,}",
                        f"{stop_loss_price:,}" if stop_loss_price > 0 else "-",
                    ]
                )
            report += self._format_text_table(
                headers=[
                    "종목",
                    "보유수량",
                    "평단가",
                    "현재가",
                    "수익률",
                    "평가손익",
                    "손절가",
                ],
                rows=rows,
            )
            report += f"\n• 보유종목 총 평가액: {total_eval:,}원\n\n"
        else:
            report += "📂 **현재 보유 종목**: 없음\n\n"

        scheduled_orders = self._load_scheduled_orders()
        if scheduled_orders:
            report += "🗓️ **예약 주문**\n"
            scheduled_rows = []
            for order in scheduled_orders:
                target = (
                    f"{order['quantity']}주"
                    if order.get("quantity") is not None
                    else f"{int(order.get('ratio', 0) * 100)}%"
                )
                scheduled_rows.append(
                    [
                        "매수" if order["side"] == "buy" else "매도",
                        order["company"],
                        target,
                        f"{order['trigger_price']:,}원",
                        self._format_scheduled_stop_loss(order),
                    ]
                )
            report += self._format_text_table(
                headers=["구분", "종목", "주문", "목표가", "손절"],
                rows=scheduled_rows,
            )
            report += "\n"

        # 3. 실현 손익 (매매 이력 기반)
        history = self._load_history()
        if history:
            report += "🏁 **최근 실현 손익 (History)**\n"
            total_pnl = 0
            # 최근 10개까지만 표시
            for item in history[-10:]:
                total_pnl += item["pnl"]
                emoji = "🔥" if item["pnl"] >= 0 else "🧊"
                report += f"• {item['company']}: {item['pnl_rate'] * 100:+.2f}% ({item['pnl']:+,}원)\n"
            report += f"**▶️ 누적 실현 손익**: {total_pnl:+,}원"
        else:
            report += "🏁 **최근 실현 손익**: 이력 없음"

        return report

    async def place_manual_buy(
        self,
        company: str,
        code: str,
        quantity: int,
        stop_loss_price: int | None = None,
        stop_loss_pct: float | None = None,
        trade_style: str = "manual",
    ) -> tuple[bool, str]:
        verb = "매수"
        try:
            if self.is_mock:
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                if price <= 0:
                    return False, f"❌ {verb} 주문 실패: 현재가를 조회하지 못했습니다."
                resolved_stop_loss_price, stop_loss_label = self._resolve_stop_loss(
                    entry_price=price,
                    stop_loss_price=stop_loss_price,
                    stop_loss_pct=stop_loss_pct,
                )
                self.record_executed_buy(
                    company,
                    code,
                    quantity,
                    price,
                    stop_loss_price=resolved_stop_loss_price,
                    trade_style=trade_style,
                )
                return (
                    True,
                    f"✅ {verb} 주문 성공: {company}({code}) {quantity}주, 체결가 {price:,}원, "
                    f"손절 {self._format_stop_loss_price(resolved_stop_loss_price)} ({stop_loss_label})",
                )

            res = self.market_handler.create_market_buy_order(code, quantity)
            if res.get("rt_cd") != "0":
                error_msg = res.get("msg1") or res.get("msg_cd") or "알 수 없는 오류"
                return (
                    False,
                    f"❌ {verb} 주문 실패: {company}({code}) {quantity}주, {error_msg}",
                )

            price = self._extract_price(res, fallback_code=code)
            resolved_stop_loss_price, stop_loss_label = self._resolve_stop_loss(
                entry_price=price,
                stop_loss_price=stop_loss_price,
                stop_loss_pct=stop_loss_pct,
            )
            self.record_executed_buy(
                company,
                code,
                quantity,
                price,
                stop_loss_price=resolved_stop_loss_price,
                trade_style=trade_style,
            )
            return (
                True,
                f"✅ {verb} 주문 성공: {company}({code}) {quantity}주, 체결가 {price:,}원, "
                f"손절 {self._format_stop_loss_price(resolved_stop_loss_price)} ({stop_loss_label})",
            )
        except Exception as exc:
            logger.error(f"Manual buy command failed: {exc}")
            return False, f"❌ {verb} 주문 중 오류가 발생했습니다: {exc}"

    async def place_manual_sell(
        self,
        company: str,
        code: str,
        quantity: int | None = None,
        ratio: float | None = None,
    ) -> tuple[bool, str]:
        trade = self._load_trades().get(code)
        if not trade:
            return False, f"❌ {company}({code}) 보유 내역이 없습니다."

        try:
            sell_quantity = self._resolve_sell_quantity(
                trade["quantity"], quantity=quantity, ratio=ratio
            )
            verb_detail = f"{sell_quantity}주"
            if ratio is not None:
                verb_detail = f"{int(ratio * 100)}% ({sell_quantity}주)"

            if self.is_mock:
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                if price <= 0:
                    return False, "❌ 매도 주문 실패: 현재가를 조회하지 못했습니다."
                self.record_executed_sell(
                    company, code, sell_quantity, price, active_trade=trade
                )
                return (
                    True,
                    f"✅ 매도 주문 성공: {company}({code}) {verb_detail}, 체결가 {price:,}원",
                )

            res = self.market_handler.create_market_sell_order(code, sell_quantity)
            if res.get("rt_cd") != "0":
                error_msg = res.get("msg1") or res.get("msg_cd") or "알 수 없는 오류"
                return (
                    False,
                    f"❌ 매도 주문 실패: {company}({code}) {verb_detail}, {error_msg}",
                )

            price = self._extract_price(res, fallback_code=code)
            self.record_executed_sell(
                company, code, sell_quantity, price, active_trade=trade
            )
            return (
                True,
                f"✅ 매도 주문 성공: {company}({code}) {verb_detail}, 체결가 {price:,}원",
            )
        except Exception as exc:
            logger.error(f"Manual sell command failed: {exc}")
            return False, f"❌ 매도 주문 중 오류가 발생했습니다: {exc}"

    def schedule_manual_order(
        self,
        *,
        company: str,
        code: str,
        side: str,
        quantity: int | None = None,
        ratio: float | None = None,
        trigger_price: int,
        stop_loss_price: int | None = None,
        stop_loss_pct: float | None = None,
    ) -> dict:
        orders = self._load_scheduled_orders()
        order = {
            "order_id": self._make_scheduled_order_id(code, side, trigger_price),
            "company": company,
            "code": code,
            "side": side,
            "quantity": quantity,
            "ratio": ratio,
            "trigger_price": trigger_price,
            "stop_loss_price": stop_loss_price,
            "stop_loss_pct": stop_loss_pct,
            "created_at": datetime.now(self.market_timezone).isoformat(),
        }
        orders.append(order)
        self._save_scheduled_orders(orders)
        return order

    def record_executed_buy(
        self,
        company: str,
        code: str,
        quantity: int,
        price: int,
        stop_loss_price: int | None = None,
        trade_style: str = "manual",
    ) -> dict | None:
        trade = self._save_trade(
            company,
            code,
            quantity,
            price,
            "buy",
            stop_loss_price=stop_loss_price,
            trade_style=trade_style,
        )
        if trade:
            self._record_trade_snapshot(trade, price, phase="entry")
        return trade

    def record_executed_sell(
        self,
        company: str,
        code: str,
        quantity: int,
        price: int,
        active_trade: dict | None = None,
    ) -> None:
        trade = active_trade or self._load_trades().get(code)
        if trade:
            self._record_trade_snapshot(trade, price, phase="exit")
            entry_price = trade["entry_price"]
            pnl = (price - entry_price) * quantity
            pnl_rate = (price - entry_price) / entry_price
            self._save_history(
                company, code, quantity, entry_price, price, pnl, pnl_rate, trade=trade
            )
        self._save_trade(company, code, quantity, price, "sell")

    async def process_scheduled_orders(self) -> None:
        scheduled_orders = self._load_scheduled_orders()
        if not scheduled_orders:
            return

        now = self._now_market_tz()
        remaining_orders = []

        for order in scheduled_orders:
            if not self._is_regular_market_open(now):
                remaining_orders.append(order)
                continue

            price_info = self.market_handler.fetch_price(order["code"])
            current_price = int(float(price_info.get("output", {}).get("stck_prpr", 0)))
            if current_price <= 0:
                remaining_orders.append(order)
                continue

            trigger_price = int(order["trigger_price"])
            if not self._should_execute_scheduled_order(
                order["side"], current_price, trigger_price
            ):
                remaining_orders.append(order)
                continue

            if order["side"] == "buy":
                ok, result_message = await self.place_manual_buy(
                    order["company"],
                    order["code"],
                    int(order["quantity"]),
                    stop_loss_price=order.get("stop_loss_price"),
                    stop_loss_pct=order.get("stop_loss_pct"),
                )
            else:
                ok, result_message = await self.place_manual_sell(
                    order["company"],
                    order["code"],
                    quantity=order.get("quantity"),
                    ratio=order.get("ratio"),
                )

            prefix = f"🗓️ **[예약 주문 실행]** 현재가 {current_price:,}원 / 목표가 {trigger_price:,}원\n"
            await self.notifier.notify_all(prefix + result_message)
            if not ok and not self._is_non_retryable_order_failure(result_message):
                remaining_orders.append(order)

        self._save_scheduled_orders(remaining_orders)

    @staticmethod
    def _format_text_table(headers: list[str], rows: list[list[str]]) -> str:
        widths = [len(header) for header in headers]
        for row in rows:
            for idx, value in enumerate(row):
                widths[idx] = max(widths[idx], len(value))

        def render(row: list[str]) -> str:
            return " | ".join(value.ljust(widths[idx]) for idx, value in enumerate(row))

        separator = "-+-".join("-" * width for width in widths)
        lines = [render(headers), separator]
        lines.extend(render(row) for row in rows)
        return "```text\n" + "\n".join(lines) + "\n```\n"

    def _save_history(
        self,
        company: str,
        code: str,
        quantity: int,
        buy_price: int,
        sell_price: int,
        pnl: int,
        pnl_rate: float,
        trade: dict | None = None,
    ):
        history = self._load_history()
        history.append(
            {
                "company": company,
                "code": code,
                "quantity": quantity,
                "buy_price": buy_price,
                "sell_price": sell_price,
                "pnl": pnl,
                "pnl_rate": pnl_rate,
                "trade_id": trade.get("trade_id") if trade else None,
                "trade_style": trade.get("trade_style") if trade else "unknown",
                "entry_at": trade.get("entry_at") if trade else None,
                "closed_at": datetime.now().isoformat(),
            }
        )
        with open(self.trade_history_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def _load_history(self) -> list:
        if not self.trade_history_path.exists():
            return []
        try:
            with open(self.trade_history_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    async def _execute_stop_loss_sell(
        self,
        company: str,
        code: str,
        quantity: int,
        entry_price: int,
        current_price: float,
    ):
        """손절 라인 도달 시 사용자 확인 없이 시장가 매도를 실행합니다."""
        try:
            if self.is_mock:
                price = int(current_price)
                success_msg = (
                    f"🍦 **[Mock Stop Loss Sell]** {company} {quantity}주 자동 손절 시뮬레이션 완료 "
                    f"(매도가: {price:,}원)"
                )
                await self.notifier.notify_all(success_msg)
                await self.notifier.notify_diary(
                    f"✅ [Mock Stop Loss] {company} @ {price:,}원"
                )
                active_trade = self._load_trades().get(code)
                self.record_executed_sell(
                    company, code, quantity, price, active_trade=active_trade
                )
                return

            res = self.market_handler.create_market_sell_order(code, quantity)
            if res.get("rt_cd") == "0":
                price = self._extract_price(res, fallback_code=code) or int(
                    current_price
                )
                success_msg = (
                    f"🛑 **[Stop Loss Sell Success]** {company} {quantity}주 자동 손절 완료 "
                    f"(매도가: {price:,}원)"
                )
                await self.notifier.notify_all(success_msg)
                await self.notifier.notify_diary(
                    f"🛑 [Stop Loss Sold] {company} @ {price:,}원"
                )
                active_trade = self._load_trades().get(code)
                self.record_executed_sell(
                    company, code, quantity, price, active_trade=active_trade
                )
            else:
                self._mark_trade_stop_loss_pending(code, False)
                fail_msg = f"❌ **[Stop Loss Sell Fail]** {company} 자동 손절 실패: {res.get('msg1')}"
                await self.notifier.notify_all(fail_msg)
        except Exception as exc:
            self._mark_trade_stop_loss_pending(code, False)
            await self.notifier.notify_all(
                f"❌ **[Stop Loss Exception]** {company}: {exc}"
            )

    def _save_trade(
        self,
        company: str,
        code: str,
        quantity: int,
        price: int,
        action: str,
        stop_loss_price: int | None = None,
        trade_style: str = "manual",
    ) -> dict | None:
        trades = self._load_trades()
        if action == "buy":
            entry_at = datetime.now().isoformat()
            existing_trade = trades.get(code)
            if existing_trade:
                existing_quantity = int(existing_trade["quantity"])
                new_quantity = existing_quantity + quantity
                weighted_avg = round(
                    (
                        (existing_trade["entry_price"] * existing_quantity)
                        + (price * quantity)
                    )
                    / new_quantity
                )
                existing_trade["company"] = company
                existing_trade["quantity"] = new_quantity
                existing_trade["entry_price"] = weighted_avg
                existing_trade["trade_style"] = self._normalize_trade_style_value(
                    existing_trade.get("trade_style") or trade_style
                )
                existing_trade["last_buy_price"] = price
                existing_trade["last_buy_at"] = entry_at
                existing_trade["last_tracked_at"] = entry_at
                existing_trade["last_tracked_price"] = price
                if stop_loss_price is not None:
                    existing_trade["stop_loss_price"] = stop_loss_price
                trades[code] = existing_trade
            else:
                trades[code] = {
                    "trade_id": self._make_trade_id(code, entry_at),
                    "company": company,
                    "code": code,
                    "quantity": quantity,
                    "entry_price": price,
                    "trade_style": self._normalize_trade_style_value(trade_style),
                    "stop_loss_price": stop_loss_price,
                    "stop_loss_pending": False,
                    "entry_at": entry_at,
                    "last_tracked_at": entry_at,
                    "last_tracked_price": price,
                    "tracking_interval_minutes": self.price_tracking_minutes,
                }
        elif action == "sell":
            trade = trades.get(code)
            if trade:
                remaining_quantity = int(trade["quantity"]) - quantity
                if remaining_quantity > 0:
                    trade["quantity"] = remaining_quantity
                    trade["last_sell_price"] = price
                    trade["last_sell_at"] = datetime.now().isoformat()
                    trade["stop_loss_pending"] = False
                    trades[code] = trade
                else:
                    del trades[code]

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)
        return trades.get(code)

    def _load_trades(self) -> dict:
        if not self.active_trades_path.exists():
            return {}
        try:
            with open(self.active_trades_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _reconcile_active_trades_with_balance(self, trades: dict | None = None) -> dict:
        if self.is_mock:
            return trades if trades is not None else self._load_trades()

        trades = trades if trades is not None else self._load_trades()
        if not trades:
            return {}

        try:
            balance_info = self.market_handler.fetch_balance()
        except Exception as exc:
            logger.warning(f"Failed to reconcile active trades with balance: {exc}")
            return trades

        holdings = self._extract_balance_holdings(balance_info)
        changed = False

        for code in list(trades.keys()):
            holding = holdings.get(code)
            if not holding:
                logger.warning(
                    "Removing stale active trade not present in balance: {}({})",
                    trades[code].get("company"),
                    code,
                )
                del trades[code]
                changed = True
                continue

            actual_qty = int(holding.get("quantity", 0))
            if actual_qty <= 0:
                del trades[code]
                changed = True
                continue

            if int(trades[code].get("quantity", 0)) != actual_qty:
                trades[code]["quantity"] = actual_qty
                changed = True

            avg_price = int(holding.get("avg_price", 0))
            if avg_price > 0 and int(trades[code].get("entry_price", 0)) != avg_price:
                trades[code]["entry_price"] = avg_price
                changed = True

        if changed:
            with open(self.active_trades_path, "w", encoding="utf-8") as f:
                json.dump(trades, f, ensure_ascii=False, indent=2)

        return trades

    def _extract_balance_holdings(self, balance_info: dict) -> dict[str, dict]:
        holdings: dict[str, dict] = {}
        for item in balance_info.get("output1", []):
            if not isinstance(item, dict):
                continue

            code = str(
                item.get("pdno")
                or item.get("mksc_shrn_iscd")
                or item.get("prdt_no")
                or ""
            ).strip()
            if not code:
                continue

            quantity = self._to_int(
                item.get("hldg_qty") or item.get("hold_qty") or item.get("cblc_qty13")
            )
            avg_price = self._to_int(
                item.get("pchs_avg_pric")
                or item.get("pchs_pric")
                or item.get("avg_unpr")
                or item.get("purc_avg_pric")
            )
            holdings[code] = {
                "company": item.get("prdt_name") or item.get("name") or code,
                "quantity": quantity,
                "avg_price": avg_price,
            }
        return holdings

    def _save_scheduled_orders(self, orders: list[dict]) -> None:
        with open(self.scheduled_orders_path, "w", encoding="utf-8") as f:
            json.dump(orders, f, ensure_ascii=False, indent=2)

    def _load_scheduled_orders(self) -> list[dict]:
        if not self.scheduled_orders_path.exists():
            return []
        try:
            with open(self.scheduled_orders_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _record_investment_journal(
        self,
        signal: ReadingSignal,
        *,
        code: str | None,
        decision: str,
        reason: str,
    ) -> None:
        payload = {
            "recorded_at": self._now_market_tz().isoformat(),
            "source": signal.source,
            "strategy_name": signal.strategy_name,
            "message_id": signal.message_id,
            "posted_at": signal.posted_at.isoformat(),
            "chat_id": signal.chat_id,
            "chat_title": signal.chat_title,
            "company": signal.company_name,
            "code": code,
            "action": signal.action,
            "trade_style": self._normalize_trade_style_value(signal.trade_style),
            "confidence": signal.confidence,
            "stop_loss_pct": signal.stop_loss_pct,
            "entry_hint": signal.entry_hint,
            "decision": decision,
            "reason": reason,
            "summary": signal.summary,
            "rationale_text": signal.rationale_text,
        }
        self._append_jsonl(self.investment_journal_path, payload)

    def _load_investment_journal(self) -> list[dict]:
        if not self.investment_journal_path.exists():
            return []

        records = []
        with open(self.investment_journal_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return records

    @staticmethod
    def _append_jsonl(path, payload: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _resolve_signal_stop_loss_pct(self, signal: ReadingSignal) -> float | None:
        if signal.stop_loss_pct is not None:
            return signal.stop_loss_pct

        trade_style = self._normalize_trade_style_value(signal.trade_style)
        if trade_style == "daytrade":
            return self.daytrade_stop_loss_pct
        if trade_style == "swing":
            return self.swing_stop_loss_pct
        return self.default_stop_loss_pct

    def _auto_buy_min_confidence(self, trade_style: str | None) -> float:
        normalized = self._normalize_trade_style_value(trade_style)
        if normalized == "daytrade":
            return self.llm_daytrade_buy_min_confidence
        if normalized == "swing":
            return self.llm_swing_buy_min_confidence
        return self.llm_auto_buy_min_confidence

    def _autonomous_buy_risk_allows(
        self, code: str, active_trades: dict | None = None
    ) -> bool:
        active_trades = (
            active_trades if active_trades is not None else self._load_trades()
        )
        if (
            self.llm_auto_max_active_positions > 0
            and len(active_trades) >= self.llm_auto_max_active_positions
        ):
            logger.info(
                "LLM auto buy skipped: active positions {} reached limit {}",
                len(active_trades),
                self.llm_auto_max_active_positions,
            )
            return False

        state = self._load_autonomous_state()
        if (
            self.llm_auto_max_buys_per_day > 0
            and int(state.get("buy_count", 0)) >= self.llm_auto_max_buys_per_day
        ):
            logger.info(
                "LLM auto buy skipped: daily buy count {} reached limit {}",
                state.get("buy_count", 0),
                self.llm_auto_max_buys_per_day,
            )
            return False

        if self.llm_auto_symbol_cooldown_minutes > 0:
            last_action_at = self._last_autonomous_action_at(state, "buy", code)
            if last_action_at:
                elapsed = self._now_market_tz() - last_action_at
                cooldown = timedelta(minutes=self.llm_auto_symbol_cooldown_minutes)
                if elapsed < cooldown:
                    logger.info(
                        "LLM auto buy skipped: {} is in cooldown for {:.1f} more minutes",
                        code,
                        (cooldown - elapsed).total_seconds() / 60,
                    )
                    return False

        return True

    def _record_autonomous_action(self, side: str, code: str, company: str) -> None:
        state = self._load_autonomous_state()
        now = self._now_market_tz()
        actions = state.setdefault("last_actions", {})
        actions[f"{side}:{code}"] = {
            "company": company,
            "executed_at": now.isoformat(),
        }
        if side == "buy":
            state["buy_count"] = int(state.get("buy_count", 0)) + 1
        self._save_autonomous_state(state)

    def _load_autonomous_state(self) -> dict:
        today = self._today_market_date()
        if not self.autonomous_state_path.exists():
            return {"date": today, "buy_count": 0, "last_actions": {}}

        try:
            with open(self.autonomous_state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
        except Exception:
            return {"date": today, "buy_count": 0, "last_actions": {}}

        if state.get("date") != today:
            state["date"] = today
            state["buy_count"] = 0
            state.setdefault("last_actions", {})
        return state

    def _save_autonomous_state(self, state: dict) -> None:
        with open(self.autonomous_state_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _last_autonomous_action_at(
        self, state: dict, side: str, code: str
    ) -> datetime | None:
        action = state.get("last_actions", {}).get(f"{side}:{code}")
        if not isinstance(action, dict):
            return None
        executed_at_raw = action.get("executed_at")
        if not executed_at_raw:
            return None
        try:
            executed_at = datetime.fromisoformat(executed_at_raw)
        except ValueError:
            return None
        if executed_at.tzinfo is None:
            return executed_at.replace(tzinfo=self.market_timezone)
        return executed_at.astimezone(self.market_timezone)

    def _today_market_date(self) -> str:
        return self._now_market_tz().date().isoformat()

    def _build_autonomous_strategy_status(self) -> str:
        if self.signal_strategy not in {"llm_autonomous", "llm-auto", "auto"}:
            return ""

        state = self._load_autonomous_state()
        return (
            "🤖 **LLM 자율 전략**\n"
            f"• 자동매수: {state.get('buy_count', 0)}/{self.llm_auto_max_buys_per_day}회 "
            f"/ 최대 보유 {self.llm_auto_max_active_positions}종목\n"
            f"• 신뢰도 기준: 매수 {self.llm_auto_buy_min_confidence:.2f}, "
            f"매도 {self.llm_auto_sell_min_confidence:.2f}\n"
            f"• 종목별 매수 쿨다운: {self.llm_auto_symbol_cooldown_minutes}분\n\n"
            "📈 **KOSPI 선물 전략**\n"
            f"• 일 예산: {self.kospi_futures_daily_budget_krw:,}원\n"
            f"• 계약 예산: {self.kospi_futures_contract_budget_krw:,}원 / 계약\n"
            f"• 기본 종목코드: {self.kospi_futures_contract_code}\n"
            f"• 모드: {self.kospi_futures_mode}\n\n"
        )

    def _build_kospi_futures_status(self) -> str:
        state = self._load_kospi_futures_state()
        open_quantity = int(state.get("open_quantity", 0) or 0)
        avg_entry_price = float(state.get("avg_entry_price", 0.0) or 0.0)
        realized_pnl_points = float(state.get("realized_pnl_points", 0.0) or 0.0)

        if self.kospi_futures_track_only:
            current_price = self.market_handler.fetch_domestic_future_price(
                self.kospi_futures_contract_code,
                market_cls_code=self.kospi_futures_market_cls_code,
            )
            unrealized_pnl_points = (
                (float(current_price) - avg_entry_price) * open_quantity
                if open_quantity > 0
                else 0.0
            )
            return (
                "📈 **KOSPI 선물 가상 추적**\n"
                f"• 종목코드: {self.kospi_futures_contract_code}\n"
                f"• 보유: {open_quantity}계약\n"
                f"• 평균 진입가: {avg_entry_price:,.2f}\n"
                f"• 현재가: {float(current_price):,.2f}\n"
                f"• 평가손익: {unrealized_pnl_points:+.2f}pt\n"
                f"• 누적 실현손익: {realized_pnl_points:+.2f}pt\n\n"
            )

        return ""

    def _build_daily_review(self, review_date: date) -> str:
        date_key = review_date.isoformat()
        journal_records = [
            record
            for record in self._load_investment_journal()
            if self._iso_date(record.get("recorded_at")) == date_key
        ]
        closed_trades = [
            trade
            for trade in self._load_history()
            if self._iso_date(trade.get("closed_at")) == date_key
        ]
        active_trades = self._load_trades()

        lines = [
            "---",
            "type: invest-diary",
            f"date: {date_key}",
            "system: tleading-llm-autonomous",
            "---",
            "",
            f"# 투자 복기 - {date_key}",
            "",
            "## 단타 vs 스윙 비교",
            "",
            "| 전략 | 신호 | 자동시도 | 확인요청 | 종료거래 | 실현손익 | 승률 | 보유 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]

        for style in ["daytrade", "swing", "manual", "unknown"]:
            style_journal = [
                record
                for record in journal_records
                if self._normalize_trade_style_value(record.get("trade_style")) == style
            ]
            style_trades = [
                trade
                for trade in closed_trades
                if self._normalize_trade_style_value(trade.get("trade_style")) == style
            ]
            style_active = [
                trade
                for trade in active_trades.values()
                if self._normalize_trade_style_value(trade.get("trade_style")) == style
            ]
            auto_attempts = sum(
                1
                for record in style_journal
                if str(record.get("decision", "")).startswith("auto_")
            )
            confirm_requests = sum(
                1
                for record in style_journal
                if str(record.get("decision", "")).startswith("confirm_")
            )
            realized_pnl = sum(int(trade.get("pnl", 0)) for trade in style_trades)
            wins = sum(1 for trade in style_trades if int(trade.get("pnl", 0)) > 0)
            win_rate = (wins / len(style_trades) * 100) if style_trades else 0.0
            lines.append(
                f"| {self._style_label(style)} | "
                f"{len(style_journal)} | "
                f"{auto_attempts} | "
                f"{confirm_requests} | "
                f"{len(style_trades)} | "
                f"{realized_pnl:+,} | "
                f"{win_rate:.0f}% | "
                f"{len(style_active)} |"
            )

        lines.extend(
            [
                "",
                "## 복기 포인트",
                "",
                self._build_daily_review_notes(
                    journal_records, closed_trades, active_trades
                ),
                "",
                "## 판단 로그",
                "",
                self._build_journal_markdown_table(journal_records),
                "",
                "## 종료 거래",
                "",
                self._build_closed_trades_markdown_table(closed_trades),
                "",
                "## 장마감 보유",
                "",
                self._build_active_trades_markdown_table(active_trades),
            ]
        )
        return "\n".join(lines)

    def _build_daily_review_notes(
        self,
        journal_records: list[dict],
        closed_trades: list[dict],
        active_trades: dict,
    ) -> str:
        auto_records = [
            record
            for record in journal_records
            if str(record.get("decision", "")).startswith("auto_")
        ]
        skipped_records = [
            record for record in journal_records if record.get("decision") == "skipped"
        ]
        total_pnl = sum(int(trade.get("pnl", 0)) for trade in closed_trades)

        return (
            f"- LLM 자동 시도: {len(auto_records)}건 / 스킵: {len(skipped_records)}건\n"
            f"- 당일 종료 거래 실현손익: {total_pnl:+,}원\n"
            f"- 장마감 보유 종목: {len(active_trades)}개\n"
            "- 단타는 자동 집행 후 짧은 손익 반응을 중심으로 보고, 스윙은 보유 논리 유지 여부를 다음 복기에서 비교합니다."
        )

    def _build_journal_markdown_table(self, journal_records: list[dict]) -> str:
        if not journal_records:
            return "_기록 없음_"

        lines = [
            "| 시간 | 전략 | 종목 | 액션 | 신뢰도 | 판단 | 사유 | 요약 |",
            "|---|---|---|---|---:|---|---|---|",
        ]
        for record in journal_records[-30:]:
            lines.append(
                "| "
                f"{self._hhmm(record.get('recorded_at'))} | "
                f"{self._style_label(self._normalize_trade_style_value(record.get('trade_style')))} | "
                f"{self._md_cell(record.get('company') or '-')} | "
                f"{self._md_cell(record.get('action') or '-')} | "
                f"{float(record.get('confidence') or 0):.2f} | "
                f"{self._md_cell(record.get('decision') or '-')} | "
                f"{self._md_cell(record.get('reason') or '-')} | "
                f"{self._md_cell(record.get('summary') or '-')} |"
            )
        return "\n".join(lines)

    def _build_closed_trades_markdown_table(self, closed_trades: list[dict]) -> str:
        if not closed_trades:
            return "_종료 거래 없음_"

        lines = [
            "| 시간 | 전략 | 종목 | 수량 | 매수가 | 매도가 | 실현손익 | 수익률 |",
            "|---|---|---|---:|---:|---:|---:|---:|",
        ]
        for trade in closed_trades:
            pnl_rate = float(trade.get("pnl_rate") or 0) * 100
            lines.append(
                "| "
                f"{self._hhmm(trade.get('closed_at'))} | "
                f"{self._style_label(self._normalize_trade_style_value(trade.get('trade_style')))} | "
                f"{self._md_cell(trade.get('company') or '-')} | "
                f"{int(trade.get('quantity') or 0)} | "
                f"{int(trade.get('buy_price') or 0):,} | "
                f"{int(trade.get('sell_price') or 0):,} | "
                f"{int(trade.get('pnl') or 0):+,} | "
                f"{pnl_rate:+.2f}% |"
            )
        return "\n".join(lines)

    def _build_active_trades_markdown_table(self, active_trades: dict) -> str:
        if not active_trades:
            return "_보유 종목 없음_"

        lines = [
            "| 전략 | 종목 | 수량 | 평단가 | 손절가 | 진입시각 |",
            "|---|---|---:|---:|---:|---|",
        ]
        for trade in active_trades.values():
            lines.append(
                "| "
                f"{self._style_label(self._normalize_trade_style_value(trade.get('trade_style')))} | "
                f"{self._md_cell(trade.get('company') or '-')} | "
                f"{int(trade.get('quantity') or 0)} | "
                f"{int(trade.get('entry_price') or 0):,} | "
                f"{int(trade.get('stop_loss_price') or 0):,} | "
                f"{self._hhmm(trade.get('entry_at'))} |"
            )
        return "\n".join(lines)

    def _write_daily_review_markdown(self, review_date: date, review: str) -> Path:
        self.obsidian_diary_dir.mkdir(parents=True, exist_ok=True)
        path = self.obsidian_diary_dir / f"{review_date.isoformat()}.md"
        with open(path, "w", encoding="utf-8") as f:
            f.write(review.rstrip() + "\n")
        return path

    def _append_daily_review_feedback_markdown(
        self, review_date: date, feedback: str
    ) -> None:
        self.obsidian_diary_dir.mkdir(parents=True, exist_ok=True)
        path = self.obsidian_diary_dir / f"{review_date.isoformat()}.md"
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n## 사용자 피드백\n")
            f.write(feedback.rstrip() + "\n")

    def _load_daily_reviews(self) -> dict:
        if not self.daily_reviews_path.exists():
            return {}
        try:
            with open(self.daily_reviews_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_daily_reviews(self, reviews: dict) -> None:
        with open(self.daily_reviews_path, "w", encoding="utf-8") as f:
            json.dump(reviews, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _build_daily_review_response(feedback: str, review_date: str) -> str:
        cleaned = feedback.strip()
        if not cleaned:
            return "피드백을 비워 두셨습니다."
        if cleaned.lower() == "skip":
            return f"{review_date} 복기 피드백을 건너뛰었습니다."
        return f"{review_date} 복기 피드백을 기록했습니다: {cleaned[:120]}"

    @staticmethod
    def _iso_date(raw_value) -> str | None:
        if not raw_value:
            return None
        try:
            return datetime.fromisoformat(str(raw_value)).date().isoformat()
        except ValueError:
            return None

    @staticmethod
    def _style_label(style: str) -> str:
        if style == "daytrade":
            return "단타"
        if style == "swing":
            return "스윙"
        if style == "manual":
            return "수동"
        return "미분류"

    @staticmethod
    def _hhmm(raw_value) -> str:
        if not raw_value:
            return "-"
        try:
            return datetime.fromisoformat(str(raw_value)).strftime("%H:%M")
        except ValueError:
            return "-"

    @staticmethod
    def _md_cell(raw_value) -> str:
        text = str(raw_value).replace("\n", " ").replace("|", "\\|").strip()
        if len(text) > 80:
            return text[:77] + "..."
        return text or "-"

    def _mark_trade_stop_loss_pending(
        self, code: str, pending: bool, trigger_price: float | None = None
    ):
        trades = self._load_trades()
        if code not in trades:
            return

        trades[code]["stop_loss_pending"] = pending
        if trigger_price is not None:
            trades[code]["last_stop_loss_trigger_price"] = trigger_price
            trades[code]["last_stop_loss_triggered_at"] = datetime.now().isoformat()

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    def _record_trade_snapshot(
        self,
        trade: dict,
        current_price: int,
        phase: str,
        tracked_at: datetime | None = None,
    ):
        tracked_at = tracked_at or datetime.now()
        history = self._load_tracking_history()
        entry_price = int(trade["entry_price"])
        quantity = int(trade["quantity"])
        profit_amount = (current_price - entry_price) * quantity
        profit_rate = (
            (current_price - entry_price) / entry_price if entry_price else 0.0
        )

        history.append(
            {
                "trade_id": trade.get("trade_id"),
                "company": trade["company"],
                "code": trade.get("code") or trade.get("pdno"),
                "quantity": quantity,
                "entry_price": entry_price,
                "trade_style": self._normalize_trade_style_value(
                    trade.get("trade_style")
                ),
                "current_price": current_price,
                "profit_amount": profit_amount,
                "profit_rate": profit_rate,
                "phase": phase,
                "tracked_at": tracked_at.isoformat(),
                "entry_at": trade.get("entry_at"),
            }
        )

        with open(self.trade_tracking_path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def _load_tracking_history(self) -> list[dict]:
        if not self.trade_tracking_path.exists():
            return []
        try:
            with open(self.trade_tracking_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []

    def _is_tracking_due(self, trade: dict) -> bool:
        last_tracked_at_raw = trade.get("last_tracked_at")
        if not last_tracked_at_raw:
            return True

        try:
            last_tracked_at = datetime.fromisoformat(last_tracked_at_raw)
        except ValueError:
            return True

        next_tracking_at = last_tracked_at + timedelta(
            minutes=self.price_tracking_minutes
        )
        return datetime.now() >= next_tracking_at

    def _now_market_tz(self) -> datetime:
        return datetime.now(self.market_timezone)

    def _is_regular_market_open(self, now: datetime | None = None) -> bool:
        now = now or self._now_market_tz()
        if now.weekday() >= 5:
            return False
        current_time = now.time()
        return self.market_open_time <= current_time <= self.market_close_time

    def _should_send_post_market_briefing(self, now: datetime | None = None) -> bool:
        now = now or self._now_market_tz()
        return now.weekday() < 5 and now.time() > self.market_close_time

    def _is_weekend(self, now: datetime | None = None) -> bool:
        now = now or self._now_market_tz()
        return now.weekday() >= 5

    def _build_post_market_briefing(
        self, code: str, trade: dict, trade_date: date
    ) -> str | None:
        price_info = self.market_handler.fetch_price(code)
        output = price_info.get("output", {})
        current_price = self._to_int(output.get("stck_prpr"))
        if current_price <= 0:
            return None

        entry_price = int(trade["entry_price"])
        quantity = int(trade["quantity"])
        profit_amount = (current_price - entry_price) * quantity
        profit_rate = (
            ((current_price - entry_price) / entry_price) * 100 if entry_price else 0.0
        )

        investor_flow = self._extract_investor_flow_snapshot(code)
        volume = self._to_int(investor_flow.get("volume"))
        trade_value = self._to_int(investor_flow.get("trade_value"))
        foreign_buy = self._to_int(investor_flow.get("foreign_buy"))
        foreign_sell = self._to_int(investor_flow.get("foreign_sell"))
        foreign_net = self._to_int(investor_flow.get("foreign_net"))
        institution_buy = self._to_int(investor_flow.get("institution_buy"))
        institution_sell = self._to_int(investor_flow.get("institution_sell"))
        institution_net = institution_buy - institution_sell

        return (
            f"🧾 **[Daily Briefing]** {trade['company']}({code}) {trade_date.isoformat()}\n"
            f"• 매입가: {entry_price:,}원 / 종가 기준 현재가: {current_price:,}원\n"
            f"• 보유수량: {quantity}주 / 평가손익: {profit_amount:+,}원 ({profit_rate:+.2f}%)\n"
            f"• 거래량: {volume:,} / 거래대금: {trade_value:,}\n"
            f"• 외국인: 매수 {foreign_buy:,} / 매도 {foreign_sell:,} / 순매수 {foreign_net:+,}\n"
            f"• 기관: 매수 {institution_buy:,} / 매도 {institution_sell:,} / 순매수 {institution_net:+,}\n"
            f"• 장 종료 후에는 15분 추적 대신 일일 브리핑으로 마감합니다."
        )

    def _extract_investor_flow_snapshot(self, code: str) -> dict:
        payload = self.market_handler.fetch_investor_flow(code)
        raw = payload.get("output1") or payload.get("output") or {}
        row = raw[0] if isinstance(raw, list) and raw else raw
        if not isinstance(row, dict):
            return {}

        return {
            "foreign_buy": row.get("frgn_buy_vol"),
            "foreign_sell": row.get("frgn_sel_vol"),
            "foreign_net": row.get("frgn_ntby_qty"),
            "institution_buy": row.get("orgn_buy_vol"),
            "institution_sell": row.get("orgn_sel_vol"),
            "volume": row.get("acml_vol"),
            "trade_value": row.get("acml_tr_pbmn"),
        }

    @staticmethod
    def _to_int(value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(float(str(value).replace(",", "").strip()))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _parse_hhmm(raw_value: str) -> time:
        try:
            hour_raw, minute_raw = raw_value.strip().split(":", 1)
            hour = int(hour_raw)
            minute = int(minute_raw)
        except (AttributeError, ValueError) as exc:
            raise ValueError("time value must use HH:MM format") from exc

        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            raise ValueError("time value must use a valid HH:MM time")
        return time(hour, minute)

    @staticmethod
    def _normalize_trade_style_value(raw_value) -> str:
        normalized = str(raw_value or "").strip().lower()
        if normalized in {"daytrade", "short_term", "short-term", "단타"}:
            return "daytrade"
        if normalized in {"swing", "스윙"}:
            return "swing"
        if normalized == "manual":
            return "manual"
        return "unknown"

    def _update_trade_tracking_state(
        self, code: str, current_price: int, tracked_at: datetime
    ):
        trades = self._load_trades()
        trade = trades.get(code)
        if not trade:
            return

        trade["last_tracked_at"] = tracked_at.isoformat()
        trade["last_tracked_price"] = current_price
        trade["last_profit_rate"] = (
            (current_price - trade["entry_price"]) / trade["entry_price"]
            if trade.get("entry_price")
            else 0.0
        )

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    def _mark_trade_daily_briefing_sent(self, code: str, trade_date: date):
        trades = self._load_trades()
        trade = trades.get(code)
        if not trade:
            return

        trade["last_daily_briefing_date"] = trade_date.isoformat()

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _make_trade_id(code: str, entry_at: str) -> str:
        return f"{code}:{entry_at}"

    def _resolve_stop_loss(
        self,
        *,
        entry_price: int,
        stop_loss_price: int | None = None,
        stop_loss_pct: float | None = None,
    ) -> tuple[int | None, str]:
        if stop_loss_price is not None:
            if stop_loss_price <= 0:
                raise ValueError("손절가는 1원 이상이어야 합니다.")
            return stop_loss_price, f"지정가 {stop_loss_price:,}원"

        if stop_loss_pct is not None:
            if stop_loss_pct <= 0 or stop_loss_pct >= 1:
                raise ValueError("손절률은 0% 초과 100% 미만이어야 합니다.")
            return int(
                entry_price * (1 - stop_loss_pct)
            ), f"매매가 기준 {stop_loss_pct * 100:.1f}%"

        if self.default_stop_loss_price is not None:
            return (
                self.default_stop_loss_price,
                f"기본 지정가 {self.default_stop_loss_price:,}원",
            )

        if self.default_stop_loss_pct is not None:
            return (
                int(entry_price * (1 - self.default_stop_loss_pct)),
                f"매매가 기준 {self.default_stop_loss_pct * 100:.1f}%",
            )

        return None, "미설정"

    @staticmethod
    def _parse_stop_loss_pct(raw: str | None) -> float | None:
        if raw is None:
            return None

        normalized = raw.strip().replace("％", "%")
        if not normalized:
            return None

        is_percent = normalized.endswith("%")
        if is_percent:
            normalized = normalized[:-1].strip()

        value = float(normalized)
        if is_percent or value > 1:
            value = value / 100

        if value <= 0 or value >= 1:
            raise ValueError(
                "TLEADING_DEFAULT_STOP_LOSS_PCT must be between 0 and 100%."
            )
        return value

    @staticmethod
    def _parse_stop_loss_price(raw: str | None) -> int | None:
        if raw is None:
            return None

        normalized = raw.strip().replace(",", "")
        if not normalized:
            return None

        value = int(normalized)
        if value <= 0:
            raise ValueError("TLEADING_DEFAULT_STOP_LOSS_PRICE must be greater than 0.")
        return value

    @staticmethod
    def _format_stop_loss_price(stop_loss_price: int | None) -> str:
        if stop_loss_price is None:
            return "미설정"
        return f"{stop_loss_price:,}원"

    @staticmethod
    def _format_scheduled_stop_loss(order: dict) -> str:
        if order.get("stop_loss_price") is not None:
            return f"{int(order['stop_loss_price']):,}원"
        if order.get("stop_loss_pct") is not None:
            return f"{float(order['stop_loss_pct']) * 100:.1f}%"
        return "기본"

    @staticmethod
    def _make_scheduled_order_id(code: str, side: str, trigger_price: int) -> str:
        return f"{code}:{side}:{trigger_price}:{datetime.now().isoformat()}"

    @staticmethod
    def _should_execute_scheduled_order(
        side: str, current_price: int, trigger_price: int
    ) -> bool:
        if side == "buy":
            return current_price <= trigger_price
        if side == "sell":
            return current_price >= trigger_price
        return False

    @staticmethod
    def _resolve_sell_quantity(
        holding_quantity: int,
        *,
        quantity: int | None = None,
        ratio: float | None = None,
    ) -> int:
        if quantity is not None:
            if quantity <= 0:
                raise ValueError("매도 수량은 1주 이상이어야 합니다.")
            if quantity > holding_quantity:
                raise ValueError(
                    f"보유 수량({holding_quantity}주)을 초과해 매도할 수 없습니다."
                )
            return quantity

        if ratio is None:
            raise ValueError("매도 수량 또는 비율이 필요합니다.")

        if ratio <= 0 or ratio > 1:
            raise ValueError("매도 비율은 0 초과 100% 이하여야 합니다.")

        computed_quantity = max(1, math.floor(holding_quantity * ratio))
        return min(computed_quantity, holding_quantity)

    @staticmethod
    def _is_non_retryable_order_failure(message: str) -> bool:
        keywords = [
            "보유 내역이 없습니다",
            "초과해 매도할 수 없습니다",
            "수량은 1주 이상",
            "비율은 0 초과 100% 이하여야",
        ]
        return any(keyword in message for keyword in keywords)

    def _extract_price(self, res: dict, fallback_code: str | None = None) -> int:
        # KIS 주문 결과에서 가격 추출 (실제로는 체결 결과를 확인해야 정확하지만, 편의상 현재가 기반으로 추정하거나 응답에서 확인)
        # 시장가 주문의 경우 응답에 가격이 즉시 안 올 수 있으므로 현재가를 다시 조회하는 것이 안전
        code = res.get("output", {}).get("pdno") or fallback_code
        if code:
            price_info = self.market_handler.fetch_price(code)
            return int(price_info.get("output", {}).get("stck_prpr", 0))
        return 0
