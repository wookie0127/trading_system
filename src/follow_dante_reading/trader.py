import anyio
import anyio.abc
import json
import os
from datetime import datetime

from loguru import logger
from tenacity import AsyncRetrying, wait_exponential, stop_after_attempt, retry_if_exception_type

from core.kis_market_handler import MarketHandler
from bots.notifier import Notifier
from follow_dante_reading.client import get_discord_input
from follow_dante_reading.signal_schema import ReadingSignal, project_root


class DanteTrader:
    def __init__(self, notifier: Notifier | None = None, is_mock: bool = False):
        self.market_handler = MarketHandler()
        self.notifier = notifier or Notifier()
        self.active_trades_path = project_root() / "data" / "follow_dante_reading" / "active_trades.json"
        self.trade_history_path = project_root() / "data" / "follow_dante_reading" / "trade_history.json"
        self.active_trades_path.parent.mkdir(parents=True, exist_ok=True)
        self.is_mock = is_mock
        self.default_stop_loss_pct = float(os.getenv("DANTE_DEFAULT_STOP_LOSS_PCT", "0.05"))
        self.order_quantity = int(os.getenv("DANTE_ORDER_QUANTITY", "1"))
        self.holdings_poll_seconds = int(os.getenv("DANTE_HOLDINGS_POLL_SECONDS", "300"))
        self.auto_stop_loss_enabled = os.getenv("DANTE_AUTO_STOP_LOSS_ENABLED", "true").lower() == "true"
        if self.is_mock:
            logger.info("DanteTrader initialized in MOCK MODE (No real trades)")
        logger.info(
            "DanteTrader config: "
            f"order_quantity={self.order_quantity}, "
            f"default_stop_loss_pct={self.default_stop_loss_pct:.4f}, "
            f"holdings_poll_seconds={self.holdings_poll_seconds}, "
            f"auto_stop_loss_enabled={self.auto_stop_loss_enabled}, "
            f"is_mock={self.is_mock}"
        )

    async def handle_signal(self, signal: ReadingSignal, tg: anyio.abc.TaskGroup | None = None):
        """매매 신호를 처리하고 필요 시 Discord 컨펌을 요청합니다."""
        # 1. 시그널 요약 다이어리에 기록
        summary_msg = f"📔 **[Dante Diary]** {signal.company_name or '시황 요약'}\n• 요약: {signal.summary}\n• 판단: {signal.action} (신뢰도: {signal.confidence:.2f})\n• 근거: {signal.rationale_text}"
        await self.notifier.notify_diary(summary_msg)

        # 2. 매매 액션 처리
        if signal.action == "ignore":
            return

        company = signal.company_name
        if not company:
            logger.info(f"Signal action is {signal.action} but company name is missing. Skipping trade.")
            return

        code = self.market_handler.get_code(company)
        if not code:
            logger.warning(f"Could not find code for {company}. Skipping trade.")
            return

        if signal.action == "buy_candidate":
            active_trades = self._load_trades()
            if code in active_trades:
                logger.info(f"{company}({code}) is already in active trades. Skipping duplicate buy.")
                return
            await self._confirm_and_buy(company, code, signal)
        elif signal.action == "sell":
            await self._confirm_and_sell(company, code, signal)

    async def _confirm_and_buy(self, company: str, code: str, signal: ReadingSignal):
        """매수 컨펌 및 실행"""
        prompt = (
            f"📢 **[Trade Confirm]** '{company}'({code})를 매수할까요? "
            f"(신뢰도: {signal.confidence:.2f})\n"
            f"• 응답: `buy` 또는 `skip` (`y`/`yes`도 가능)\n"
            f"• 원문: {signal.rationale_text[:100]}..."
        )
        answer = await get_discord_input(prompt)

        if self._is_trade_confirmed(answer, expected_action="buy"):
            quantity = self.order_quantity
            price = 0

            # 손절가 결정
            sl_pct = signal.stop_loss_pct if signal.stop_loss_pct is not None else self.default_stop_loss_pct
            sl_label = f"{sl_pct*100:.1f}%"

            if self.is_mock:
                # 모의 투자: 현재가 조회 후 성공 처리
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                sl_price = int(price * (1 - sl_pct))

                success_msg = (
                    f"🍦 **[Mock Buy]** {company} {quantity}주 매수 완료\n"
                    f"• 체결가: {price:,}원\n"
                    f"• 손절라인: {sl_price:,}원 (신호가 기준 {sl_label} 하락 시 자동 매도 예정)"
                )
                await self.notifier.notify_all(success_msg)
                await self.notifier.notify_diary(f"✅ [Mock Buy Success] {company} @ {price:,}원 (SL: {sl_price:,}원)")
                self._save_trade(company, code, quantity, price, "buy", stop_loss_price=sl_price)
            else:
                # 실제 투자
                res = self.market_handler.create_market_buy_order(code, quantity)
                if res.get("rt_cd") == "0":
                    price = self._extract_price(res)
                    sl_price = int(price * (1 - sl_pct))

                    success_msg = (
                        f"✅ **[Buy Success]** {company} {quantity}주 매수 완료\n"
                        f"• 평균가: {price:,}원\n"
                        f"• 손절 예약: {sl_price:,}원 ({sl_label}) 설정 완료"
                    )
                    await self.notifier.notify_all(success_msg)
                    await self.notifier.notify_diary(f"✅ [Buy Success] {company} @ {price:,}원 (SL: {sl_price:,}원)")

                    # 실제 예약 매도 주문 로직 (KIS API에 따라 구현 필요, 여기서는 기록 후 감시)
                    self._save_trade(company, code, quantity, price, "buy", stop_loss_price=sl_price)
                else:
                    fail_msg = f"❌ **[Buy Fail]** {company} 매수 실패: {res.get('msg1')}"
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
            f"📢 **[Trade Confirm]** 보유 중인 '{company}'({code})를 매도할까요?\n"
            f"• 응답: `sell` 또는 `skip` (`y`/`yes`도 가능)\n"
            f"• 원문: {signal.rationale_text[:100]}..."
        )
        answer = await get_discord_input(prompt)

        if self._is_trade_confirmed(answer, expected_action="sell"):
            quantity = active_trades[code]["quantity"]
            price = 0

            if self.is_mock:
                # 모의 투자: 현재가 조회 후 성공 처리
                price_info = self.market_handler.fetch_price(code)
                price = int(price_info.get("output", {}).get("stck_prpr", 0))
                success_msg = f"🍦 **[Mock Sell]** {company} {quantity}주 매도 시뮬레이션 완료 (매도가: {price:,}원)"
                await self.notifier.notify_all(success_msg)
                self._save_trade(company, code, quantity, price, "sell")
            else:
                # 실제 투자
                res = self.market_handler.create_market_sell_order(code, quantity)
                if res.get("rt_cd") == "0":
                    price = self._extract_price(res)
                    success_msg = f"✅ **[Sell Success]** {company} {quantity}주 전량 매도 완료 (매도가: {price:,}원)"
                    await self.notifier.notify_all(success_msg)

                    # 매도 성공 시 이력 저장 (실현 손익 계산 포함)
                    entry_price = active_trades[code]["entry_price"]
                    pnl = (price - entry_price) * quantity
                    pnl_rate = (price - entry_price) / entry_price
                    self._save_history(company, code, quantity, entry_price, price, pnl, pnl_rate)

                    self._save_trade(company, code, quantity, price, "sell")
                else:
                    fail_msg = f"❌ **[Sell Fail]** {company} 매도 실패: {res.get('msg1')}"
                    await self.notifier.notify_all(fail_msg)
        else:
            await self.notifier.notify_all(f"🚫 {company} 매도를 거절하셨습니다.")

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
            return normalized in {"sell", "s", "y", "yes", "네", "ㅇㅇ", "ok", "매도", "정리"}

        return False

    async def track_holdings_loop(self, tg: anyio.abc.TaskGroup):
        """주기적으로 보유 주식 현황과 손절 트리거를 체크합니다."""
        logger.info(f"Starting holdings tracking loop (Interval: {self.holdings_poll_seconds} seconds)")
        while True:
            try:
                await self.report_holdings(tg)
            except Exception as e:
                logger.error(f"Error in tracking loop: {e}")

            await anyio.sleep(self.holdings_poll_seconds)

    async def report_holdings(self, tg: anyio.abc.TaskGroup):
        """현재 보유 종목의 수익률 현황을 Discord로 보고합니다."""
        # API 호출 안정성을 위해 tenacity 적용
        async for attempt in AsyncRetrying(
            wait=wait_exponential(multiplier=1, min=2, max=10),
            stop=stop_after_attempt(3),
            retry=retry_if_exception_type(Exception),
            reraise=True
        ):
            with attempt:
                active_trades = self._load_trades()
                if not active_trades:
                    return

                msg = "📊 **[Hourly Status]** 현재 보유 종목 현황\n"
                has_updates = False

                for code, data in active_trades.items():
                    price_info = self.market_handler.fetch_price(code)
                    current_price = float(price_info.get("output", {}).get("stck_prpr", 0))
                    if current_price == 0:
                        continue

                    entry_price = data["entry_price"]
                    profit_rate = (current_price - entry_price) / entry_price
                    stop_loss_price = int(data.get("stop_loss_price") or 0)

                    status_emoji = "📈" if profit_rate > 0 else "📉"
                    stop_loss_suffix = f", SL {stop_loss_price:,}원" if stop_loss_price > 0 else ""
                    msg += (
                        f"• {data['company']}: {current_price:,}원 "
                        f"({status_emoji} {profit_rate*100:+.2f}%{stop_loss_suffix})\n"
                    )
                    has_updates = True

                    stop_loss_pending = bool(data.get("stop_loss_pending"))
                    threshold_price = stop_loss_price or int(entry_price * (1 - self.default_stop_loss_pct))

                    if self.auto_stop_loss_enabled and threshold_price > 0 and current_price <= threshold_price:
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
        cash = int(balance_info.get("output2", [{}])[0].get("dnca_tot_amt", 0)) if not self.is_mock else 10000000 # 모의는 1천만 시작 가정

        report = f"💰 **[Account Status]**\n• **예수금**: {cash:,}원\n\n"

        # 2. 보유 종목 현황
        active_trades = self._load_trades()
        if active_trades:
            report += "📂 **현재 보유 종목**\n"
            total_eval = 0
            for code, data in active_trades.items():
                price_info = self.market_handler.fetch_price(code)
                curr_price = int(price_info.get("output", {}).get("stck_prpr", 0))
                eval_pnl = (curr_price - data['entry_price']) * data['quantity']
                eval_rate = (curr_price - data['entry_price']) / data['entry_price']
                total_eval += curr_price * data['quantity']
                stop_loss_price = int(data.get("stop_loss_price") or 0)

                emoji = "📈" if eval_pnl >= 0 else "📉"
                stop_loss_suffix = f", SL {stop_loss_price:,}원" if stop_loss_price > 0 else ""
                report += (
                    f"• {data['company']}: {curr_price:,}원 "
                    f"({emoji} {eval_rate*100:+.2f}%, {eval_pnl:+,}원{stop_loss_suffix})\n"
                )
            report += f"  (보유종목 총 평가액: {total_eval:,}원)\n\n"
        else:
            report += "📂 **현재 보유 종목**: 없음\n\n"

        # 3. 실현 손익 (매매 이력 기반)
        history = self._load_history()
        if history:
            report += "🏁 **최근 실현 손익 (History)**\n"
            total_pnl = 0
            # 최근 10개까지만 표시
            for item in history[-10:]:
                total_pnl += item['pnl']
                emoji = "🔥" if item['pnl'] >= 0 else "🧊"
                report += f"• {item['company']}: {item['pnl_rate']*100:+.2f}% ({item['pnl']:+,}원)\n"
            report += f"**▶️ 누적 실현 손익**: {total_pnl:+,}원"
        else:
            report += "🏁 **최근 실현 손익**: 이력 없음"

        return report

    def _save_history(self, company: str, code: str, quantity: int, buy_price: int, sell_price: int, pnl: int, pnl_rate: float):
        history = self._load_history()
        history.append({
            "company": company,
            "code": code,
            "quantity": quantity,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "pnl": pnl,
            "pnl_rate": pnl_rate,
            "closed_at": datetime.now().isoformat()
        })
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
                await self.notifier.notify_diary(f"✅ [Mock Stop Loss] {company} @ {price:,}원")
                pnl = (price - entry_price) * quantity
                pnl_rate = (price - entry_price) / entry_price
                self._save_history(company, code, quantity, entry_price, price, pnl, pnl_rate)
                self._save_trade(company, code, quantity, price, "sell")
                return

            res = self.market_handler.create_market_sell_order(code, quantity)
            if res.get("rt_cd") == "0":
                price = self._extract_price(res) or int(current_price)
                success_msg = (
                    f"🛑 **[Stop Loss Sell Success]** {company} {quantity}주 자동 손절 완료 "
                    f"(매도가: {price:,}원)"
                )
                await self.notifier.notify_all(success_msg)
                await self.notifier.notify_diary(f"🛑 [Stop Loss Sold] {company} @ {price:,}원")

                pnl = (price - entry_price) * quantity
                pnl_rate = (price - entry_price) / entry_price
                self._save_history(company, code, quantity, entry_price, price, pnl, pnl_rate)
                self._save_trade(company, code, quantity, price, "sell")
            else:
                self._mark_trade_stop_loss_pending(code, False)
                fail_msg = f"❌ **[Stop Loss Sell Fail]** {company} 자동 손절 실패: {res.get('msg1')}"
                await self.notifier.notify_all(fail_msg)
        except Exception as exc:
            self._mark_trade_stop_loss_pending(code, False)
            await self.notifier.notify_all(f"❌ **[Stop Loss Exception]** {company}: {exc}")

    def _save_trade(self, company: str, code: str, quantity: int, price: int, action: str, stop_loss_price: int | None = None):
        trades = self._load_trades()
        if action == "buy":
            trades[code] = {
                "company": company,
                "quantity": quantity,
                "entry_price": price,
                "stop_loss_price": stop_loss_price,
                "stop_loss_pending": False,
                "entry_at": datetime.now().isoformat()
            }
        elif action == "sell":
            if code in trades:
                del trades[code]

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    def _load_trades(self) -> dict:
        if not self.active_trades_path.exists():
            return {}
        try:
            with open(self.active_trades_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _mark_trade_stop_loss_pending(self, code: str, pending: bool, trigger_price: float | None = None):
        trades = self._load_trades()
        if code not in trades:
            return

        trades[code]["stop_loss_pending"] = pending
        if trigger_price is not None:
            trades[code]["last_stop_loss_trigger_price"] = trigger_price
            trades[code]["last_stop_loss_triggered_at"] = datetime.now().isoformat()

        with open(self.active_trades_path, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)

    def _extract_price(self, res: dict) -> int:
        # KIS 주문 결과에서 가격 추출 (실제로는 체결 결과를 확인해야 정확하지만, 편의상 현재가 기반으로 추정하거나 응답에서 확인)
        # 시장가 주문의 경우 응답에 가격이 즉시 안 올 수 있으므로 현재가를 다시 조회하는 것이 안전
        code = res.get("output", {}).get("pdno")
        if code:
            price_info = self.market_handler.fetch_price(code)
            return int(price_info.get("output", {}).get("stck_prpr", 0))
        return 0
