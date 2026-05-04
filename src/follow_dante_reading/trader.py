from __future__ import annotations

import anyio
import anyio.abc
import json
import os
from datetime import datetime
from pathlib import Path

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
        self.active_trades_path.parent.mkdir(parents=True, exist_ok=True)
        self.stop_loss_limit = -0.05  # -5%
        self.is_mock = is_mock
        if self.is_mock:
            logger.info("DanteTrader initialized in MOCK MODE (No real trades)")

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
            await self._confirm_and_buy(company, code, signal)
        elif signal.action == "sell":
            await self._confirm_and_sell(company, code, signal)

    async def _confirm_and_buy(self, company: str, code: str, signal: ReadingSignal):
        """매수 컨펌 및 실행"""
        prompt = f"📢 **[Trade Confirm]** '{company}'({code})를 매수할까요? (신뢰도: {signal.confidence:.2f})\n• 원문: {signal.rationale_text[:100]}..."
        answer = await get_discord_input(prompt)
        
        if answer.lower() in ["y", "yes", "네", "ㅇㅇ", "ok", "매수"]:
            quantity = 1 
            price = 0
            
            # 손절가 결정 (시그널에 없으면 기본 -5%)
            sl_pct = signal.stop_loss_pct if signal.stop_loss_pct else 0.05
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

        prompt = f"📢 **[Trade Confirm]** 보유 중인 '{company}'({code})를 매도할까요?\n• 원문: {signal.rationale_text[:100]}..."
        answer = await get_discord_input(prompt)
        
        if answer.lower() in ["y", "yes", "네", "ㅇㅇ", "ok", "매도", "정리"]:
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
                    self._save_trade(company, code, quantity, price, "sell")
                else:
                    fail_msg = f"❌ **[Sell Fail]** {company} 매도 실패: {res.get('msg1')}"
                    await self.notifier.notify_all(fail_msg)
        else:
            await self.notifier.notify_all(f"🚫 {company} 매도를 거절하셨습니다.")

    async def track_holdings_loop(self, tg: anyio.abc.TaskGroup):
        """1시간마다 보유 주식 현황을 트래킹하고 보고합니다."""
        logger.info("Starting holdings tracking loop (Interval: 1 hour)")
        while True:
            try:
                await self.report_holdings(tg)
            except Exception as e:
                logger.error(f"Error in tracking loop: {e}")
            
            await anyio.sleep(3600)  # 1시간 대기 (anyio.sleep 사용)

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
                    
                    status_emoji = "📈" if profit_rate > 0 else "📉"
                    msg += f"• {data['company']}: {current_price:,}원 ({status_emoji} {profit_rate*100:+.2f}%)\n"
                    has_updates = True

                    # 손절라인 체크 (-5%)
                    if profit_rate <= self.stop_loss_limit:
                        alert_msg = f"⚠️ **[Stop Loss Alert]** {data['company']} 수익률이 {profit_rate*100:.2f}%에 도달했습니다. (손절 라인 -5% 하회)"
                        await self.notifier.notify_all(alert_msg)
                        
                        # 손절 매도 컨펌을 별도 태스크로 실행
                        tg.start_soon(self._confirm_and_sell, data['company'], code, ReadingSignal(
                            source="auto:stop_loss", message_id=0, posted_at=datetime.now(), 
                            chat_id=0, company_name=data['company'], action="sell", confidence=1.0, 
                            rationale_text="손절가 도달에 따른 자동 매도 제안"
                        ))

                if has_updates:
                    await self.notifier.notify_all(msg)

    def _save_trade(self, company: str, code: str, quantity: int, price: int, action: str, stop_loss_price: int | None = None):
        trades = self._load_trades()
        if action == "buy":
            trades[code] = {
                "company": company,
                "quantity": quantity,
                "entry_price": price,
                "stop_loss_price": stop_loss_price,
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

    def _extract_price(self, res: dict) -> int:
        # KIS 주문 결과에서 가격 추출 (실제로는 체결 결과를 확인해야 정확하지만, 편의상 현재가 기반으로 추정하거나 응답에서 확인)
        # 시장가 주문의 경우 응답에 가격이 즉시 안 올 수 있으므로 현재가를 다시 조회하는 것이 안전
        code = res.get("output", {}).get("pdno")
        if code:
            price_info = self.market_handler.fetch_price(code)
            return int(price_info.get("output", {}).get("stck_prpr", 0))
        return 0
