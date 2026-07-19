from __future__ import annotations

import json
from datetime import datetime, timezone

import anyio

from follow_telegram_leading.signal_schema import ReadingSignal
from follow_telegram_leading.trader import TleadingTrader


class _FakeNotifier:
    def __init__(self):
        self.messages: list[str] = []

    async def notify_all(self, message: str):
        self.messages.append(message)

    async def notify_kospi_futures(self, message: str):
        self.messages.append(message)

    async def notify_diary(self, message: str):
        self.messages.append(message)


class _FakeMarketHandler:
    def __init__(self, *, buy_response: dict | None = None):
        self.calls: list[tuple[str, tuple, dict]] = []
        self.buy_response = buy_response or {"rt_cd": "0", "msg1": "OK"}

    def fetch_domestic_future_price(
        self, shtn_pdno: str, market_cls_code: str = "MKI"
    ) -> int:
        self.calls.append(
            (
                "fetch_domestic_future_price",
                (shtn_pdno,),
                {"market_cls_code": market_cls_code},
            )
        )
        return 345.0

    def create_futureoption_buy_order(
        self, shtn_pdno: str, quantity: int, account_product_code: str = "03"
    ):
        self.calls.append(
            (
                "create_futureoption_buy_order",
                (shtn_pdno, quantity),
                {"account_product_code": account_product_code},
            )
        )
        return self.buy_response

    def create_futureoption_sell_order(
        self, shtn_pdno: str, quantity: int, account_product_code: str = "03"
    ):
        self.calls.append(
            (
                "create_futureoption_sell_order",
                (shtn_pdno, quantity),
                {"account_product_code": account_product_code},
            )
        )
        return {"rt_cd": "0", "msg1": "OK"}


def _build_signal(action: str, text: str) -> ReadingSignal:
    return ReadingSignal(
        source="telegram:chart_master_kospi",
        message_id=1,
        posted_at=datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc),
        chat_id=3956165696,
        chat_title="차트마스터 코스피방",
        company_name=None,
        action=action,
        confidence=0.9,
        stop_loss_pct=None,
        entry_hint=None,
        rationale_text=text,
        summary=text,
        raw_text=text,
        trade_style="unknown",
        media_path=None,
        strategy_name="chart_master_kospi",
    )


def test_kospi_futures_mode_prefers_explicit_mode(monkeypatch):
    trader = TleadingTrader.__new__(TleadingTrader)
    monkeypatch.setenv("TLEADING_KOSPI_FUTURES_MODE", "paper")
    monkeypatch.setenv("TLEADING_KOSPI_FUTURES_TRACK_ONLY", "true")

    assert trader._resolve_kospi_futures_mode() == "paper"


def test_kospi_futures_mode_falls_back_to_legacy_track_only(monkeypatch):
    trader = TleadingTrader.__new__(TleadingTrader)
    monkeypatch.delenv("TLEADING_KOSPI_FUTURES_MODE", raising=False)
    monkeypatch.setenv("TLEADING_KOSPI_FUTURES_TRACK_ONLY", "true")

    assert trader._resolve_kospi_futures_mode() == "tracking"


def test_kospi_futures_live_mode_rejects_simulation_handler():
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.kospi_futures_mode = "live"
    trader.market_handler = type("Handler", (), {"is_simulation": True})()

    try:
        trader._validate_kospi_futures_mode()
    except ValueError as exc:
        assert "KIS_PROFILE=live" in str(exc)
    else:
        raise AssertionError(
            "expected live mode validation to reject simulation handler"
        )


def test_chart_master_kospi_signal_uses_futures_handler_and_budget(tmp_path):
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.notifier = _FakeNotifier()
    trader.market_handler = _FakeMarketHandler()
    trader.market_timezone = timezone.utc
    trader.kospi_futures_state_path = tmp_path / "kospi_futures_state.json"
    trader.kospi_futures_contract_code = "101W09"
    trader.kospi_futures_market_cls_code = "MKI"
    trader.kospi_futures_daily_budget_krw = 1_000_000
    trader.kospi_futures_contract_budget_krw = 1_000_000
    trader.kospi_futures_default_quantity = 1
    trader.kospi_futures_mode = "paper"
    trader.kospi_futures_track_only = False
    trader._today_market_date = lambda: "2026-06-30"
    trader._now_market_tz = lambda: datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc)
    trader._record_investment_journal = lambda *args, **kwargs: None

    anyio.run(
        trader._handle_chart_master_kospi_signal,
        _build_signal("buy_candidate", "코스피 2계약 매수진입"),
    )

    state = json.loads(trader.kospi_futures_state_path.read_text(encoding="utf-8"))
    assert state["open_quantity"] == 1
    assert any(
        call[0] == "create_futureoption_buy_order"
        for call in trader.market_handler.calls
    )
    buy_call = next(
        call
        for call in trader.market_handler.calls
        if call[0] == "create_futureoption_buy_order"
    )
    assert buy_call[1] == ("101W09", 1)
    assert buy_call[2]["account_product_code"] == "03"

    anyio.run(
        trader._handle_chart_master_kospi_signal,
        _build_signal("sell", "청산하겠습니다"),
    )

    state = json.loads(trader.kospi_futures_state_path.read_text(encoding="utf-8"))
    assert state["open_quantity"] == 0
    assert any(
        call[0] == "create_futureoption_sell_order"
        for call in trader.market_handler.calls
    )


def test_chart_master_kospi_blocks_repeated_buys_after_insufficient_orderable_amount(
    tmp_path,
):
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.notifier = _FakeNotifier()
    trader.market_handler = _FakeMarketHandler(
        buy_response={
            "rt_cd": "1",
            "msg_cd": "40250000",
            "msg1": "모의투자 주문가능금액이 부족합니다.",
        }
    )
    trader.market_timezone = timezone.utc
    trader.kospi_futures_state_path = tmp_path / "kospi_futures_state.json"
    trader.kospi_futures_contract_code = "101W09"
    trader.kospi_futures_market_cls_code = "MKI"
    trader.kospi_futures_daily_budget_krw = 1_000_000
    trader.kospi_futures_contract_budget_krw = 1_000_000
    trader.kospi_futures_default_quantity = 1
    trader.kospi_futures_mode = "paper"
    trader.kospi_futures_track_only = False
    trader._today_market_date = lambda: "2026-06-30"
    trader._now_market_tz = lambda: datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc)
    trader._record_investment_journal = lambda *args, **kwargs: None

    signal = _build_signal("buy_candidate", "코스피 1계약 매수진입")
    anyio.run(trader._handle_chart_master_kospi_signal, signal)
    anyio.run(trader._handle_chart_master_kospi_signal, signal)

    buy_calls = [
        call
        for call in trader.market_handler.calls
        if call[0] == "create_futureoption_buy_order"
    ]
    assert len(buy_calls) == 1

    state = json.loads(trader.kospi_futures_state_path.read_text(encoding="utf-8"))
    assert state["buy_blocked_date"] == "2026-06-30"
    assert state["buy_blocked_reason"] == "모의투자 주문가능금액이 부족합니다."
    assert any(
        "KOSPI Futures Budget Mismatch" in message
        for message in trader.notifier.messages
    )


def test_chart_master_kospi_track_only_updates_paper_position_and_pnl(tmp_path):
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.notifier = _FakeNotifier()
    trader.market_handler = _FakeMarketHandler()
    trader.market_timezone = timezone.utc
    trader.kospi_futures_state_path = tmp_path / "kospi_futures_state.json"
    trader.kospi_futures_contract_code = "101W09"
    trader.kospi_futures_market_cls_code = "MKI"
    trader.kospi_futures_daily_budget_krw = 1_000_000
    trader.kospi_futures_contract_budget_krw = 1_000_000
    trader.kospi_futures_default_quantity = 1
    trader.kospi_futures_mode = "tracking"
    trader.kospi_futures_track_only = True
    trader._today_market_date = lambda: "2026-06-30"
    trader._now_market_tz = lambda: datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc)
    trader._record_investment_journal = lambda *args, **kwargs: None

    anyio.run(
        trader._handle_chart_master_kospi_signal,
        _build_signal("buy_candidate", "코스피 2계약 매수진입"),
    )
    state = json.loads(trader.kospi_futures_state_path.read_text(encoding="utf-8"))
    assert state["open_quantity"] == 2
    assert state["avg_entry_price"] == 345.0
    assert not any(
        call[0] == "create_futureoption_buy_order"
        for call in trader.market_handler.calls
    )

    trader.market_handler.fetch_domestic_future_price = lambda *args, **kwargs: 347.5
    anyio.run(
        trader._handle_chart_master_kospi_signal,
        _build_signal("sell", "청산하겠습니다"),
    )

    state = json.loads(trader.kospi_futures_state_path.read_text(encoding="utf-8"))
    assert state["open_quantity"] == 1
    assert state["realized_pnl_points"] == 2.5
    assert any(
        "KOSPI Futures Paper Buy" in message for message in trader.notifier.messages
    )
    assert any(
        "KOSPI Futures Paper Sell" in message for message in trader.notifier.messages
    )


def test_handle_signal_skips_weekend_processing(tmp_path):
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.notifier = _FakeNotifier()
    trader.market_handler = _FakeMarketHandler()
    trader.market_timezone = timezone.utc
    trader._now_market_tz = lambda: datetime(2026, 7, 4, 9, 0, tzinfo=timezone.utc)

    anyio.run(
        trader.handle_signal, _build_signal("buy_candidate", "코스피 2계약 매수진입")
    )

    assert trader.notifier.messages == []
    assert trader.market_handler.calls == []


def test_daily_review_skips_weekend_processing(monkeypatch):
    trader = TleadingTrader.__new__(TleadingTrader)
    trader.market_timezone = timezone.utc
    trader.daily_review_time = datetime(2026, 7, 4, 15, 45, tzinfo=timezone.utc).time()
    trader._now_market_tz = lambda: datetime(2026, 7, 4, 16, 0, tzinfo=timezone.utc)
    trader._load_daily_reviews = lambda: (_ for _ in ()).throw(
        AssertionError("should not load reviews on weekend")
    )
    trader._save_daily_reviews = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("should not save reviews on weekend")
    )
    trader._build_daily_review = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("should not build review on weekend")
    )
    trader._write_daily_review_markdown = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("should not write review on weekend")
    )

    async def fail_input(*args, **kwargs):
        raise AssertionError("should not request review input on weekend")

    monkeypatch.setattr("follow_telegram_leading.trader.get_discord_input", fail_input)

    anyio.run(trader.process_daily_review)
