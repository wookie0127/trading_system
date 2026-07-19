import pytest
from src.trading_system.execution.paper import PaperExecutionAdapter
from src.trading_system.execution.live import LiveExecutionAdapter
from src.trading_system.execution.shadow import ShadowExecutionAdapter
from src.trading_system.execution.factory import get_execution_adapter


def test_paper_execution_adapter():
    adapter = PaperExecutionAdapter()
    res = adapter.execute_order(
        decision_id="dec-123",
        side="BUY",
        order_type="MARKET",
        requested_price=60000.0,
        requested_quantity=0.1,
    )
    assert res["mode"] == "paper"
    assert res["status"] == "FILLED"
    assert res["filled_price"] > 60000.0  # Buy slippage applied
    assert res["fee"] > 0


def test_live_execution_adapter_safety_guard():
    # Without confirm_live=True
    adapter = LiveExecutionAdapter(confirm_live=False)
    res = adapter.execute_order(
        decision_id="dec-123",
        side="BUY",
        order_type="MARKET",
        requested_price=60000.0,
        requested_quantity=0.1,
    )
    assert res["mode"] == "live"
    assert res["status"] == "REJECTED_SAFETY_GUARD"
    assert "Safety confirmation missing" in res["error"]


def test_live_execution_adapter_max_value_guard():
    # With confirm_live=True, but value > max_order_value_usdt
    adapter = LiveExecutionAdapter(confirm_live=True, max_order_value_usdt=1000.0)
    res = adapter.execute_order(
        decision_id="dec-123",
        side="BUY",
        order_type="MARKET",
        requested_price=60000.0,
        requested_quantity=0.1,  # 6000 USDT > 1000 USDT
    )
    assert res["status"] == "REJECTED_MAX_VALUE_EXCEEDED"


def test_execution_adapter_factory():
    paper = get_execution_adapter("paper")
    assert isinstance(paper, PaperExecutionAdapter)

    shadow = get_execution_adapter("shadow")
    assert isinstance(shadow, ShadowExecutionAdapter)

    live = get_execution_adapter("live", confirm_live=False)
    assert isinstance(live, LiveExecutionAdapter)
