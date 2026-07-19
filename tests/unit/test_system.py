import pandas as pd
import numpy as np

from src.trading_system.features.technical import compute_technical_indicators
from src.trading_system.policy.state_machine import PositionStateMachine
from src.trading_system.risk.sizing import PositionSizer
from src.trading_system.risk.stop_loss import StopLossCalculator
from src.trading_system.risk.kill_switch import KillSwitch


def test_technical_indicators_calculation():
    dates = pd.date_range(start="2026-01-01", periods=100, freq="4h")
    prices = 60000 + np.cumsum(np.random.randn(100) * 100)
    df = pd.DataFrame(
        {
            "timestamp": dates.astype(str),
            "open": prices,
            "high": prices + 50,
            "low": prices - 50,
            "close": prices + 10,
            "volume": np.random.randint(10, 100, size=100),
        }
    )

    res_df = compute_technical_indicators(df)
    assert "ema_20" in res_df.columns
    assert "rsi_14" in res_df.columns
    assert "macd" in res_df.columns
    assert "atr_14" in res_df.columns
    assert "bollinger_upper" in res_df.columns
    assert len(res_df) == 100


def test_position_state_machine():
    assert PositionStateMachine.is_valid_transition("FLAT", "OPEN_LONG") is True
    assert PositionStateMachine.is_valid_transition("FLAT", "OPEN_SHORT") is True
    assert PositionStateMachine.is_valid_transition("LONG", "CLOSE") is True
    assert (
        PositionStateMachine.is_valid_transition("LONG", "OPEN_SHORT") is False
    )  # Immediate reversal invalid
    assert PositionStateMachine.is_valid_transition("SHORT", "OPEN_LONG") is False


def test_position_sizer_deterministic():
    size = PositionSizer.calculate_position_size(
        account_balance=10000.0,
        entry_price=60000.0,
        stop_loss_price=58000.0,  # 2000 stop distance
        max_risk_ratio=0.005,  # $50 risk allowed
        max_position_ratio=0.15,
        max_leverage=2.0,
    )
    # $50 risk / 2000 distance = 0.025 BTC
    assert size == 0.025


def test_stop_loss_calculator():
    sl_long = StopLossCalculator.calculate_stop_loss(
        "LONG", entry_price=60000.0, atr_14=1000.0, multiplier=2.0
    )
    assert sl_long == 58000.0

    sl_short = StopLossCalculator.calculate_stop_loss(
        "SHORT", entry_price=60000.0, atr_14=1000.0, multiplier=2.0
    )
    assert sl_short == 62000.0


def test_kill_switch_trigger():
    ks = KillSwitch(max_daily_loss_ratio=0.02, max_drawdown_ratio=0.08)
    assert (
        ks.check(daily_loss_ratio=0.01, drawdown_ratio=0.05, consecutive_losses=2)
        is False
    )
    assert (
        ks.check(daily_loss_ratio=0.025, drawdown_ratio=0.05, consecutive_losses=2)
        is True
    )
    assert ks.is_active is True
