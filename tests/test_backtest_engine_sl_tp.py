from datetime import datetime, timedelta
import polars as pl
from backtest.engine import run_backtest

class MockStrategy:
    def generate_signals(self, df):
        return df

def test_backtest_stop_loss_trigger():
    # Buy signal on index 0, price drops on index 2
    timestamps = [datetime(2026, 7, 1, 9) + timedelta(minutes=i) for i in range(4)]
    frame = pl.DataFrame({
        "timestamp": timestamps,
        "symbol": ["BTCUSDT"] * 4,
        "open": [100.0, 100.0, 95.0, 95.0],
        "high": [100.0, 100.0, 95.0, 95.0],
        "low": [100.0, 100.0, 95.0, 95.0],
        "close": [100.0, 100.0, 95.0, 95.0],
        "volume": [1000.0] * 4,
        "buy_signal": [True, False, False, False],
        "sell_signal": [False, False, False, False],
    })

    # Run backtest with 3% stop loss (SL price = 97.0)
    result = run_backtest(
        frame,
        symbol="BTCUSDT",
        initial_balance=1_000_000.0,
        fee=0.0,
        slippage=0.0,
        stop_loss_pct=0.03,
        strategy=MockStrategy()
    )

    trades = result.trades.to_dicts()
    assert len(trades) == 2  # BUY and SELL
    assert trades[0]["side"] == "BUY"
    assert trades[0]["price"] == 100.0
    assert trades[1]["side"] == "SELL"
    assert trades[1]["price"] == 97.0  # Executed at exact Stop Loss price
    assert trades[1]["reason"] == "Stop Loss"

def test_backtest_take_profit_trigger():
    # Buy signal on index 0, price rises on index 2
    timestamps = [datetime(2026, 7, 1, 9) + timedelta(minutes=i) for i in range(4)]
    frame = pl.DataFrame({
        "timestamp": timestamps,
        "symbol": ["BTCUSDT"] * 4,
        "open": [100.0, 100.0, 105.0, 105.0],
        "high": [100.0, 100.0, 105.0, 105.0],
        "low": [100.0, 100.0, 105.0, 105.0],
        "close": [100.0, 100.0, 105.0, 105.0],
        "volume": [1000.0] * 4,
        "buy_signal": [True, False, False, False],
        "sell_signal": [False, False, False, False],
    })

    # Run backtest with 4% take profit (TP price = 104.0)
    result = run_backtest(
        frame,
        symbol="BTCUSDT",
        initial_balance=1_000_000.0,
        fee=0.0,
        slippage=0.0,
        take_profit_pct=0.04,
        strategy=MockStrategy()
    )

    trades = result.trades.to_dicts()
    assert len(trades) == 2
    assert trades[0]["side"] == "BUY"
    assert trades[0]["price"] == 100.0
    assert trades[1]["side"] == "SELL"
    assert trades[1]["price"] == 104.0  # Executed at exact TP price
    assert trades[1]["reason"] == "Take Profit"

def test_backtest_simultaneous_sl_tp_hit_defaults_to_sl():
    # Buy signal on index 0. On index 1, candle high is 110, low is 90.
    # SL is 95 (-5%), TP is 105 (+5%). Both are hit in the same bar.
    timestamps = [datetime(2026, 7, 1, 9) + timedelta(minutes=i) for i in range(2)]
    frame = pl.DataFrame({
        "timestamp": timestamps,
        "symbol": ["BTCUSDT"] * 2,
        "open": [100.0, 100.0],
        "high": [100.0, 110.0],
        "low": [100.0, 90.0],
        "close": [100.0, 100.0],
        "volume": [1000.0] * 2,
        "buy_signal": [True, False],
        "sell_signal": [False, False],
    })

    result = run_backtest(
        frame,
        symbol="BTCUSDT",
        initial_balance=1_000_000.0,
        fee=0.0,
        slippage=0.0,
        stop_loss_pct=0.05,
        take_profit_pct=0.05,
        strategy=MockStrategy()
    )

    trades = result.trades.to_dicts()
    assert len(trades) == 2
    assert trades[1]["side"] == "SELL"
    assert trades[1]["price"] == 95.0  # Should trigger Stop Loss (95.0) rather than Take Profit (105.0)
    assert trades[1]["reason"] == "Stop Loss"
