from datetime import date, timedelta

import polars as pl

from trading_harness.backtest.engine import LongOnlyBacktestEngine
from trading_harness.backtest.metrics import max_drawdown


def test_max_drawdown():
    curve = [1.0, 1.2, 0.9, 1.1]

    assert round(max_drawdown(curve), 4) == -0.25


def test_long_only_backtest_engine_outputs_metrics():
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(5)]
    prices = pl.DataFrame(
        {
            "date": dates,
            "symbol": "qqq",
            "close": [100, 102, 104, 103, 105],
        }
    )
    signals = pl.DataFrame(
        {
            "date": dates,
            "symbol": "qqq",
            "signal": [0, 1, 1, 0, 0],
        }
    )

    results = LongOnlyBacktestEngine(fee_rate=0.0, slippage_rate=0.0).run(
        prices,
        signals,
        "trend_following",
    )

    row = results.to_dicts()[0]
    assert row["strategy"] == "trend_following"
    assert row["symbol"] == "qqq"
    assert row["number_of_trades"] == 1
