from pathlib import Path

import polars as pl
import pytest

from strategies.ma_goldencross import add_signals, backtest_ma_cross


KOSPI_1MIN_PATH = Path("src/data/market_data/kr/kospi200/1min/2026-06-29.parquet")


def test_ma_goldencross_runs_on_collected_kospi_1min_data():
    if not KOSPI_1MIN_PATH.exists():
        pytest.skip(f"Collected KOSPI 1min data not found: {KOSPI_1MIN_PATH}")

    df = pl.read_parquet(KOSPI_1MIN_PATH)
    signals = add_signals(df)

    assert signals.height == df.height
    assert {"short_ma", "long_ma", "buy_signal", "sell_signal"}.issubset(signals.columns)
    assert signals["symbol"].n_unique() > 1
    assert signals.select(pl.col("buy_signal").sum()).item() > 0
    assert signals.select(pl.col("sell_signal").sum()).item() > 0

    first_symbol = signals["symbol"][0]
    result = backtest_ma_cross(
        signals.filter(pl.col("symbol") == first_symbol).sort("timestamp"),
        initial_balance=1_000_000,
    )

    assert result["initial_balance"] == 1_000_000
    assert result["final_value"] > 0
    assert isinstance(result["trades"], list)
