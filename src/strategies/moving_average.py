from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from strategies.ma_goldencross import add_signals


@dataclass(frozen=True)
class MovingAverageCrossStrategy:
    name: str = "Moving Average Cross"

    def generate_signals(self, df: pl.DataFrame, fast_window: int = 20, slow_window: int = 60) -> pl.DataFrame:
        return add_moving_average_signals(df, fast_window=fast_window, slow_window=slow_window)


def add_moving_average_signals(df: pl.DataFrame, fast_window: int = 20, slow_window: int = 60) -> pl.DataFrame:
    if fast_window <= 0 or slow_window <= 0:
        raise ValueError("MA windows must be positive integers.")
    if fast_window >= slow_window:
        raise ValueError("Fast MA must be smaller than Slow MA.")

    signal_frame = add_signals(
        df,
        short_ma_period=fast_window,
        long_ma_period=slow_window,
    ).rename({"short_ma": "ma_fast", "long_ma": "ma_slow"})
    return signal_frame.with_columns(
        pl.when(pl.col("buy_signal"))
        .then(1)
        .when(pl.col("sell_signal"))
        .then(-1)
        .otherwise(0)
        .alias("signal")
    )
