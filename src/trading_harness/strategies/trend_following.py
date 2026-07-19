from __future__ import annotations

import polars as pl

from trading_harness.strategies.base import BaseStrategy


class SimpleTrendFollowingStrategy(BaseStrategy):
    name = "trend_following"

    def __init__(self, symbols: list[str] | None = None) -> None:
        self.symbols = symbols or ["qqq", "btc", "eth"]

    def generate(self, features: pl.DataFrame) -> pl.DataFrame:
        return (
            features.filter(pl.col("symbol").is_in(self.symbols))
            .with_columns(
                (
                    (pl.col("close") > pl.col("ma_20"))
                    & (pl.col("ma_20") > pl.col("ma_60"))
                )
                .cast(pl.Int8)
                .alias("signal"),
                pl.lit(self.name).alias("strategy"),
            )
            .select(
                [
                    "date",
                    "symbol",
                    "strategy",
                    "signal",
                    "close",
                    "ma_20",
                    "ma_60",
                    "return_1d",
                ]
            )
            .sort(["symbol", "date"])
        )
