from __future__ import annotations

import polars as pl

from trading_harness.strategies.base import BaseStrategy


class USMarketShockInverseStrategy(BaseStrategy):
    name = "us_market_shock_inverse"

    def __init__(
        self,
        qqq_symbol: str = "qqq",
        vix_symbol: str = "vix",
        qqq_return_threshold: float = -0.015,
        vix_change_threshold: float = 0.05,
        target_symbol: str = "kospi200_inverse",
    ) -> None:
        self.qqq_symbol = qqq_symbol
        self.vix_symbol = vix_symbol
        self.qqq_return_threshold = qqq_return_threshold
        self.vix_change_threshold = vix_change_threshold
        self.target_symbol = target_symbol

    def generate(self, features: pl.DataFrame) -> pl.DataFrame:
        qqq = (
            features.filter(pl.col("symbol") == self.qqq_symbol)
            .select(["date", "return_1d"])
            .rename({"return_1d": "qqq_return_1d"})
        )
        vix = (
            features.filter(pl.col("symbol") == self.vix_symbol)
            .select(["date", "return_1d"])
            .rename({"return_1d": "vix_change_1d"})
        )
        return (
            qqq.join(vix, on="date", how="inner")
            .with_columns(
                (
                    (pl.col("qqq_return_1d") <= self.qqq_return_threshold)
                    & (pl.col("vix_change_1d") >= self.vix_change_threshold)
                )
                .cast(pl.Int8)
                .alias("signal"),
                pl.lit(self.target_symbol).alias("symbol"),
                pl.lit(self.name).alias("strategy"),
            )
            .select(
                [
                    "date",
                    "symbol",
                    "strategy",
                    "signal",
                    "qqq_return_1d",
                    "vix_change_1d",
                ]
            )
            .sort("date")
        )
