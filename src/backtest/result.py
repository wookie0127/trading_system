from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True)
class BacktestResult:
    symbol: str
    stock_name: str
    ohlcv: pl.DataFrame
    signals: pl.DataFrame
    trades: pl.DataFrame
    equity_curve: pl.DataFrame
    metrics: dict[str, float | int | str]
