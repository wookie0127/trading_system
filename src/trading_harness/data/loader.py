from __future__ import annotations

from pathlib import Path

import polars as pl
import yfinance as yf

from trading_harness.data.normalizer import normalize_ohlcv
from trading_harness.data.repository import ParquetRepository


class MarketDataLoader:
    def __init__(self, repository: ParquetRepository | None = None) -> None:
        self.repository = repository or ParquetRepository()

    def load_local(self, path: str | Path) -> pl.DataFrame:
        return self.repository.read(path)

    def download_yfinance(
        self,
        ticker: str,
        symbol: str,
        start_date: str,
        end_date: str | None = None,
        interval: str = "1d",
    ) -> pl.DataFrame:
        raw = yf.download(
            ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        return normalize_ohlcv(raw, symbol)
