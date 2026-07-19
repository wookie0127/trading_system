from __future__ import annotations

from pathlib import Path

import polars as pl

from trading_harness.config import load_yaml
from trading_harness.data.loader import MarketDataLoader
from trading_harness.data.normalizer import normalize_ohlcv
from trading_harness.data.repository import ParquetRepository


class MarketDataAgent:
    def __init__(self, config_path: str | Path = "configs/tickers.yaml") -> None:
        self.config = load_yaml(config_path)
        self.repository = ParquetRepository()
        self.loader = MarketDataLoader(self.repository)

    def run(self, skip_existing: bool | None = None) -> list[Path]:
        market_config = self.config["market_data"]
        raw_dir = self.config["data"]["raw_dir"]
        skip = (
            market_config.get("skip_existing", True)
            if skip_existing is None
            else skip_existing
        )
        written: list[Path] = []

        for symbol, ticker in market_config["tickers"].items():
            output = Path(raw_dir) / f"{symbol}.parquet"
            if skip and self.repository.exists(output):
                written.append(self.repository.resolve(output))
                continue
            df = self.loader.download_yfinance(
                ticker=ticker,
                symbol=symbol,
                start_date=market_config["start_date"],
                end_date=market_config.get("end_date"),
                interval=market_config.get("interval", "1d"),
            )
            written.append(self.repository.write(df, output))
        return written

    def load_all_raw(self) -> pl.DataFrame:
        raw_dir = Path(self.config["data"]["raw_dir"])
        frames = []
        for symbol in self.config["market_data"]["tickers"]:
            path = raw_dir / f"{symbol}.parquet"
            if self.repository.exists(path):
                frames.append(normalize_ohlcv(self.repository.read(path), symbol))
        return pl.concat(frames, how="vertical") if frames else pl.DataFrame()
