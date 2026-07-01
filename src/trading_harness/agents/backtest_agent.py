from __future__ import annotations

from pathlib import Path

import polars as pl

from trading_harness.backtest.engine import LongOnlyBacktestEngine
from trading_harness.config import load_yaml
from trading_harness.data.repository import ParquetRepository


class BacktestAgent:
    def __init__(
        self,
        ticker_config_path: str | Path = "configs/tickers.yaml",
        strategy_config_path: str | Path = "configs/strategies.yaml",
        backtest_config_path: str | Path = "configs/backtest.yaml",
    ) -> None:
        self.ticker_config = load_yaml(ticker_config_path)
        self.strategy_config = load_yaml(strategy_config_path)
        self.backtest_config = load_yaml(backtest_config_path)
        self.repository = ParquetRepository()

    def run(self) -> Path:
        config = self.backtest_config["backtest"]
        prices = self.repository.read(Path(self.ticker_config["data"]["features_dir"]) / "feature_dataset.parquet")
        engine = LongOnlyBacktestEngine(
            fee_rate=config.get("fee_rate", 0.0005),
            slippage_rate=config.get("slippage_rate", 0.0005),
            trading_days_per_year=config.get("trading_days_per_year", 252),
        )
        frames = []
        for name, strategy_config in self.strategy_config["strategies"].items():
            output = strategy_config.get("output")
            if not output or not self.repository.exists(output):
                continue
            signals = self.repository.read(output)
            frames.append(engine.run(prices, signals, name))
        results = pl.concat(frames, how="vertical") if frames else pl.DataFrame()
        return self.repository.write(results, config["output"])
