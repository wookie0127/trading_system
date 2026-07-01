from __future__ import annotations

from pathlib import Path

import polars as pl

from trading_harness.config import load_yaml
from trading_harness.data.repository import ParquetRepository
from trading_harness.strategies.trend_following import SimpleTrendFollowingStrategy
from trading_harness.strategies.us_market_shock_inverse import USMarketShockInverseStrategy


class StrategyAgent:
    def __init__(
        self,
        ticker_config_path: str | Path = "configs/tickers.yaml",
        strategy_config_path: str | Path = "configs/strategies.yaml",
    ) -> None:
        self.ticker_config = load_yaml(ticker_config_path)
        self.strategy_config = load_yaml(strategy_config_path)
        self.repository = ParquetRepository()

    def run(self) -> dict[str, Path]:
        feature_path = Path(self.ticker_config["data"]["features_dir"]) / "feature_dataset.parquet"
        features = self.repository.read(feature_path)
        outputs: dict[str, Path] = {}
        configs = self.strategy_config["strategies"]

        shock_config = configs["us_market_shock_inverse"]
        if shock_config.get("enabled", True):
            strategy = USMarketShockInverseStrategy(
                qqq_symbol=shock_config.get("qqq_symbol", "qqq"),
                vix_symbol=shock_config.get("vix_symbol", "vix"),
                qqq_return_threshold=shock_config.get("qqq_return_threshold", -0.015),
                vix_change_threshold=shock_config.get("vix_change_threshold", 0.05),
            )
            signals = strategy.generate(features)
            outputs[strategy.name] = self.repository.write(signals, shock_config["output"])

        trend_config = configs["trend_following"]
        if trend_config.get("enabled", True):
            strategy = SimpleTrendFollowingStrategy(symbols=trend_config.get("symbols"))
            signals = strategy.generate(features)
            outputs[strategy.name] = self.repository.write(signals, trend_config["output"])

        return outputs

    def load_signals(self) -> dict[str, pl.DataFrame]:
        signals = {}
        for name, config in self.strategy_config["strategies"].items():
            output = config.get("output")
            if output and self.repository.exists(output):
                signals[name] = self.repository.read(output)
        return signals
