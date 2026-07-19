from __future__ import annotations

from pathlib import Path

import polars as pl

from trading_harness.config import load_yaml
from trading_harness.data.repository import ParquetRepository
from trading_harness.features.technical_indicators import build_feature_dataset


class FeatureAgent:
    def __init__(self, config_path: str | Path = "configs/tickers.yaml") -> None:
        self.config = load_yaml(config_path)
        self.repository = ParquetRepository()

    def run(self) -> Path:
        raw_dir = Path(self.config["data"]["raw_dir"])
        frames = [
            self.repository.read(path)
            for path in sorted(self.repository.resolve(raw_dir).glob("*.parquet"))
        ]
        if not frames:
            raise FileNotFoundError(
                f"No raw parquet files found in {self.repository.resolve(raw_dir)}"
            )
        raw = pl.concat(frames, how="vertical")
        features = build_feature_dataset(raw)
        output = Path(self.config["data"]["features_dir"]) / "feature_dataset.parquet"
        return self.repository.write(features, output)
