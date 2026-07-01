from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl


class BaseStrategy(ABC):
    name: str

    @abstractmethod
    def generate(self, features: pl.DataFrame) -> pl.DataFrame:
        """Generate a signal DataFrame with date, symbol, strategy, signal."""
