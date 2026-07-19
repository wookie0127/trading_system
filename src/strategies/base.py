from abc import ABC, abstractmethod
import polars as pl


class BaseStrategy(ABC):
    @abstractmethod
    def generate_signals(self, df: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError(
            "Subclasses must implement the generate_signals method."
        )
