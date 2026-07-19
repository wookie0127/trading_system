import sqlite3
from pathlib import Path
import pandas as pd
from typing import Union

try:
    from src.core.config import DATA_PROCESSED_DIR, DB_PATH
except ImportError:
    from core.config import DATA_PROCESSED_DIR, DB_PATH


class MarketDataCollector:
    def __init__(
        self,
        parquet_path: Union[str, Path] = DATA_PROCESSED_DIR / "BTCUSDT_4h.parquet",
        db_path: Union[str, Path] = DB_PATH,
    ):
        self.parquet_path = Path(parquet_path)
        self.db_path = Path(db_path)

    def load_4h_candles(self) -> pd.DataFrame:
        """
        Loads BTCUSDT 4H candles dataframe with columns:
        ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        """
        if self.parquet_path.exists():
            df = pd.read_parquet(self.parquet_path)
            cols_map = {col: col.lower() for col in df.columns}
            df = df.rename(columns=cols_map)

            req = ["timestamp", "open", "high", "low", "close", "volume"]
            for col in req:
                if col not in df.columns:
                    if col == "timestamp" and "datetime" in df.columns:
                        df["timestamp"] = df["datetime"]
                    elif col == "timestamp" and "date" in df.columns:
                        df["timestamp"] = df["date"]

            df = df.sort_values(by="timestamp").reset_index(drop=True)
            return df[req]

        elif self.db_path.exists():
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql(
                    "SELECT * FROM daily_prices WHERE symbol='BTCUSDT'", conn
                )
                if not df.empty:
                    df["timestamp"] = df["date"]
                    return df[["timestamp", "open", "high", "low", "close", "volume"]]

        # Fallback dummy data if files don't exist
        dates = pd.date_range(end=pd.Timestamp.now(), periods=200, freq="4h")
        import numpy as np

        prices = 60000 + np.cumsum(np.random.randn(200) * 200)
        df = pd.DataFrame(
            {
                "timestamp": dates.astype(str),
                "open": prices,
                "high": prices + 150,
                "low": prices - 150,
                "close": prices + 20,
                "volume": np.random.randint(100, 1000, size=200),
            }
        )
        return df
