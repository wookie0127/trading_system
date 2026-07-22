import sqlite3
from pathlib import Path
import pandas as pd
from typing import Union
from loguru import logger

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

    def fetch_realtime_candles(self, interval: str = "15m", limit: int = 100) -> pd.DataFrame:
        """
        Fetches real-time candles from Binance public API.
        Supported intervals: 1m, 15m, 1h, 4h, 1d
        """
        import requests
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": "BTCUSDT",
            "interval": interval,
            "limit": limit
        }
        try:
            logger.info(f"Fetching real-time BTCUSDT candles from Binance (interval: {interval}, limit: {limit})...")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_volume", "number_of_trades",
                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
            ])
            
            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms").astype(str)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(float)
                
            return df[["timestamp", "open", "high", "low", "close", "volume"]]
        except Exception as e:
            logger.warning(f"Failed to fetch real-time Binance candles: {e}. Falling back to static data.")
            raise e

    def load_4h_candles(self) -> pd.DataFrame:
        """
        Loads BTCUSDT 4H candles. Now defaults to fetching real-time data from Binance,
        falling back to local files if the request fails.
        """
        try:
            return self.fetch_realtime_candles(interval="4h", limit=100)
        except Exception:
            pass

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
