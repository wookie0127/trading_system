import os
import time
import requests
import polars as pl
from datetime import datetime, date
from pathlib import Path
from loguru import logger

def fetch_binance_1m(
    symbol: str = "BTCUSDT",
    start_date: str | date = "2026-06-01",
    end_date: str | date = "2026-07-10"
) -> pl.DataFrame:
    """
    Downloads historical 1-minute OHLCV data from Binance public REST API.
    Paginates using startTime in chunks of 1000 candles.
    """
    if isinstance(start_date, str):
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = datetime.combine(start_date, datetime.min.time())

    if isinstance(end_date, str):
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.combine(end_date, datetime.max.time())

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    current_ms = start_ms
    all_klines = []

    logger.info(f"Downloading {symbol} 1m data from {start_dt} to {end_dt}...")

    while current_ms < end_ms:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": current_ms,
            "limit": 1000
        }
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Error fetching data at timestamp {current_ms}: {e}")
            time.sleep(5)
            continue

        if not data:
            logger.info("No more data returned from Binance API.")
            break

        all_klines.extend(data)
        
        # Get the timestamp of the last item in this batch
        last_ts = data[-1][0]
        logger.debug(f"Fetched up to {datetime.fromtimestamp(last_ts / 1000)}")
        
        if last_ts >= end_ms:
            break

        # Next startTime is 1 minute (60,000 ms) after the last candle
        new_ms = last_ts + 60000
        if new_ms <= current_ms:
            # Prevent infinite loop in case of API anomaly
            break
        current_ms = new_ms
        time.sleep(0.1)  # Polite delay

    if not all_klines:
        logger.warning(f"No data fetched for {symbol}")
        return pl.DataFrame()

    # Process and build Polars DataFrame
    # Binance kline structure:
    # 0: Open time (ms)
    # 1: Open price
    # 2: High price
    # 3: Low price
    # 4: Close price
    # 5: Volume
    # ...
    records = []
    for kline in all_klines:
        ts_ms = kline[0]
        if ts_ms > end_ms:
            continue
        records.append({
            "timestamp": datetime.fromtimestamp(ts_ms / 1000),
            "symbol": symbol,
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
            "volume": float(kline[5]),
        })

    df = pl.DataFrame(records)
    # Sort and drop duplicates
    df = df.unique(subset=["timestamp", "symbol"]).sort("timestamp")
    logger.success(f"Successfully downloaded {df.height} 1m candles for {symbol}.")
    return df

def save_binance_data(df: pl.DataFrame, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)
    logger.info(f"Saved dataset to {path}")

if __name__ == "__main__":
    df = fetch_binance_1m("BTCUSDT", "2026-06-01", "2026-07-10")
    if not df.is_empty():
        save_binance_data(df, "data/raw/BTCUSDT_1m.parquet")
