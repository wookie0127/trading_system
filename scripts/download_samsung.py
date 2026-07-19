import os
import sys
import polars as pl
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path

# Add src/ to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from loguru import logger
from data.resampler import resample_ohlcv

def download_and_resample_kospi_samples():
    symbols = {
        "005930": "005930.KS", # Samsung Electronics
        "000660": "000660.KS", # SK Hynix
    }
    
    # Define date range: last 28 days for 1-minute data (yfinance limit)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=28)
    
    raw_dir = Path("data/raw")
    processed_dir = Path("data/processed")
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    for code, yf_symbol in symbols.items():
        logger.info(f"Downloading historical 1m candles for {code} ({yf_symbol}) from {start_date} to {end_date}...")
        
        try:
            # Download 1m data in chunks of 7 days (yfinance limit for 1m interval)
            chunks = []
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=7), end_date)
                logger.debug(f"Fetching chunk {current_start} to {current_end}...")
                chunk = yf.download(
                    tickers=yf_symbol,
                    start=current_start.isoformat(),
                    end=current_end.isoformat(),
                    interval="1m",
                    auto_adjust=True,
                    progress=False
                )
                if not chunk.empty:
                    chunks.append(chunk)
                current_start = current_end
                
            if not chunks:
                logger.warning(f"No 1-minute data fetched for {yf_symbol}")
                continue
                
            raw_df = pd_concat_yfinance(chunks, code)
            if raw_df.is_empty():
                logger.warning(f"Parsed DataFrame for {code} is empty.")
                continue

            raw_path = raw_dir / f"{code}_1m.parquet"
            raw_df.write_parquet(raw_path)
            logger.success(f"Saved {raw_df.height} 1m candles for {code} to {raw_path}")
            
            # Resample to 5m, 15m, 30m, 1h, 4h, 1d
            timeframes = ["5m", "15m", "30m", "1h", "4h", "1d"]
            for tf in timeframes:
                logger.info(f"Resampling {code} to {tf}...")
                resampled = resample_ohlcv(raw_df, tf)
                out_path = processed_dir / f"{code}_{tf}.parquet"
                resampled.write_parquet(out_path)
                logger.success(f"Saved {resampled.height} candles of {tf} to {out_path}")
                
        except Exception as e:
            logger.exception(f"Failed to process KOSPI sample for {code}: {e}")

def pd_concat_yfinance(chunks: list, symbol: str) -> pl.DataFrame:
    import pandas as pd
    combined_pd = pd.concat(chunks)
    # Remove duplicate timestamps
    combined_pd = combined_pd[~combined_pd.index.duplicated(keep='first')].sort_index()
    
    # Convert index to column
    combined_pd = combined_pd.reset_index()
    
    # Flatten MultiIndex columns if present
    if isinstance(combined_pd.columns, pd.MultiIndex):
        combined_pd.columns = combined_pd.columns.get_level_values(0)
    
    # Rename columns to standard schema
    # Index is either 'Datetime' or 'Date'
    time_col = combined_pd.columns[0]
    combined_pd = combined_pd.rename(columns={
        time_col: "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume"
    })
    
    # Filter columns
    cols = ["timestamp", "open", "high", "low", "close", "volume"]
    combined_pd = combined_pd[cols]
    combined_pd["symbol"] = symbol
    
    # Convert to Polars
    df = pl.from_pandas(combined_pd)
    # Drop rows with null close
    df = df.drop_nulls(["close"])
    # Cast types
    df = df.with_columns(
        pl.col("timestamp").cast(pl.Datetime),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64)
    )
    return df.sort("timestamp")

if __name__ == "__main__":
    download_and_resample_kospi_samples()
