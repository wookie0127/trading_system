"""
NASDAQ index intraday data collector (1-minute candles).

Source: yfinance
Target: ^IXIC (NASDAQ Composite) or ^NDX (NASDAQ 100)
Storage: market_data/us/nasdaq/1min/<YYYY-MM-DD>.parquet
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta

import pandas as pd
import yfinance as yf
from loguru import logger

from data_validation import validate_intraday
from parquet_writer import daily_path, write_parquet

# yfinance supports up to 7 days of 1-min history per request
_MAX_DAYS_PER_CHUNK = 7
_DEFAULT_SYMBOL = "^IXIC"


def _fetch_chunk(symbol: str, start: date, end: date) -> pd.DataFrame:
    """Download 1-min OHLCV for [start, end) from yfinance."""
    ticker = yf.Ticker(symbol)
    df = ticker.history(
        start=start.isoformat(),
        end=end.isoformat(),
        interval="1m",
        auto_adjust=True,
    )
    if df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    df = df.rename(columns={
        "Datetime": "timestamp",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
    })
    df["symbol"] = symbol

    # Normalize timestamp to UTC-aware or tz-naive
    if df["timestamp"].dt.tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)

    return df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]]


async def collect_nasdaq(
    symbol: str = _DEFAULT_SYMBOL,
    start: date | None = None,
    end: date | None = None,
) -> None:
    """
    Collect 1-min NASDAQ data for the given date range and write to parquet.

    yfinance limits 1-min history to the past ~30 days. Iterates in 7-day
    chunks to stay within that constraint.

    Args:
        symbol: yfinance ticker (default ^IXIC).
        start:  First date to collect (inclusive). Defaults to yesterday.
        end:    Last date to collect (inclusive). Defaults to today.
    """
    today = date.today()
    if end is None:
        end = today
    if start is None:
        start = end - timedelta(days=1)

    logger.info(f"Collecting NASDAQ {symbol} 1-min: {start} → {end}")

    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=_MAX_DAYS_PER_CHUNK), end + timedelta(days=1))
        logger.debug(f"  Fetching chunk {current} → {chunk_end - timedelta(days=1)}")

        df = await asyncio.to_thread(_fetch_chunk, symbol, current, chunk_end)

        if df.empty:
            logger.warning(f"  No data returned for {current} → {chunk_end}")
        else:
            df = validate_intraday(df, symbol=symbol)
            # Write per trading day
            for day, group in df.groupby(df["timestamp"].dt.date):
                dest = daily_path("nasdaq_1min", str(day))
                write_parquet(group.reset_index(drop=True), dest)

        current = chunk_end
        await asyncio.sleep(0.5)

    logger.success(f"NASDAQ collection complete: {symbol}")


if __name__ == "__main__":
    import argparse
    from datetime import date

    parser = argparse.ArgumentParser(description="Collect NASDAQ 1-min data")
    parser.add_argument("--symbol", default=_DEFAULT_SYMBOL)
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None

    asyncio.run(collect_nasdaq(symbol=args.symbol, start=start, end=end))
