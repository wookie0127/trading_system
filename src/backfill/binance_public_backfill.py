"""
Binance Public Data Historical Backfiller.
Downloads 1-minute Kline data (Spot) from data.binance.vision since a start date.
Saves data into daily parquet files using storage.parquet_writer.
"""

import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path


import argparse
import asyncio
import io
import zipfile
from datetime import date, timedelta
import httpx
import pandas as pd
from loguru import logger

from storage.data_validation import validate_intraday
from storage.parquet_writer import daily_path, write_parquet, is_symbol_in_data

# Base URLs
SPOT_MONTHLY_BASE = "https://data.binance.vision/data/spot/monthly/klines"
SPOT_DAILY_BASE = "https://data.binance.vision/data/spot/daily/klines"


def get_date_ranges(
    start_date: date, end_date: date
) -> tuple[list[tuple[int, int]], list[date]]:
    """
    Split the range [start_date, end_date] into completed calendar months
    and remaining individual days in the current/ongoing month.
    """
    months = []
    days = []

    current = start_date
    today = date.today()

    while current <= end_date:
        # Determine the last day of the current month
        if current.month == 12:
            next_month_start = date(current.year + 1, 1, 1)
        else:
            next_month_start = date(current.year, current.month + 1, 1)

        last_day_of_month = next_month_start - timedelta(days=1)

        # Monthly file is generated when today has completely passed the month
        if last_day_of_month <= end_date and today > last_day_of_month:
            months.append((current.year, current.month))
            current = next_month_start
        else:
            days.append(current)
            current += timedelta(days=1)

    return months, days


async def download_url(client: httpx.AsyncClient, url: str) -> bytes | None:
    for attempt in range(3):
        try:
            resp = await client.get(url, timeout=30.0)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"Error downloading {url} (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2.0)
    return None


def process_zip_content(content: bytes, symbol: str) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        names = z.namelist()
        if not names:
            return pd.DataFrame()
        # Expecting a single CSV inside the ZIP archive
        with z.open(names[0]) as f:
            df = pd.read_csv(f, header=None)

    # Columns:
    # 0: Open time (ms)
    # 1: Open
    # 2: High
    # 3: Low
    # 4: Close
    # 5: Volume (Base asset)
    # 6: Close time (ms)
    # 7: Quote asset volume (equivalent to trade_value)
    df = df.rename(
        columns={
            0: "timestamp",
            1: "open",
            2: "high",
            3: "low",
            4: "close",
            5: "volume",
            7: "trade_value",
        }
    )

    if not df.empty:
        first_val = df["timestamp"].iloc[0]
        if first_val > 1e16:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ns")
        elif first_val > 1e13:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    df["symbol"] = symbol

    cols = [
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = 0.0

    return df[cols]


async def backfill_monthly(
    client: httpx.AsyncClient,
    symbol: str,
    year: int,
    month: int,
    skip_existing: bool,
) -> bool:
    if month == 12:
        next_month_start = date(year + 1, 1, 1)
    else:
        next_month_start = date(year, month + 1, 1)

    start_of_month = date(year, month, 1)
    month_dates = []
    curr = start_of_month
    while curr < next_month_start:
        month_dates.append(curr)
        curr += timedelta(days=1)

    if skip_existing:
        all_exist = True
        for d in month_dates:
            path = daily_path("crypto_1min", str(d))
            if not is_symbol_in_data(path, symbol):
                all_exist = False
                break
        if all_exist:
            logger.info(
                f"Skipping monthly {symbol} {year}-{month:02d} (all daily files exist)"
            )
            return True

    url = f"{SPOT_MONTHLY_BASE}/{symbol}/1m/{symbol}-1m-{year}-{month:02d}.zip"
    content = await download_url(client, url)
    if not content:
        logger.warning(f"Monthly file not found or failed to download: {url}")
        return False

    try:
        df = process_zip_content(content, symbol)
        if df.empty:
            return False

        df = validate_intraday(df, symbol=symbol)

        # Split monthly df into daily files
        df["_date"] = df["timestamp"].dt.date
        for day, group in df.groupby("_date"):
            dest = daily_path("crypto_1min", str(day))
            write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

        logger.success(
            f"Successfully processed and saved monthly data for {symbol} {year}-{month:02d}"
        )
        return True
    except Exception as e:
        logger.error(
            f"Error processing monthly data for {symbol} {year}-{month:02d}: {e}"
        )
        return False


async def backfill_daily(
    client: httpx.AsyncClient,
    symbol: str,
    target_date: date,
    skip_existing: bool,
) -> bool:
    dest = daily_path("crypto_1min", str(target_date))
    if skip_existing and is_symbol_in_data(dest, symbol):
        logger.info(f"Skipping daily {symbol} {target_date} (file already exists)")
        return True

    year, month, day = target_date.year, target_date.month, target_date.day
    url = f"{SPOT_DAILY_BASE}/{symbol}/1m/{symbol}-1m-{year}-{month:02d}-{day:02d}.zip"
    content = await download_url(client, url)
    if not content:
        logger.warning(f"Daily file not found or failed to download: {url}")
        return False

    try:
        df = process_zip_content(content, symbol)
        if df.empty:
            return False

        df = validate_intraday(df, symbol=symbol)
        write_parquet(df, dest)
        logger.success(
            f"Successfully processed and saved daily data for {symbol} {target_date}"
        )
        return True
    except Exception as e:
        logger.error(f"Error processing daily data for {symbol} {target_date}: {e}")
        return False


async def backfill_symbol(
    symbol: str,
    start_date: date,
    end_date: date,
    skip_existing: bool,
) -> None:
    logger.info(f"Starting backfill for {symbol}: {start_date} → {end_date}")
    months, days = get_date_ranges(start_date, end_date)

    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. Process Monthly Files
        for year, month in months:
            await backfill_monthly(client, symbol, year, month, skip_existing)
            await asyncio.sleep(1.0)  # Rate limiting safety sleep

        # 2. Process Daily Files
        for d in days:
            await backfill_daily(client, symbol, d, skip_existing)
            await asyncio.sleep(1.0)


async def main():
    parser = argparse.ArgumentParser(
        description="Binance Public Data Historical Backfiller (1m)"
    )
    parser.add_argument(
        "--symbols", default="BTCUSDT,ETHUSDT", help="Comma-separated symbols"
    )
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument(
        "--end", default=None, help="End date (YYYY-MM-DD), default is yesterday"
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Force overwrite existing files"
    )
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start_date = date.fromisoformat(args.start)
    end_date = (
        date.fromisoformat(args.end) if args.end else (date.today() - timedelta(days=1))
    )
    skip_existing = not args.overwrite

    logger.info("Binance Backfill Configuration:")
    logger.info(f"  Symbols:       {symbols}")
    logger.info(f"  Date Range:    {start_date} -> {end_date}")
    logger.info(f"  Skip Existing: {skip_existing}")

    for symbol in symbols:
        await backfill_symbol(symbol, start_date, end_date, skip_existing)


if __name__ == "__main__":
    asyncio.run(main())
