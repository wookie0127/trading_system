"""
Backfill manager for domestic stocks.
Paginates backward through the intraday chart to collect as much history as KIS allows.
Typically retrieves about 7-8 trading days (~3,000 candles).

Usage:
    tmux new -s backfill
    uv run src/backfill_manager.py
"""

from __future__ import annotations

import asyncio
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from kis_market_handler import MarketHandler
from parquet_writer import daily_path, write_parquet


# --- Configuration ---
SYMBOLS_LIMIT = 200  # Number of stocks to backfill (e.g. KOSPI 200)
PAGES_LIMIT = 100    # KIS limit for backward pagination (approx 30*100 = 3000 rows)
DELAY_RANGE = (0.2, 0.5)
JSON_CODE_PATH = Path("data/kospi_code_list.json")


def load_stock_symbols() -> list[str]:
    """Load and filter standard 6-digit stock codes from JSON."""
    if not JSON_CODE_PATH.exists():
        logger.error(f"Symbols file missing: {JSON_CODE_PATH}")
        return []
        
    with open(JSON_CODE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Filter for valid 6-digit numeric codes
    symbols = []
    for item in data:
        code = item.get("code", "")
        if len(code) == 6 and code.isdigit():
            symbols.append(code)
    
    # Sort for deterministic processing
    return sorted(list(set(symbols)))


def parse_output(raw_rows: list[dict], symbol: str) -> pd.DataFrame:
    records = []
    # KIS returns today's date context? No, it usually has 'stck_bsop_date' or just uses today.
    # Actually, the chart API output2 doesn't always have a date field?
    # Wait, stck_cntg_hour is HHMMSS. 
    # Chart API FHKST03010200 output2 rows have stck_bsop_date (business date).
    
    for row in raw_rows:
        date_str = row.get("stck_bsop_date")
        time_str = row.get("stck_cntg_hour")
        if not date_str or not time_str:
            continue
            
        try:
            ts = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
        except ValueError:
            continue

        records.append({
            "timestamp": ts,
            "symbol": symbol,
            "open": float(row.get("stck_oprc", 0) or 0),
            "high": float(row.get("stck_hgpr", 0) or 0),
            "low": float(row.get("stck_lwpr", 0) or 0),
            "close": float(row.get("stck_prpr", 0) or 0),
            "volume": float(row.get("cntg_vol", 0) or 0),
        })
    return pd.DataFrame(records)


async def backfill_symbol(handler: MarketHandler, symbol: str) -> None:
    """Paginate backward for a single symbol and save day-wise."""
    logger.info(f"Backfilling {symbol} ...")
    
    cursor_time = "" # Empty for latest
    page_count = 0
    all_data = []

    while page_count < PAGES_LIMIT:
        rows = handler.fetch_domestic_intraday(symbol, end_time=cursor_time)
        if not rows:
            break
            
        df = parse_output(rows, symbol)
        if df.empty:
            break
            
        all_data.append(df)
        page_count += 1
        
        # Move cursor to 1 minute before the oldest in this batch
        oldest = df["timestamp"].min()
        cursor_time = (oldest - timedelta(minutes=1)).strftime("%H%M%S")
        
        # Small delay between pages
        await asyncio.sleep(random.uniform(0.1, 0.2))

    if not all_data:
        return

    combined = pd.concat(all_data).drop_duplicates(subset=["timestamp", "symbol"])
    
    # Save day-wise as per original convention
    for date_obj, group in combined.groupby(combined["timestamp"].dt.date):
        dest = daily_path("kospi200_1min", str(date_obj))
        write_parquet(group, dest)

    logger.success(f"[{symbol}] Backfill complete ({len(combined)} rows).")


async def main():
    symbols = load_stock_symbols()
    # Limit to e.g. top 200 for now
    target_symbols = symbols[:SYMBOLS_LIMIT]
    
    logger.info(f"Targeting {len(target_symbols)} symbols for backfill.")
    handler = MarketHandler(exchange="서울")
    
    for i, symbol in enumerate(target_symbols, 1):
        try:
            await backfill_symbol(handler, symbol)
            # Random delay between stocks
            await asyncio.sleep(random.uniform(*DELAY_RANGE))
        except Exception as e:
            logger.error(f"Error backfilling {symbol}: {e}")
            
    logger.success("Backfill process finished.")


if __name__ == "__main__":
    asyncio.run(main())
