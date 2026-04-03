"""
Historical Intraday (1-min) Backfiller for a specific stock using Yahoo Finance.
Downloads data from today backwards up to ~30 days (yfinance limit).
"""

import argparse
import asyncio
from datetime import date, timedelta
from loguru import logger

from parquet_writer import daily_path, write_parquet, is_symbol_in_data
from yahoo_finance_collector import _to_yf_kr, _download_1min

async def backfill_stock_intraday(symbol_raw: str, days: int = 30) -> dict:
    yf_symbol = _to_yf_kr(symbol_raw)
    today = date.today()
    
    logger.info(f"Starting 1-min backfill for {symbol_raw} ({yf_symbol}) for the last {days} days...")
    
    results = {"success": 0, "skipped": 0, "failed": 0, "dates": []}
    
    # yfinance 1-min data is only available for the last 30 days.
    for i in range(days):
        target_date = today - timedelta(days=i)
        if target_date.weekday() >= 5: continue
            
        dest = daily_path("kr_stock_1min", str(target_date))
        
        # Check if this specific symbol is already in the data for this day
        if is_symbol_in_data(dest, symbol_raw):
            logger.info(f"  {target_date}: Already collected {symbol_raw}. Skipping.")
            results["skipped"] += 1
            continue
            
        try:
            df = await asyncio.to_thread(_download_1min, [yf_symbol], target_date, target_date)
            if not df.empty:
                df["symbol"] = symbol_raw
                write_parquet(df, dest)
                logger.success(f"  Saved {len(df)} candles for {target_date}")
                results["success"] += 1
                results["dates"].append(str(target_date))
            else:
                logger.warning(f"  No data for {target_date} (possibly too old for 1-m or holiday)")
                results["failed"] += 1
        except Exception as e:
            logger.error(f"  Error on {target_date}: {e}")
            results["failed"] += 1
            
        await asyncio.sleep(1.0)
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Stock symbol (e.g. 005930)")
    parser.add_argument("--days", type=int, default=30, help="Number of days to go back (max ~30 for 1-m)")
    args = parser.parse_args()

    asyncio.run(backfill_stock_intraday(args.symbol, args.days))
