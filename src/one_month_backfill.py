"""
One-month backfill script for KOSPI 200 1-minute data using Yahoo Finance.
Iterates over the last 30 days to collect historical intraday data.
"""

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Add src to path if needed
sys.path.append(str(Path(__file__).parent))

from loguru import logger
from yahoo_finance_collector import collect_kospi200_intraday
from kospi200_component_collector import fetch_kospi200_components
from parquet_writer import daily_path

async def main():
    # 1. Get current KOSPI 200 codes
    logger.info("Fetching latest KOSPI 200 components list...")
    try:
        df_components = await fetch_kospi200_components()
    except Exception as e:
        logger.error(f"Failed to fetch components: {e}")
        return
        
    if df_components.empty:
        logger.error("KOSPI 200 components list is empty. Aborting.")
        return
    
    codes = df_components["symbol"].tolist()
    logger.success(f"Successfully loaded {len(codes)} KOSPI 200 components.")

    # 2. Define date range (last 30 days)
    today = date.today()
    start_date = today - timedelta(days=30)
    
    logger.info(f"Backfill range: {start_date} -> {today}")

    # 3. Iterate through each day
    for i in range(31): # 0 to 30 inclusive
        target_date = today - timedelta(days=i)
        
        # Skip weekends
        if target_date.weekday() >= 5:
            logger.info(f"Skipping {target_date} (Weekend/Holiday)")
            continue
            
        dest_file = daily_path("kospi200_1min", str(target_date))
        if dest_file.exists():
            logger.info(f"Data for {target_date} already exists at {dest_file}. Skipping.")
            continue

        logger.info(f"=== Downloading 1-min data for {target_date} ===")
        try:
            # We use the symbols we fetched at the beginning for the whole month backfill
            await collect_kospi200_intraday(trade_date=target_date, codes=codes)
        except Exception as e:
            logger.error(f"Failed to collect data for {target_date}: {e}")
        
        # Polite delay to avoid yfinance rate limits
        await asyncio.sleep(2.0)

    logger.success("One-month KOSPI 200 1-minute backfill process finished.")

if __name__ == "__main__":
    asyncio.run(main())
