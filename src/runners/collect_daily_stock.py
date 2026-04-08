"""
Collect Daily OHLCV data for domestic stocks.
Example: python src/collect_daily_stock.py --symbols 005930,000660
"""
import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path


import argparse
import asyncio
from datetime import datetime
import pandas as pd
from loguru import logger

from core.kis_market_handler import MarketHandler
from storage.parquet_writer import get_dir, write_parquet

def parse_daily_ohlcv(rows: list[dict], symbol: str) -> pd.DataFrame:
    records = []
    for row in rows:
        date_str = row.get("stck_bsop_date")
        if not date_str:
            continue
        
        try:
            ts = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue

        records.append({
            "timestamp": ts,
            "symbol": symbol,
            "open": float(row.get("stck_oprc") or 0),
            "high": float(row.get("stck_hgpr") or 0),
            "low": float(row.get("stck_lwpr") or 0),
            "close": float(row.get("stck_clpr") or 0),
            "volume": float(row.get("acml_vol") or 0),
            "value": float(row.get("acml_tr_pbmn") or 0),
        })
    return pd.DataFrame(records)

async def collect_daily_data(symbols: list[str], limit: int = 100):
    handler = MarketHandler(exchange="서울")
    dest_dir = get_dir("kr_stock_daily")
    
    for symbol in symbols:
        logger.info(f"Fetching daily OHLCV for {symbol} (limit={limit}) ...")
        try:
            rows = handler.fetch_ohlcv(symbol, timeframe="D", limit=limit)
            if rows:
                df = parse_daily_ohlcv(rows, symbol)
                if not df.empty:
                    # Save to a single file per stock for daily, or one big file?
                    # Let's use <symbol>.parquet in kr_stock_daily/
                    dest = dest_dir / f"{symbol}.parquet"
                    write_parquet(df, dest)
                    logger.success(f"Saved {len(df)} days of data for {symbol} to {dest}")
                else:
                    logger.warning(f"No valid data parsed for {symbol}")
            else:
                logger.warning(f"No data returned for {symbol}")
        except Exception as e:
            logger.error(f"Error collecting data for {symbol}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default="005930,000660", help="Comma-separated stock symbols")
    parser.add_argument("--limit", type=int, default=1000, help="Number of days to fetch")
    args = parser.parse_args()

    symbol_list = [s.strip() for s in args.symbols.split(",")]
    asyncio.run(collect_daily_data(symbol_list, args.limit))
