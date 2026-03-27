"""
Flexible Stock Data Collector.
Collects 1-min candles (domestic/overseas) and 10-min investor flow (domestic).

Usage:
    python src/collect_stock_data.py --symbol 005930 --exchange 서울
    python src/collect_stock_data.py --symbol AAPL --exchange 나스닥
"""

import argparse
import asyncio
from datetime import datetime
import pandas as pd
from loguru import logger

from kis_market_handler import MarketHandler
from parquet_writer import daily_path, write_parquet

def parse_intraday(rows: list[dict], symbol: str, trade_date: str) -> pd.DataFrame:
    records = []
    for row in rows:
        time_str = row.get("stck_cntg_hour") or row.get("t_cntg_hour") or row.get("tm")
        if not time_str: continue
        
        # Format HHMMSS
        if len(time_str) == 4: # HHMM
            time_str += "00"
            
        try:
            ts = datetime.strptime(f"{trade_date} {time_str}", "%Y%m%d %H%M%S")
        except ValueError:
            continue

        records.append({
            "timestamp": ts,
            "symbol": symbol,
            "open": float(row.get("stck_oprc") or row.get("open") or 0),
            "high": float(row.get("stck_hgpr") or row.get("high") or 0),
            "low": float(row.get("stck_lwpr") or row.get("low") or 0),
            "close": float(row.get("stck_prpr") or row.get("last") or 0),
            "volume": float(row.get("cntg_vol") or row.get("tvol") or 0),
        })
    return pd.DataFrame(records)

def parse_investor_flow(rows: list[dict], symbol: str, trade_date: str) -> pd.DataFrame:
    records = []
    for row in rows:
        time_str = row.get("stck_cntg_hour")
        if not time_str: continue
        
        try:
            ts = datetime.strptime(f"{trade_date} {time_str}", "%Y%m%d %H%M%S")
        except ValueError:
            continue

        records.append({
            "timestamp": ts,
            "symbol": symbol,
            "price": float(row.get("stck_prpr") or 0),
            "foreigner_net_buy": int(row.get("fore_ntby_qty") or 0),
            "institution_net_buy": int(row.get("orgn_ntby_qty") or 0),
            "private_net_buy": int(row.get("prsn_ntby_qty") or 0),
        })
    return pd.DataFrame(records)

async def collect_data(symbol: str, exchange: str):
    handler = MarketHandler(exchange=exchange)
    today_str = datetime.now().strftime("%Y%m%d")
    today_iso = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Collecting data for {symbol} ({exchange}) ...")

    # 1. Collect Intraday Candles
    candle_rows = handler.fetch_intraday_candles(symbol)
    if candle_rows:
        df_candles = parse_intraday(candle_rows, symbol, today_str)
        if not df_candles.empty:
            path_key = "kr_stock_1min" if exchange == "서울" else "us_stock_1min"
            dest = daily_path(path_key, today_iso)
            write_parquet(df_candles, dest)
            logger.success(f"Saved {len(df_candles)} candles to {dest}")
    else:
        logger.warning(f"No candle data for {symbol}")

    # 2. Collect Investor Flow (Domestic Only)
    if exchange == "서울":
        flow_rows = handler.fetch_investor_flow(symbol)
        if flow_rows:
            df_flow = parse_investor_flow(flow_rows, symbol, today_str)
            if not df_flow.empty:
                dest = daily_path("investor_flow_10min", today_iso)
                write_parquet(df_flow, dest)
                logger.success(f"Saved {len(df_flow)} investor flow rows to {dest}")
        else:
            logger.warning(f"No investor flow data for {symbol}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="Stock symbol (e.g. 005930, AAPL)")
    parser.add_argument("--exchange", default="서울", help="Exchange (서울, 나스닥, 뉴욕)")
    args = parser.parse_args()

    asyncio.run(collect_data(args.symbol, args.exchange))
