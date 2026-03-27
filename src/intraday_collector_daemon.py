"""
Intraday collector daemon for KR stocks.
Runs in the background, periodically fetching 1-minute candles.

Usage:
  nohup python -u src/intraday_collector_daemon.py > logs/intraday_daemon.log 2>&1 &
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, time as dt_time, timedelta

import httpx
import pandas as pd
from loguru import logger

from kis_auth_handler import KISAuthHandler
from kis_config import API_ROOT
from kospi200_component_collector import load_components
from parquet_writer import daily_path, write_parquet


# --- Configuration ---
POLL_INTERVAL_MINUTES = 10     # How often to trigger collection
RANDOM_DELAY_RANGE = (0.2, 0.8) # Random sleep (seconds) between individual stock requests
MARKET_OPEN = dt_time(9, 0)
MARKET_CLOSE = dt_time(15, 40) # A bit after 15:30 to catch final corrections

_ENDPOINT = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
_TR_ID = "FHKST03010200"


def _is_market_open() -> bool:
    """Check if KR market is currently open (Weekdays 09:00 - 15:40)."""
    now = datetime.now()
    if now.weekday() >= 5: # Saturday, Sunday
        return False
    
    current_time = now.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE


def _build_headers(auth: KISAuthHandler) -> dict:
    token = auth.get_valid_token()
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": auth.app_key,
        "appsecret": auth.app_secret,
        "tr_id": _TR_ID,
    }


def _parse_output(raw_rows: list[dict], symbol: str, trade_date: str) -> pd.DataFrame:
    records = []
    for row in raw_rows:
        time_str = row.get("stck_cntg_hour", "") # HHMMSS
        if not time_str:
            continue
        try:
            ts = datetime.strptime(f"{trade_date} {time_str}", "%Y%m%d %H%M%S")
        except ValueError:
            continue

        records.append({
            "timestamp":   ts,
            "symbol":      symbol,
            "open":        float(row.get("stck_oprc", 0) or 0),
            "high":        float(row.get("stck_hgpr", 0) or 0),
            "low":         float(row.get("stck_lwpr", 0) or 0),
            "close":       float(row.get("stck_prpr", 0) or 0),
            "volume":      float(row.get("cntg_vol", 0) or 0),
            "trade_value": float(row.get("acml_tr_pbmn", 0) or 0),
        })
    return pd.DataFrame(records)


async def fetch_latest_candles(
    client: httpx.AsyncClient, 
    auth: KISAuthHandler, 
    symbol: str
) -> pd.DataFrame:
    """Fetch the last 30 1-min candles for a symbol."""
    headers = _build_headers(auth)
    # FID_INPUT_HOUR_1 empty or current time fetches the most recent 30
    params = {
        "FID_ETC_CLS_CODE":      "",
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":        symbol,
        "FID_INPUT_HOUR_1":      "", 
        "FID_PW_DATA_INCU_YN":   "Y",
    }

    try:
        resp = await client.get(f"{API_ROOT}/{_ENDPOINT}", headers=headers, params=params)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.error(f"[{symbol}] API Error: {exc}")
        return pd.DataFrame()

    rows = payload.get("output2") or []
    if not rows:
        return pd.DataFrame()

    today_str = datetime.now().strftime("%Y%m%d")
    return _parse_output(rows, symbol, today_str)


async def run_poll_cycle(auth: KISAuthHandler, symbols: list[str]) -> None:
    """Perform a single pass over all target symbols."""
    logger.info(f"Starting collection cycle for {len(symbols)} symbols...")
    
    today = datetime.now().date()
    # Path where we save/deduplicate day's data
    dest = daily_path("kospi200_1min", str(today))
    
    async with httpx.AsyncClient(timeout=30) as client:
        for i, symbol in enumerate(symbols, 1):
            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(symbols)}...")

            df = await fetch_latest_candles(client, auth, symbol)
            
            if not df.empty:
                # write_parquet handles deduplication automatically
                write_parquet(df, dest)

            # Randomize delay to stay under rate limits and avoid detection
            delay = random.uniform(*RANDOM_DELAY_RANGE)
            await asyncio.sleep(delay)

    logger.success(f"Cycle completed. Data at {dest}")


async def main() -> None:
    logger.info("Initializing Intraday Collector Daemon...")
    from pathlib import Path
    Path("logs").mkdir(exist_ok=True)
    
    auth = KISAuthHandler()
    
    while True:
        now = datetime.now()
        
        if _is_market_open():
            try:
                # Load current components (e.g. KOSPI 200)
                try:
                    symbols = load_components(now.date())
                except FileNotFoundError:
                    logger.warning(f"Component file for {now.date()} not found. Fetching now...")
                    from kospi200_component_collector import collect_kospi200_components
                    await collect_kospi200_components(now.date())
                    symbols = load_components(now.date())
                
                start_time = time.time()
                await run_poll_cycle(auth, symbols)
                
                # Calculate sleep duration to maintain interval
                elapsed = time.time() - start_time
                wait_time = max(10, int((POLL_INTERVAL_MINUTES * 60) - elapsed))
                
                logger.info(f"Finished cycle. Waiting {wait_time/60:.1f} minutes until next...")
                await asyncio.sleep(float(wait_time))
                
            except Exception as e:
                logger.opt(exception=True).error(f"Error in daemon loop: {e}")
                # Wait a bit before retrying after a crash
                await asyncio.sleep(60)
        else:
            # Market closed - sleep longer or calculate time until opening
            if now.weekday() >= 5: # Weekend
                logger.info("Market closed (Weekend). Sleeping until tomorrow.")
                await asyncio.sleep(3600) # Check every hour
            else:
                curr_t = now.time()
                if curr_t < MARKET_OPEN:
                    logger.info("Market not open yet. Waiting...")
                    await asyncio.sleep(600)
                else:
                    logger.info("Market closed for the day. Sleeping until tomorrow.")
                    await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Daemon stopped by user.")
