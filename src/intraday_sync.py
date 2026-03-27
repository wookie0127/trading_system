"""
Intraday synchronization task (One-shot).
Designed to be called by Crontab or Prefect.
Fetches the most recent 30 minutes of data for all KOSPI 200 components.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime

import httpx
import pandas as pd
from loguru import logger

from kis_auth_handler import KISAuthHandler
from kis_config import API_ROOT
from kospi200_component_collector import load_components, collect_kospi200_components
from parquet_writer import daily_path, write_parquet


_ENDPOINT = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
_TR_ID = "FHKST03010200"

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
        if not time_str: continue
        try:
            ts = datetime.strptime(f"{trade_date} {time_str}", "%Y%m%d %H%M%S")
        except ValueError: continue
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

async def sync_symbol(client: httpx.AsyncClient, auth: KISAuthHandler, symbol: str) -> pd.DataFrame:
    headers = _build_headers(auth)
    params = {
        "FID_ETC_CLS_CODE":      "",
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":        symbol,
        "FID_INPUT_HOUR_1":      "", # Latency: Get top 30
        "FID_PW_DATA_INCU_YN":   "Y",
    }
    try:
        resp = await client.get(f"{API_ROOT}/{_ENDPOINT}", headers=headers, params=params)
        resp.raise_for_status()
        rows = resp.json().get("output2") or []
        if not rows: return pd.DataFrame()
        return _parse_output(rows, symbol, datetime.now().strftime("%Y%m%d"))
    except Exception as e:
        logger.error(f"[{symbol}] Sync error: {e}")
        return pd.DataFrame()

async def run_sync_all(target_date: str | None = None) -> tuple[int, int]:
    """A single execution run to sync all symbols once.
    Returns: (num_success, num_total)
    """
    now = datetime.now()
    if target_date:
        run_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        run_date = now.date()

    if run_date.weekday() >= 5:
        logger.warning(f"Market closed on {run_date} (Weekend). Skipping sync.")
        return 0, 0

    dest = daily_path("kospi200_1min", str(run_date))
    # Skip if past date and file already exists
    if run_date < now.date() and dest.exists():
        logger.info(f"Data for {run_date} already exists. Skipping.")
        return 0, 0

    logger.info(f"Initializing KIS Sync for {run_date}...")
    auth = KISAuthHandler()
    
    try:
        symbols = load_components(run_date)
    except FileNotFoundError:
        logger.info(f"Components missing for {run_date}. Fetching...")
        await collect_kospi200_components(run_date)
        symbols = load_components(run_date)

    success_count = 0
    total_count = len(symbols)
    
    async with httpx.AsyncClient(timeout=30) as client:
        for symbol in symbols:
            # Sync symbol logic for specific date
            df = await sync_symbol(client, auth, symbol)
            if not df.empty:
                write_parquet(df, dest)
                success_count += 1
            await asyncio.sleep(random.uniform(0.2, 0.5))
    
    logger.success(f"Sync complete for {success_count}/{total_count} symbols.")
    return success_count, total_count

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    asyncio.run(run_sync_all(args.date))
