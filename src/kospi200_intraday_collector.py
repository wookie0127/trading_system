"""
KOSPI200 intraday data collector (1-minute candles) via KIS OpenAPI.

KIS API:
  endpoint : uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice
  tr_id    : FHKST03010200

Returns up to 30 rows per call (30 1-min candles ending at FID_INPUT_HOUR_1).
We paginate backwards from market close (153000) to market open (090000).

Storage: market_data/kr/kospi200/1min/<YYYY-MM-DD>.parquet
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime

import httpx
import pandas as pd
from loguru import logger

from kis_auth_handler import KISAuthHandler
from kis_config import API_ROOT
from kospi200_component_collector import load_components
from data_validation import validate_intraday
from parquet_writer import daily_path, write_parquet

_ENDPOINT = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
_TR_ID = "FHKST03010200"

# KR market hours: 09:00 ~ 15:30
_MARKET_OPEN_HHMM = 900
_MARKET_CLOSE_HHMMSS = "153000"

# Rows returned per API call (KIS limit)
_ROWS_PER_CALL = 30

# Polite delay between API calls (seconds)
_API_DELAY = 0.2


def _build_headers(auth: KISAuthHandler) -> dict:
    token = auth.get_valid_token()
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": auth.app_key,
        "appsecret": auth.app_secret,
        "tr_id": _TR_ID,
    }


def _parse_output(raw_rows: list[dict], symbol: str, trade_date: date) -> pd.DataFrame:
    records = []
    for row in raw_rows:
        time_str = row.get("stck_cntg_hour", "")  # HHMMSS
        if not time_str:
            continue
        try:
            ts = datetime.strptime(
                f"{trade_date.strftime('%Y%m%d')} {time_str}", "%Y%m%d %H%M%S"
            )
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


async def _fetch_symbol_intraday(
    client: httpx.AsyncClient,
    auth: KISAuthHandler,
    symbol: str,
    trade_date: date,
) -> pd.DataFrame:
    """Paginate through the full trading day for a single symbol."""
    all_frames: list[pd.DataFrame] = []
    cursor_time = _MARKET_CLOSE_HHMMSS  # start from 15:30:00, go backwards

    while True:
        headers = _build_headers(auth)
        params = {
            "FID_ETC_CLS_CODE":      "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":        symbol,
            "FID_INPUT_HOUR_1":      cursor_time,
            "FID_PW_DATA_INCU_YN":   "Y",
        }

        try:
            resp = await client.get(
                f"{API_ROOT}/{_ENDPOINT}", headers=headers, params=params
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            logger.warning(f"[{symbol}] API error at {cursor_time}: {exc}")
            break

        rows = payload.get("output2") or []
        if not isinstance(rows, list) or not rows:
            break

        frame = _parse_output(rows, symbol, trade_date)
        if not frame.empty:
            all_frames.append(frame)

        # Oldest timestamp in this batch → move cursor back 1 minute
        oldest_time = frame["timestamp"].min() if not frame.empty else None
        if oldest_time is None:
            break

        oldest_hhmm = oldest_time.hour * 100 + oldest_time.minute
        if oldest_hhmm <= _MARKET_OPEN_HHMM:
            break

        # Next cursor: 1 minute before the oldest candle in this batch
        prev_minute = oldest_time - pd.Timedelta(minutes=1)
        cursor_time = prev_minute.strftime("%H%M%S")

        await asyncio.sleep(_API_DELAY)

    if not all_frames:
        return pd.DataFrame()

    combined = pd.concat(all_frames).drop_duplicates(subset=["timestamp", "symbol"])
    return combined.sort_values("timestamp").reset_index(drop=True)


async def collect_kospi200_intraday(trade_date: date | None = None) -> None:
    """
    Collect 1-min intraday data for all KOSPI200 components on *trade_date*.

    Requires the component list to already be saved locally.
    Run kospi200_component_collector.py first.
    """
    if trade_date is None:
        trade_date = date.today()

    symbols = load_components(trade_date)
    logger.info(f"Collecting KOSPI200 intraday for {trade_date}: {len(symbols)} stocks")

    auth = KISAuthHandler()
    dest = daily_path("kospi200_1min", str(trade_date))

    async with httpx.AsyncClient(timeout=60) as client:
        for i, symbol in enumerate(symbols, 1):
            logger.debug(f"[{i}/{len(symbols)}] {symbol}")
            df = await _fetch_symbol_intraday(client, auth, symbol, trade_date)

            if df.empty:
                logger.warning(f"[{symbol}] No data.")
                continue

            df = validate_intraday(df, symbol=symbol)
            if not df.empty:
                write_parquet(df, dest)

            # Rate-limit between stocks
            await asyncio.sleep(_API_DELAY)

    logger.success(f"KOSPI200 intraday collection done → {dest}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Collect KOSPI200 1-min intraday data")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    trade_date = date.fromisoformat(args.date) if args.date else date.today()
    asyncio.run(collect_kospi200_intraday(trade_date))
