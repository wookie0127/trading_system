"""
Daily investor trading flow collector via KIS OpenAPI.

Investor groups: foreign / institutional / individual

KIS API:
  endpoint : uapi/domestic-stock/v1/quotations/inquire-investor
  tr_id    : FHKST01010900

Storage: market_data/kr/investor_flow/daily/<YYYY-MM-DD>.parquet

Schema (§4.2):
  date, symbol,
  foreign_buy, foreign_sell, foreign_net,
  institution_buy, institution_sell,
  individual_buy, individual_sell,
  volume, trade_value
"""

from __future__ import annotations

import asyncio
from datetime import date

import httpx
import pandas as pd
from loguru import logger

from kis_auth_handler import KISAuthHandler
from kis_config import API_ROOT
from kospi200_component_collector import load_components
from data_validation import validate_investor_flow
from parquet_writer import daily_path, write_parquet

_ENDPOINT = "uapi/domestic-stock/v1/quotations/inquire-investor"
_TR_ID = "FHKST01010900"
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


def _parse_output(payload: dict, symbol: str, trade_date: date) -> dict | None:
    """Extract the latest single-day investor flow row from API response."""
    # output1 is the summary for the queried date
    output = payload.get("output1")
    if not output:
        return None

    # output1 may be a list; take the first item matching the queried date
    if isinstance(output, list):
        if not output:
            return None
        row = output[0]
    else:
        row = output

    def _f(key: str) -> float:
        val = row.get(key, "0") or "0"
        try:
            return float(val.replace(",", ""))
        except (ValueError, AttributeError):
            return 0.0

    return {
        "date":             trade_date,
        "symbol":           symbol,
        # foreign
        "foreign_buy":      _f("frgn_buy_vol"),
        "foreign_sell":     _f("frgn_sel_vol"),
        "foreign_net":      _f("frgn_ntby_qty"),
        # institution
        "institution_buy":  _f("orgn_buy_vol"),
        "institution_sell": _f("orgn_sel_vol"),
        # individual
        "individual_buy":   _f("prsn_buy_vol"),
        "individual_sell":  _f("prsn_sel_vol"),
        # totals
        "volume":           _f("acml_vol"),
        "trade_value":      _f("acml_tr_pbmn"),
    }


async def _fetch_one(
    client: httpx.AsyncClient,
    auth: KISAuthHandler,
    symbol: str,
    trade_date: date,
) -> dict | None:
    headers = _build_headers(auth)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD":         symbol,
        "FID_INPUT_DATE_1":       trade_date.strftime("%Y%m%d"),
    }

    try:
        resp = await client.get(
            f"{API_ROOT}/{_ENDPOINT}", headers=headers, params=params
        )
        resp.raise_for_status()
        return _parse_output(resp.json(), symbol, trade_date)
    except Exception as exc:
        logger.warning(f"[{symbol}] investor flow API error: {exc}")
        return None


async def collect_investor_flow(trade_date: date | None = None) -> None:
    """
    Collect daily investor flow for all KOSPI200 components on *trade_date*.

    Requires the component list to already be saved locally.
    Run kospi200_component_collector.py first.
    """
    if trade_date is None:
        trade_date = date.today()

    symbols = load_components(trade_date)
    logger.info(f"Collecting investor flow for {trade_date}: {len(symbols)} stocks")

    auth = KISAuthHandler()
    records: list[dict] = []

    async with httpx.AsyncClient(timeout=30) as client:
        for i, symbol in enumerate(symbols, 1):
            logger.debug(f"[{i}/{len(symbols)}] {symbol}")
            record = await _fetch_one(client, auth, symbol, trade_date)
            if record:
                records.append(record)
            await asyncio.sleep(_API_DELAY)

    if not records:
        logger.warning(f"No investor flow data collected for {trade_date}")
        return

    df = pd.DataFrame(records)
    df = validate_investor_flow(df)

    if not df.empty:
        dest = daily_path("investor_flow_daily", str(trade_date))
        write_parquet(df, dest)
        logger.success(f"Investor flow saved ({len(df)} stocks) → {dest}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Collect daily investor flow data")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    trade_date = date.fromisoformat(args.date) if args.date else date.today()
    asyncio.run(collect_investor_flow(trade_date))
