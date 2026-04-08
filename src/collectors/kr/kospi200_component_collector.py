"""
KOSPI200 component list collector.

Source: KRX data portal (data.krx.co.kr)
Storage: market_data/metadata/kospi200_components/<YYYY-MM-DD>.parquet

Schema: date | symbol | name | index_name
"""
import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[2]))  # src/ 패키지 루트
del _sys, _Path


from __future__ import annotations

import asyncio
from datetime import date

import httpx
import pandas as pd
from loguru import logger

from storage.parquet_writer import daily_path, write_parquet

_KRX_OTP_URL = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
_KRX_DATA_URL = "http://data.krx.co.kr/comm/fileDn/download_csv.cmd"
_KRX_JSON_URL = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}


def _parse_components(raw: dict, target_date: date) -> pd.DataFrame:
    """Parse KRX JSON response into a tidy DataFrame."""
    rows = raw.get("output", [])
    if not rows:
        return pd.DataFrame()

    records = []
    for item in rows:
        # KRX field names vary — try common keys
        symbol = item.get("ISU_SRT_CD") or item.get("CMP_CD") or ""
        name = item.get("ISU_ABBRV") or item.get("CMP_KOR") or ""
        records.append({
            "date": target_date,
            "symbol": symbol.strip(),
            "name": name.strip(),
            "index_name": "KOSPI200",
        })

    return pd.DataFrame(records)


async def fetch_kospi200_components(target_date: date | None = None) -> pd.DataFrame:
    """
    Fetch KOSPI200 component list for *target_date* from KRX.

    Returns a DataFrame with columns: date, symbol, name, index_name.
    """
    if target_date is None:
        target_date = date.today()

    date_str = target_date.strftime("%Y%m%d")
    logger.info(f"Fetching KOSPI200 components for {target_date}")

    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT00601",
        "indIdx": "1",
        "indIdx2": "028",   # KOSPI200
        "trdDd": date_str,
        "money": "1",
        "csvxls_isNo": "false",
    }

    # KRX requires a valid session cookie obtained by visiting the referer page first
    referer_url = "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020506"

    async with httpx.AsyncClient(headers=_HEADERS, timeout=30, follow_redirects=True) as client:
        # Step 1: Establish session by visiting the referer page
        await client.get(referer_url)
        # Step 2: POST the actual data request with the session cookie
        resp = await client.post(_KRX_JSON_URL, data=payload)
        resp.raise_for_status()
        raw = resp.json()

    df = _parse_components(raw, target_date)
    if df.empty:
        logger.warning(f"No KOSPI200 components returned for {target_date}. "
                       "KRX may not have data for non-trading days.")
    else:
        logger.info(f"Found {len(df)} KOSPI200 components for {target_date}")
    return df


async def collect_kospi200_components(target_date: date | None = None) -> None:
    """Fetch and save KOSPI200 component list for *target_date*."""
    if target_date is None:
        target_date = date.today()

    df = await fetch_kospi200_components(target_date)
    if df.empty:
        return

    dest = daily_path("kospi200_components", str(target_date))
    write_parquet(df, dest)
    logger.success(f"KOSPI200 components saved → {dest}")


def load_components(target_date: date) -> list[str]:
    """Load KOSPI200 symbol list for *target_date* from local parquet."""
    import pyarrow.parquet as pq

    path = daily_path("kospi200_components", str(target_date))
    if not path.exists():
        raise FileNotFoundError(
            f"Component file not found: {path}\n"
            f"Run: python kospi200_component_collector.py --date {target_date}"
        )
    table = pq.read_table(path, columns=["symbol"])
    return table.column("symbol").to_pylist()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Collect KOSPI200 component list")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()
    asyncio.run(collect_kospi200_components(target))
