"""
Daily data collection pipeline runner.

Workflow (per spec §7):
  Step 1: NASDAQ 1-min intraday
  Step 2: KOSPI200 component list
  Step 3: KOSPI200 1-min intraday
  Step 4: Investor flow (daily)

Usage:
  python collect_daily.py                    # today
  python collect_daily.py --date 2025-01-15  # specific date
  python collect_daily.py --step nasdaq      # single step
"""
import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path


from __future__ import annotations

import asyncio
from datetime import date

from loguru import logger

from collectors.us.nasdaq_collector import collect_nasdaq
from collectors.kr.kospi200_component_collector import collect_kospi200_components
from collectors.kr.kospi200_intraday_collector import collect_kospi200_intraday
from collectors.kr.investor_flow_collector import collect_investor_flow


async def run_all(trade_date: date) -> None:
    logger.info(f"=== Daily collection pipeline: {trade_date} ===")

    # Step 1: NASDAQ (no KIS dependency)
    logger.info("Step 1/4 — NASDAQ 1-min")
    await collect_nasdaq(start=trade_date, end=trade_date)

    # Step 2: KOSPI200 components (must run before steps 3 & 4)
    logger.info("Step 2/4 — KOSPI200 components")
    await collect_kospi200_components(trade_date)

    # Steps 3 & 4 both depend on component list
    logger.info("Step 3/4 — KOSPI200 intraday")
    await collect_kospi200_intraday(trade_date)

    logger.info("Step 4/4 — Investor flow")
    await collect_investor_flow(trade_date)

    logger.success("=== Pipeline complete ===")


if __name__ == "__main__":
    import argparse

    _STEPS = {
        "nasdaq":     lambda d: collect_nasdaq(start=d, end=d),
        "components": collect_kospi200_components,
        "intraday":   collect_kospi200_intraday,
        "flow":       collect_investor_flow,
    }

    parser = argparse.ArgumentParser(description="Run daily market data collection")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    parser.add_argument(
        "--step",
        choices=list(_STEPS),
        default=None,
        help="Run a single step instead of the full pipeline",
    )
    args = parser.parse_args()

    trade_date = date.fromisoformat(args.date) if args.date else date.today()

    if args.step:
        asyncio.run(_STEPS[args.step](trade_date))
    else:
        asyncio.run(run_all(trade_date))
