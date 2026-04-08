"""
미국 주요 지수 구성종목 symbol/name reference JSON 갱신 스크립트.

데이터 소스: FinanceDataReader
  - S&P 500  → fdr.StockListing('S&P500')  (503종목, Sector 포함)
  - NASDAQ 100 → fdr.StockListing('NASDAQ') 시총순 상위 100종목 (FDR 미직접지원)

Output:
  data/reference/sp500_symbols.json
  data/reference/nasdaq100_symbols.json

Schema (공통):
  {
    "updated_at":  "YYYY-MM-DD",
    "source":      "FinanceDataReader",
    "index":       "S&P500" | "NASDAQ100",
    "count":       503,
    "components":  [
      {"symbol": "AAPL", "name": "Apple Inc", "sector": "Technology"},
      ...
    ]
  }

스케줄: 매월 1일 08:00 (분기 리밸런싱 반영)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

REFERENCE_DIR = Path(__file__).parents[1] / "data" / "reference"
SP500_FILE    = REFERENCE_DIR / "sp500_symbols.json"
NDX100_FILE   = REFERENCE_DIR / "nasdaq100_symbols.json"

_NASDAQ100_TOP_N = 100  # FDR NASDAQ 리스팅이 시총 내림차순이므로 상위 N개 = NASDAQ100 근사


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_sp500() -> list[dict]:
    import FinanceDataReader as fdr

    logger.info("FinanceDataReader S&P500 조회 중...")
    df = fdr.StockListing("S&P500")
    if df.empty:
        raise RuntimeError("FDR에서 S&P500 빈 데이터 반환")

    return [
        {
            "symbol": str(row["Symbol"]).strip(),
            "name":   str(row["Name"]).strip(),
            "sector": str(row.get("Sector", "")).strip(),
        }
        for _, row in df.iterrows()
        if str(row["Symbol"]).strip()
    ]


def _fetch_nasdaq100() -> list[dict]:
    """NASDAQ 상장 종목 시총 상위 100개 (= NASDAQ 100 근사값)."""
    import FinanceDataReader as fdr

    logger.info("FinanceDataReader NASDAQ 전체 조회 중 (시총 상위 100 추출)...")
    df = fdr.StockListing("NASDAQ")
    if df.empty:
        raise RuntimeError("FDR에서 NASDAQ 빈 데이터 반환")

    df = df.head(_NASDAQ100_TOP_N)

    return [
        {
            "symbol": str(row["Symbol"]).strip(),
            "name":   str(row["Name"]).strip(),
            "sector": str(row.get("Industry", "")).strip(),
        }
        for _, row in df.iterrows()
        if str(row["Symbol"]).strip()
    ]


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def _write_json(path: Path, index_name: str, components: list[dict], target_date: date) -> None:
    payload = {
        "updated_at": str(target_date),
        "source": "FinanceDataReader",
        "index": index_name,
        "count": len(components),
        "components": components,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.success(f"{index_name} symbols JSON 갱신 완료 ({len(components)}종목) → {path}")


def sync_sp500_symbols(target_date: date | None = None) -> None:
    if target_date is None:
        target_date = date.today()
    components = _fetch_sp500()
    if not components:
        logger.warning("S&P500 데이터를 받지 못했습니다.")
        return
    _write_json(SP500_FILE, "S&P500", components, target_date)


def sync_nasdaq100_symbols(target_date: date | None = None) -> None:
    if target_date is None:
        target_date = date.today()
    components = _fetch_nasdaq100()
    if not components:
        logger.warning("NASDAQ100 데이터를 받지 못했습니다.")
        return
    _write_json(NDX100_FILE, "NASDAQ100", components, target_date)


def sync_all_us_symbols(target_date: date | None = None) -> None:
    """S&P500 + NASDAQ100 모두 갱신."""
    if target_date is None:
        target_date = date.today()
    sync_sp500_symbols(target_date)
    sync_nasdaq100_symbols(target_date)


# ---------------------------------------------------------------------------
# Load helpers (다른 모듈에서 import 용)
# ---------------------------------------------------------------------------

def load_sp500() -> dict:
    if not SP500_FILE.exists():
        return {}
    with SP500_FILE.open(encoding="utf-8") as f:
        return json.load(f)


def load_nasdaq100() -> dict:
    if not NDX100_FILE.exists():
        return {}
    with NDX100_FILE.open(encoding="utf-8") as f:
        return json.load(f)


def get_sp500_symbol_map() -> dict[str, str]:
    """symbol → name"""
    return {c["symbol"]: c["name"] for c in load_sp500().get("components", [])}


def get_nasdaq100_symbol_map() -> dict[str, str]:
    """symbol → name"""
    return {c["symbol"]: c["name"] for c in load_nasdaq100().get("components", [])}


# ---------------------------------------------------------------------------
# Prefect flow
# ---------------------------------------------------------------------------
try:
    from prefect import flow, task, get_run_logger

    @task(name="Sync S&P500 Symbols JSON", retries=2, retry_delay_seconds=60)
    def sync_sp500_task(target_date: date) -> None:
        logger = get_run_logger()
        logger.info(f"S&P500 symbols JSON 갱신 시작: {target_date}")
        sync_sp500_symbols(target_date)

    @task(name="Sync NASDAQ100 Symbols JSON", retries=2, retry_delay_seconds=60)
    def sync_nasdaq100_task(target_date: date) -> None:
        logger = get_run_logger()
        logger.info(f"NASDAQ100 symbols JSON 갱신 시작: {target_date}")
        sync_nasdaq100_symbols(target_date)

    @flow(
        name="US-Symbols-Sync-Flow",
        description="S&P500 + NASDAQ100 구성종목 symbol/name JSON 갱신 (매월 1일 08:00)",
    )
    def us_symbols_sync_flow(target_date: date | None = None) -> None:
        logger = get_run_logger()
        run_date = target_date or date.today()
        logger.info(f"US Symbols Sync Flow 시작: {run_date}")
        try:
            sync_sp500_task(run_date)
            sync_nasdaq100_task(run_date)
        except Exception as e:
            logger.error(f"US Symbols Sync 실패: {e}")
            raise

except ImportError:
    pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="미국 주요 지수 symbols JSON 갱신")
    parser.add_argument(
        "--index",
        choices=["sp500", "nasdaq100", "all"],
        default="all",
        help="갱신할 지수 (default: all)",
    )
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()

    if args.index == "sp500":
        sync_sp500_symbols(target)
    elif args.index == "nasdaq100":
        sync_nasdaq100_symbols(target)
    else:
        sync_all_us_symbols(target)
