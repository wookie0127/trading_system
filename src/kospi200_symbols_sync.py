"""
KOSPI200 symbol/name reference JSON 갱신 스크립트.

데이터 소스: FinanceDataReader (KRX 전체 KOSPI 목록 → 시총 상위 200 필터링)
  - KRX data.krx.co.kr는 브라우저 JS 세션이 필요해서 headless 환경에서 차단됨.
  - FDR은 이 문제 없이 KRX 데이터에 접근 가능.
  - 공식 KOSPI200 편입/편출 기준(유동성, 관리 종목 제외 등)과 약간 다를 수 있으나
    실무 트레이딩 목적에서는 시총 상위 200종목과 거의 동일.

Output: data/reference/kospi200_symbols.json
Schema:
  {
    "updated_at": "YYYY-MM-DD",
    "source": "FinanceDataReader/KRX",
    "count": 200,
    "components": [
      {"symbol": "005930", "name": "삼성전자"},
      ...
    ]
  }

스케줄: 분기 리밸런싱 반영을 위해 매월 1일 08:00 실행 권장.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date
from pathlib import Path

from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

REFERENCE_FILE = Path(__file__).parents[1] / "data" / "reference" / "kospi200_symbols.json"
TOP_N = 200  # 시총 상위 N종목


def _fetch_via_fdr() -> list[dict]:
    """FinanceDataReader로 KOSPI 전체 목록을 받아 시총 상위 TOP_N개 반환."""
    import FinanceDataReader as fdr

    logger.info("FinanceDataReader로 KOSPI 전체 목록 조회 중...")
    df = fdr.StockListing("KOSPI")
    if df.empty:
        raise RuntimeError("FDR에서 빈 데이터 반환")

    # 관리종목/ETF/ETN 등 제외: 보통주(MarketId=STK)만
    df = df[df["MarketId"] == "STK"].copy()

    # 시총 기준 상위 TOP_N
    df = df.sort_values("Marcap", ascending=False).head(TOP_N)

    return [
        {"symbol": row["Code"], "name": row["Name"]}
        for _, row in df.iterrows()
    ]


def load_symbols() -> dict:
    """저장된 JSON 로드. 파일 없으면 빈 dict 반환."""
    if not REFERENCE_FILE.exists():
        return {}
    with REFERENCE_FILE.open(encoding="utf-8") as f:
        return json.load(f)


def get_symbol_map() -> dict[str, str]:
    """symbol → name 딕셔너리 반환. (빠른 조회용)"""
    data = load_symbols()
    return {c["symbol"]: c["name"] for c in data.get("components", [])}


def get_symbol_list() -> list[str]:
    """symbol 리스트만 반환."""
    data = load_symbols()
    return [c["symbol"] for c in data.get("components", [])]


def sync_kospi200_symbols(target_date: date | None = None) -> None:
    """KOSPI200 구성종목을 fetch하여 JSON 파일을 갱신한다."""
    if target_date is None:
        target_date = date.today()

    components = _fetch_via_fdr()
    if not components:
        logger.warning("데이터를 받지 못했습니다. JSON을 갱신하지 않습니다.")
        return

    payload = {
        "updated_at": str(target_date),
        "source": "FinanceDataReader/KRX",
        "count": len(components),
        "components": components,
    }

    REFERENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with REFERENCE_FILE.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logger.success(
        f"KOSPI200 symbols JSON 갱신 완료 ({len(components)}종목) → {REFERENCE_FILE}"
    )


# ---------------------------------------------------------------------------
# Prefect flow
# ---------------------------------------------------------------------------
try:
    from prefect import flow, task, get_run_logger

    @task(name="Sync KOSPI200 Symbols JSON", retries=2, retry_delay_seconds=60)
    def sync_symbols_task(target_date: date) -> None:
        logger = get_run_logger()
        logger.info(f"KOSPI200 symbols JSON 갱신 시작: {target_date}")
        sync_kospi200_symbols(target_date)

    @flow(
        name="KOSPI200-Symbols-Sync-Flow",
        description="KOSPI200 구성종목 symbol/name JSON 갱신 (매월 1일 08:00)",
    )
    def kospi200_symbols_sync_flow(target_date: date | None = None) -> None:
        logger = get_run_logger()
        run_date = target_date or date.today()
        logger.info(f"KOSPI200 Symbols Sync Flow 시작: {run_date}")
        try:
            sync_symbols_task(run_date)
        except Exception as e:
            logger.error(f"KOSPI200 Symbols Sync 실패: {e}")
            raise

except ImportError:
    pass  # Prefect 없는 환경에서도 standalone 실행 가능


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KOSPI200 symbols JSON 갱신")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    target = date.fromisoformat(args.date) if args.date else date.today()
    sync_kospi200_symbols(target)
