"""
Multi-market bulk backfill script.

data/reference/ 의 JSON reference 파일을 input으로 받아
전 종목 일봉(최대치) + 1분봉(최대 28 calendar days)을 수집한다.

== yfinance 제한 (2026년 4월 기준, 실측) ==
  일봉  : period='max'  → KR ~2000년, US ~1980년부터
  1분봉 : 요청당 최대 7 calendar days, 총 lookback ~28 calendar days
          (8일 이상 단건 요청 → 빈 데이터 반환)

== 지원 마켓 ==
  kospi200  → data/reference/kospi200_symbols.json  (.KS suffix 추가)
  sp500     → data/reference/sp500_symbols.json
  nasdaq100 → data/reference/nasdaq100_symbols.json

== 저장 경로 ==
  일봉  KR : market_data/kr/kospi200/daily/<YYYY-MM-DD>.parquet   (key: kospi200_daily_yf)
  일봉  US : market_data/us/stock/daily/<YYYY-MM-DD>.parquet       (key: us_stock_daily)
  1분봉 KR : market_data/kr/kospi200/1min/<YYYY-MM-DD>.parquet    (key: kospi200_1min)
  1분봉 US : market_data/us/stock/1min/<YYYY-MM-DD>.parquet        (key: us_stock_1min)

Usage:
  uv run python src/market_bulk_backfill.py --market kospi200
  uv run python src/market_bulk_backfill.py --market sp500
  uv run python src/market_bulk_backfill.py --market nasdaq100
  uv run python src/market_bulk_backfill.py --market all
  uv run python src/market_bulk_backfill.py --market kospi200 --skip-daily
  uv run python src/market_bulk_backfill.py --market kospi200 --skip-intraday
  uv run python src/market_bulk_backfill.py --market sp500 --batch-size 30
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd
import yfinance as yf
from loguru import logger

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from parquet_writer import daily_path, write_parquet, get_dir
from data_validation import validate_intraday

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

REFERENCE_DIR = Path(__file__).parents[1] / "data" / "reference"

_INTRADAY_CHUNK_DAYS = 7      # 요청당 최대 7 calendar days (yfinance 제한)
_INTRADAY_LOOKBACK   = 28     # 총 lookback: 28 calendar days (~20 거래일)
_DAILY_BATCH_SIZE    = 50     # 일봉 배치당 종목 수
_INTRADAY_BATCH_SIZE = 20     # 1분봉 배치당 종목 수 (메모리 절약)

MarketKey = Literal["kospi200", "sp500", "nasdaq100"]

_MARKET_CONFIG: dict[str, dict] = {
    "kospi200": {
        "json_file":       REFERENCE_DIR / "kospi200_symbols.json",
        "yf_suffix":       ".KS",
        "daily_path_key":  "kospi200_daily_yf",
        "intraday_path_key": "kospi200_1min",
        "region":          "KR",
    },
    "sp500": {
        "json_file":       REFERENCE_DIR / "sp500_symbols.json",
        "yf_suffix":       "",
        "daily_path_key":  "us_stock_daily",
        "intraday_path_key": "us_stock_1min",
        "region":          "US",
    },
    "nasdaq100": {
        "json_file":       REFERENCE_DIR / "nasdaq100_symbols.json",
        "yf_suffix":       "",
        "daily_path_key":  "us_stock_daily",
        "intraday_path_key": "us_stock_1min",
        "region":          "US",
    },
}


# ---------------------------------------------------------------------------
# Symbol loader
# ---------------------------------------------------------------------------

def load_symbols(market: MarketKey) -> list[dict]:
    """JSON reference 파일에서 symbol/name 목록을 반환."""
    cfg = _MARKET_CONFIG[market]
    path: Path = cfg["json_file"]
    if not path.exists():
        raise FileNotFoundError(
            f"{market} reference 파일 없음: {path}\n"
            f"먼저 실행: uv run python src/{'kospi200' if market == 'kospi200' else 'us'}_symbols_sync.py"
        )
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    components = data.get("components", [])
    logger.info(f"[{market}] {len(components)}종목 로드 (updated_at={data.get('updated_at')})")
    return components


def _to_yf_symbol(code: str, suffix: str) -> str:
    if suffix and not code.endswith(suffix):
        return code + suffix
    return code


# ---------------------------------------------------------------------------
# Daily backfill (yfinance period='max')
# ---------------------------------------------------------------------------

def _download_daily_max(yf_symbols: list[str]) -> pd.DataFrame:
    """yfinance에서 period='max' 일봉 다운로드 → tidy DataFrame."""
    raw = yf.download(
        tickers=yf_symbols,
        period="max",
        interval="1d",
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )
    if raw.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    if len(yf_symbols) == 1:
        sub = raw.reset_index().rename(columns={
            "Date": "timestamp", "Open": "open", "High": "high",
            "Low": "low", "Close": "close", "Volume": "volume",
        })
        sub["symbol"] = yf_symbols[0]
        sub["trade_value"] = sub["close"] * sub["volume"]
        frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_value"]])
    else:
        for sym in yf_symbols:
            try:
                sub = raw[sym].copy()
            except KeyError:
                logger.warning(f"  {sym}: 일봉 데이터 없음")
                continue
            if sub.empty:
                continue
            sub = sub.reset_index().rename(columns={
                "Date": "timestamp", "Open": "open", "High": "high",
                "Low": "low", "Close": "close", "Volume": "volume",
            })
            sub["symbol"] = sym
            sub["trade_value"] = sub["close"] * sub["volume"]
            frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_value"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True).dropna(subset=["open", "close"])
    return combined


async def backfill_daily(
    market: MarketKey,
    symbols: list[dict],
    batch_size: int = _DAILY_BATCH_SIZE,
) -> dict:
    cfg = _MARKET_CONFIG[market]
    suffix = cfg["yf_suffix"]
    path_key = cfg["daily_path_key"]

    yf_symbols = [_to_yf_symbol(s["symbol"], suffix) for s in symbols]
    total = len(yf_symbols)
    success_rows = 0
    failed_batches = 0

    logger.info(f"[{market}] 일봉 backfill 시작: {total}종목 (배치={batch_size})")

    for i in range(0, total, batch_size):
        batch = yf_symbols[i : i + batch_size]
        batch_label = f"{i+1}~{min(i+batch_size, total)}/{total}"
        logger.info(f"  일봉 배치 {batch_label} ({len(batch)}종목) 다운로드 중...")

        try:
            df = await asyncio.to_thread(_download_daily_max, batch)
        except Exception as e:
            logger.error(f"  배치 {batch_label} 다운로드 실패: {e}")
            failed_batches += 1
            await asyncio.sleep(2.0)
            continue

        if df.empty:
            logger.warning(f"  배치 {batch_label}: 빈 데이터")
            continue

        # 날짜별로 분할 저장
        df["_date"] = pd.to_datetime(df["timestamp"]).dt.date
        for day, group in df.groupby("_date"):
            dest = daily_path(path_key, str(day))
            write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

        success_rows += len(df)
        logger.success(f"  배치 {batch_label}: {len(df)}행 저장")
        await asyncio.sleep(1.0)

    return {"success_rows": success_rows, "failed_batches": failed_batches}


# ---------------------------------------------------------------------------
# Intraday (1-min) backfill — 7-day chunk × 4 = 28 calendar days
# ---------------------------------------------------------------------------

def _download_1min_chunk(yf_symbols: list[str], start: date, end: date) -> pd.DataFrame:
    """단일 청크(≤7 calendar days) 1분봉 다운로드."""
    raw = yf.download(
        tickers=yf_symbols,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        interval="1m",
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )
    if raw.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    if len(yf_symbols) == 1:
        sub = raw.reset_index().rename(columns={
            "Datetime": "timestamp", "Date": "timestamp",
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        sub["symbol"] = yf_symbols[0]
        frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume"]])
    else:
        for sym in yf_symbols:
            try:
                if sym not in raw.columns.get_level_values(0):
                    continue
                sub = raw[sym].copy()
            except (KeyError, AttributeError):
                continue
            if sub.empty:
                continue
            sub = sub.reset_index().rename(columns={
                "Datetime": "timestamp", "Date": "timestamp",
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume",
            })
            sub["symbol"] = sym
            frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    # tz-naive UTC 정규화
    if "timestamp" in combined.columns and combined["timestamp"].dt.tz is not None:
        combined["timestamp"] = combined["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    return combined.dropna(subset=["open", "close"])


async def backfill_intraday(
    market: MarketKey,
    symbols: list[dict],
    lookback_days: int = _INTRADAY_LOOKBACK,
    chunk_days: int = _INTRADAY_CHUNK_DAYS,
    batch_size: int = _INTRADAY_BATCH_SIZE,
) -> dict:
    cfg = _MARKET_CONFIG[market]
    suffix = cfg["yf_suffix"]
    path_key = cfg["intraday_path_key"]

    yf_symbols = [_to_yf_symbol(s["symbol"], suffix) for s in symbols]
    total = len(yf_symbols)
    today = date.today()

    # 7일 청크 목록 생성 (최신 → 과거 순)
    chunks: list[tuple[date, date]] = []
    offset = 0
    while offset < lookback_days:
        chunk_end   = today - timedelta(days=offset)
        chunk_start = max(chunk_end - timedelta(days=chunk_days), today - timedelta(days=lookback_days))
        chunks.append((chunk_start, chunk_end))
        offset += chunk_days

    logger.info(
        f"[{market}] 1분봉 backfill 시작: {total}종목 × {len(chunks)}청크 "
        f"(lookback={lookback_days}일, chunk={chunk_days}일)"
    )

    total_rows = 0
    failed_chunks = 0

    for chunk_start, chunk_end in chunks:
        logger.info(f"  청크 {chunk_start} ~ {chunk_end}")
        chunk_rows = 0

        for i in range(0, total, batch_size):
            batch = yf_symbols[i : i + batch_size]
            batch_label = f"{i+1}~{min(i+batch_size, total)}/{total}"

            try:
                df = await asyncio.to_thread(_download_1min_chunk, batch, chunk_start, chunk_end)
            except Exception as e:
                logger.error(f"    배치 {batch_label} 실패: {e}")
                failed_chunks += 1
                await asyncio.sleep(2.0)
                continue

            if df.empty:
                logger.debug(f"    배치 {batch_label}: 빈 데이터")
                continue

            df = validate_intraday(df)
            if df.empty:
                continue

            # 날짜별 분할 저장
            df["_date"] = df["timestamp"].dt.date if hasattr(df["timestamp"].dt, "date") else pd.to_datetime(df["timestamp"]).dt.date
            for day, group in df.groupby("_date"):
                dest = daily_path(path_key, str(day))
                write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

            chunk_rows += len(df)
            await asyncio.sleep(0.5)

        logger.success(f"  청크 {chunk_start}~{chunk_end}: {chunk_rows}행 저장")
        total_rows += chunk_rows
        await asyncio.sleep(1.0)

    return {"total_rows": total_rows, "failed_chunks": failed_chunks}


# ---------------------------------------------------------------------------
# 단일 마켓 backfill
# ---------------------------------------------------------------------------

async def backfill_market(
    market: MarketKey,
    skip_daily: bool = False,
    skip_intraday: bool = False,
    daily_batch_size: int = _DAILY_BATCH_SIZE,
    intraday_batch_size: int = _INTRADAY_BATCH_SIZE,
) -> None:
    symbols = load_symbols(market)
    if not symbols:
        logger.error(f"[{market}] 종목 없음. 종료.")
        return

    logger.info(f"{'='*60}")
    logger.info(f"[{market}] Bulk Backfill 시작: {len(symbols)}종목")
    logger.info(f"{'='*60}")

    results = {}

    if not skip_daily:
        logger.info(f"[{market}] [1/2] 일봉 backfill (period=max)")
        results["daily"] = await backfill_daily(market, symbols, daily_batch_size)
        logger.success(
            f"[{market}] 일봉 완료: {results['daily']['success_rows']}행, "
            f"실패배치={results['daily']['failed_batches']}"
        )

    if not skip_intraday:
        logger.info(f"[{market}] [2/2] 1분봉 backfill (최근 {_INTRADAY_LOOKBACK}일, {_INTRADAY_CHUNK_DAYS}일 청크)")
        results["intraday"] = await backfill_intraday(market, symbols, batch_size=intraday_batch_size)
        logger.success(
            f"[{market}] 1분봉 완료: {results['intraday']['total_rows']}행, "
            f"실패청크={results['intraday']['failed_chunks']}"
        )

    logger.info(f"[{market}] Bulk Backfill 완료")
    return results


# ---------------------------------------------------------------------------
# Prefect flow
# ---------------------------------------------------------------------------
try:
    from prefect import flow, task, get_run_logger
    from notifier import Notifier

    @task(name="Market Daily Backfill", retries=1, retry_delay_seconds=60)
    async def daily_backfill_task(market: str, batch_size: int) -> dict:
        logger = get_run_logger()
        symbols = load_symbols(market)
        logger.info(f"[{market}] 일봉 backfill: {len(symbols)}종목")
        return await backfill_daily(market, symbols, batch_size)

    @task(name="Market Intraday Backfill", retries=1, retry_delay_seconds=60)
    async def intraday_backfill_task(market: str, batch_size: int) -> dict:
        logger = get_run_logger()
        symbols = load_symbols(market)
        logger.info(f"[{market}] 1분봉 backfill: {len(symbols)}종목")
        return await backfill_intraday(market, symbols, batch_size=batch_size)

    @flow(name="Market-Bulk-Backfill-Flow")
    async def market_bulk_backfill_flow(
        market: str = "kospi200",
        skip_daily: bool = False,
        skip_intraday: bool = False,
        daily_batch_size: int = _DAILY_BATCH_SIZE,
        intraday_batch_size: int = _INTRADAY_BATCH_SIZE,
    ) -> None:
        logger = get_run_logger()
        notifier = Notifier()
        markets = list(_MARKET_CONFIG.keys()) if market == "all" else [market]

        for mkt in markets:
            await notifier.notify_all(
                f"⏳ *[Bulk Backfill]* `{mkt}` backfill 시작\n"
                f"• 일봉: {'skip' if skip_daily else 'period=max'}\n"
                f"• 1분봉: {'skip' if skip_intraday else f'최근 {_INTRADAY_LOOKBACK}일'}"
            )
            try:
                if not skip_daily:
                    daily_result = await daily_backfill_task(mkt, daily_batch_size)
                if not skip_intraday:
                    intraday_result = await intraday_backfill_task(mkt, intraday_batch_size)

                msg = f"✅ *[Bulk Backfill]* `{mkt}` 완료\n"
                if not skip_daily:
                    msg += f"• 일봉: {daily_result['success_rows']}행\n"
                if not skip_intraday:
                    msg += f"• 1분봉: {intraday_result['total_rows']}행"
                await notifier.notify_all(msg)
                logger.info(msg.replace("\n", " "))
            except Exception as e:
                err = f"❌ *[Bulk Backfill]* `{mkt}` 오류: {e}"
                logger.error(err)
                await notifier.notify_all(err)
                raise

except ImportError:
    pass


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Multi-market bulk backfill (일봉 max + 1분봉 최근 28일)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  # KOSPI200 전 종목 일봉+1분봉 backfill
  uv run python src/market_bulk_backfill.py --market kospi200

  # S&P500 일봉만
  uv run python src/market_bulk_backfill.py --market sp500 --skip-intraday

  # 전 마켓 1분봉만
  uv run python src/market_bulk_backfill.py --market all --skip-daily

  # 배치 크기 조정 (메모리/API 부하 조절)
  uv run python src/market_bulk_backfill.py --market sp500 --batch-size 20
        """,
    )
    parser.add_argument(
        "--market",
        choices=["kospi200", "sp500", "nasdaq100", "all"],
        default="kospi200",
        help="수집할 마켓 (default: kospi200)",
    )
    parser.add_argument("--skip-daily",    action="store_true", help="일봉 수집 건너뜀")
    parser.add_argument("--skip-intraday", action="store_true", help="1분봉 수집 건너뜀")
    parser.add_argument(
        "--batch-size", type=int, default=_DAILY_BATCH_SIZE,
        help=f"배치 크기 (일봉 default={_DAILY_BATCH_SIZE}, 1분봉은 절반 사용)",
    )
    args = parser.parse_args()

    markets = list(_MARKET_CONFIG.keys()) if args.market == "all" else [args.market]

    async def _run():
        for mkt in markets:
            await backfill_market(
                market=mkt,
                skip_daily=args.skip_daily,
                skip_intraday=args.skip_intraday,
                daily_batch_size=args.batch_size,
                intraday_batch_size=max(args.batch_size // 2, 5),
            )

    asyncio.run(_run())
