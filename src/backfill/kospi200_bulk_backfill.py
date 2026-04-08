"""
KOSPI 200 전 종목 일괄 백필 스크립트

- 일봉: 5년치 (KIS API)
- 1분봉: 최근 30일 (Yahoo Finance 최대치)

실행:
    uv run src/kospi200_bulk_backfill.py
    uv run src/kospi200_bulk_backfill.py --years 5 --days 30 --skip-intraday
"""
import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import asyncio
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).parent))

from loguru import logger
from collectors.kr.kospi200_component_collector import fetch_kospi200_components
from runners.collect_daily_stock import collect_daily_data
from backfill.intraday_backfill_stock import backfill_stock_intraday
from bots.notifier import Notifier
from storage.parquet_writer import _PATHS
import pyarrow.parquet as pq


def _recent_weekdays(n: int = 5) -> list[date]:
    """가장 최근 n개의 평일(거래일 후보) 날짜 리스트를 반환합니다."""
    days = []
    d = date.today()
    while len(days) < n:
        if d.weekday() < 5:  # 월~금
            days.append(d)
        d -= timedelta(days=1)
    return days


def _load_components_from_cache() -> list[str] | None:
    """로컬에 저장된 최신 KOSPI200 구성종목 파일에서 심볼 리스트를 반환합니다."""
    base = _PATHS.get("kospi200_components")
    if not base or not base.exists():
        return None
    files = sorted(base.glob("*.parquet"), reverse=True)
    if not files:
        return None
    latest = files[0]
    logger.info(f"로컬 캐시에서 구성종목 로드: {latest.name}")
    table = pq.read_table(latest, columns=["symbol"])
    return table.column("symbol").to_pylist()


async def _get_kospi200_symbols() -> list[str]:
    """로컬 캐시 → KRX 최근 거래일 순서로 KOSPI200 구성종목을 가져옵니다."""
    # 1. 로컬 캐시 우선
    cached = _load_components_from_cache()
    if cached:
        logger.success(f"캐시에서 {len(cached)}개 종목 로드 완료")
        return cached

    # 2. KRX에서 최근 거래일 순차 시도
    for ref_date in _recent_weekdays(7):
        logger.info(f"KRX에서 구성종목 시도: {ref_date}")
        try:
            df = await fetch_kospi200_components(ref_date)
            if not df.empty:
                logger.success(f"KRX에서 {len(df)}개 종목 로드 완료 ({ref_date})")
                return df["symbol"].tolist()
        except Exception as e:
            logger.warning(f"  {ref_date} 실패: {e}")

    return []


async def bulk_backfill(years: int = 5, days: int = 30, skip_daily: bool = False, skip_intraday: bool = False):
    notifier = Notifier()

    # 1. KOSPI 200 구성종목 로드 (캐시 우선 → KRX fallback)
    symbols = await _get_kospi200_symbols()
    if not symbols:
        logger.error("KOSPI 200 구성종목을 가져올 수 없습니다. 중단합니다.")
        return

    total = len(symbols)
    logger.success(f"총 {total}개 종목 로드 완료")

    # 시작 알림
    await notifier.notify_all(
        f"⏳ *[KOSPI200 Bulk Backfill]* 전 종목({total}개) 데이터 일괄 수집을 시작합니다.\n"
        f"• 일봉: {years}년치\n"
        f"• 1분봉: 최근 {days}일치"
    )

    # 2. 일봉 백필 (전 종목 한 번에 배치 처리)
    daily_success = 0
    daily_failed = 0
    if not skip_daily:
        limit = years * 260  # 연간 거래일 약 260일
        logger.info(f"[1/2] 일봉 {years}년치 수집 시작 (limit={limit} per stock)...")
        batch_size = 20  # API 부하를 줄이기 위해 20종목씩 배치 처리
        for i in range(0, total, batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"  일봉 배치 {i+1}~{min(i+batch_size, total)}/{total} 처리 중...")
            try:
                await collect_daily_data(batch, limit=limit)
                daily_success += len(batch)
            except Exception as e:
                logger.error(f"  배치 처리 실패: {e}")
                daily_failed += len(batch)
            await asyncio.sleep(1.0)  # API 딜레이

        logger.success(f"일봉 수집 완료: 성공 {daily_success}개 / 실패 {daily_failed}개")

    # 3. 1분봉 백필 (종목별 순차 처리)
    intraday_success = 0
    intraday_skipped = 0
    intraday_failed = 0
    if not skip_intraday:
        logger.info(f"[2/2] 1분봉 최근 {days}일치 수집 시작...")
        for idx, symbol in enumerate(symbols, 1):
            logger.info(f"  1분봉 [{idx}/{total}] {symbol} 처리 중...")
            try:
                result = await backfill_stock_intraday(symbol, days=days)
                intraday_success += result.get("success", 0)
                intraday_skipped += result.get("skipped", 0)
            except Exception as e:
                logger.error(f"  {symbol} 1분봉 수집 실패: {e}")
                intraday_failed += 1
            # yfinance rate limit 방지용 딜레이
            await asyncio.sleep(1.5)

        logger.success(
            f"1분봉 수집 완료: 성공 {intraday_success}일분 / 스킵 {intraday_skipped}일분 / 실패 {intraday_failed}종목"
        )

    # 완료 알림
    msg = "✅ *[KOSPI200 Bulk Backfill]* 전 종목 일괄 수집이 완료되었습니다.\n"
    if not skip_daily:
        msg += f"• 일봉: 성공 {daily_success}종목 / 실패 {daily_failed}종목\n"
    if not skip_intraday:
        msg += f"• 1분봉: 성공 {intraday_success}일분 / 스킵 {intraday_skipped}일분 / 실패 {intraday_failed}종목"

    logger.info(msg.replace("\n", " "))
    await notifier.notify_all(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KOSPI 200 전 종목 일괄 백필")
    parser.add_argument("--years", type=int, default=5, help="일봉 수집 연수 (기본 5년)")
    parser.add_argument("--days", type=int, default=30, help="1분봉 수집 일수 (기본 30일, Yahoo 최대치)")
    parser.add_argument("--skip-daily", action="store_true", help="일봉 수집 건너뜀")
    parser.add_argument("--skip-intraday", action="store_true", help="1분봉 수집 건너뜀")
    args = parser.parse_args()

    asyncio.run(bulk_backfill(
        years=args.years,
        days=args.days,
        skip_daily=args.skip_daily,
        skip_intraday=args.skip_intraday
    ))
