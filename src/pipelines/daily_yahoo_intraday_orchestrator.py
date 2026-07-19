import sys as _sys
from pathlib import Path as _Path

_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import asyncio
from datetime import date, timedelta

from prefect import flow, get_run_logger, task

from bots.notifier import Notifier
from collectors.kr.kospi200_component_collector import collect_kospi200_components
from collectors.us.yahoo_finance_collector import (
    GLOBAL_INTRADAY_SYMBOLS,
    collect_global_intraday,
    collect_kospi200_intraday,
    collect_us_stock_intraday,
)
from sync.kospi200_symbols_sync import get_symbol_list, sync_kospi200_symbols

DEFAULT_US_SYMBOLS = [
    "SPY",
    "QQQ",
    "IWM",
    "XLK",
    "XLF",
    "TLT",
    "GLD",
    "IBIT",
    "NQ=F",
    "ES=F",
]
DEFAULT_GLOBAL_INTRADAY_SYMBOLS = list(GLOBAL_INTRADAY_SYMBOLS)


def _previous_weekday(base_date: date) -> date:
    candidate = base_date - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


@task(name="Fetch KOSPI 200 Components (Yahoo Flow)", retries=2, retry_delay_seconds=30)
async def fetch_components_task(target_date: date) -> None:
    logger = get_run_logger()
    logger.info(f"Fetching KOSPI 200 components for {target_date}")
    await collect_kospi200_components(target_date)


@task(name="Collect KOSPI 200 Intraday (Yahoo)", retries=2, retry_delay_seconds=60)
async def collect_kospi200_intraday_task(
    target_date: date, codes: list[str] | None = None
) -> None:
    logger = get_run_logger()
    logger.info(
        f"Collecting KOSPI 200 1-min data via yfinance for {target_date} "
        f"(codes={'provided' if codes else 'component_file'})"
    )
    await collect_kospi200_intraday(trade_date=target_date, codes=codes)


@task(name="Collect US Symbol Intraday (Yahoo)", retries=2, retry_delay_seconds=60)
async def collect_us_intraday_task(target_date: date, symbols: list[str]) -> None:
    logger = get_run_logger()
    logger.info(f"Collecting US 1-min data via yfinance for {target_date}: {symbols}")
    await collect_us_stock_intraday(trade_date=target_date, symbols=symbols)


@task(name="Collect Global/Crypto Intraday (Yahoo)", retries=2, retry_delay_seconds=60)
async def collect_global_intraday_task(target_date: date, symbols: list[str]) -> None:
    logger = get_run_logger()
    logger.info(
        f"Collecting global/crypto 1-min data via yfinance for {target_date}: {symbols}"
    )
    await collect_global_intraday(trade_date=target_date, symbols=symbols)


@flow(name="Daily-Yahoo-Intraday-Flow")
async def daily_yahoo_intraday_flow(
    kr_target_date: date | None = None,
    us_target_date: date | None = None,
    us_symbols: list[str] | None = None,
    global_target_date: date | None = None,
    global_symbols: list[str] | None = None,
):
    """
    하루 1회 실행되는 yfinance 1분봉 수집 플로우.

    - KOSPI200: 직전 한국 거래일 기준
    - US ETF/종목: 직전 미국 거래일 기준

    운영에서는 한국시간 새벽, 미국장 종료 이후에 스케줄링하는 것을 전제로 한다.
    """
    logger = get_run_logger()

    today = date.today()
    resolved_kr_date = kr_target_date or _previous_weekday(today)
    resolved_us_date = us_target_date or _previous_weekday(today)
    resolved_global_date = global_target_date or today
    resolved_us_symbols = us_symbols or DEFAULT_US_SYMBOLS
    resolved_global_symbols = global_symbols or DEFAULT_GLOBAL_INTRADAY_SYMBOLS

    logger.info(
        "Starting Daily Yahoo Intraday Flow "
        f"(kr_target_date={resolved_kr_date}, us_target_date={resolved_us_date}, "
        f"global_target_date={resolved_global_date}, us_symbols={resolved_us_symbols}, "
        f"global_symbols={resolved_global_symbols})"
    )

    notifier = Notifier()
    current_step = "initializing"
    component_source = "krx"
    kospi200_codes: list[str] | None = None
    await notifier.notify_all(
        "🚀 *[Market Data]* "
        f"Yahoo 1분봉 수집 시작: KOSPI200={resolved_kr_date}, "
        f"US={resolved_us_date} ({', '.join(resolved_us_symbols)}), "
        f"Global/Crypto={resolved_global_date} ({', '.join(resolved_global_symbols)})"
    )

    try:
        current_step = "fetch_kospi200_components"
        try:
            await fetch_components_task(resolved_kr_date)
        except Exception as exc:
            component_source = "fdr_json_fallback"
            logger.warning(
                "KRX component fetch failed; falling back to FinanceDataReader JSON sync. "
                f"error={type(exc).__name__}: {exc}"
            )
            sync_kospi200_symbols(resolved_kr_date)
            kospi200_codes = get_symbol_list()
            if not kospi200_codes:
                raise RuntimeError("KOSPI200 fallback symbol list is empty") from exc

        current_step = "collect_kospi200_intraday"
        await collect_kospi200_intraday_task(resolved_kr_date, kospi200_codes)

        current_step = "collect_us_intraday"
        await collect_us_intraday_task(resolved_us_date, resolved_us_symbols)

        current_step = "collect_global_intraday"
        await collect_global_intraday_task(
            resolved_global_date, resolved_global_symbols
        )

        msg = (
            "✅ *[Market Data]* Yahoo 1분봉 수집 완료: "
            f"KOSPI200={resolved_kr_date}, "
            f"US={resolved_us_date} ({', '.join(resolved_us_symbols)}), "
            f"Global/Crypto={resolved_global_date} ({', '.join(resolved_global_symbols)})\n"
            f"• kospi200_component_source: {component_source}"
        )
        logger.info(msg)
        await notifier.notify_all(msg)
    except Exception as exc:
        error_msg = (
            "❌ *[Market Data]* Yahoo 1분봉 수집 실패: "
            f"KOSPI200={resolved_kr_date}, "
            f"US={resolved_us_date} ({', '.join(resolved_us_symbols)}), "
            f"Global/Crypto={resolved_global_date} ({', '.join(resolved_global_symbols)})\n"
            f"• step: {current_step}\n"
            f"• kospi200_component_source: {component_source}\n"
            f"• error_type: {type(exc).__name__}\n"
            f"• error: {exc}\n"
            "• note: yfinance 1분봉은 최근 약 30일, 요청당 최대 7일 제약이 있습니다."
        )
        logger.error(error_msg)
        await notifier.notify_all(error_msg)
        raise


if __name__ == "__main__":
    asyncio.run(daily_yahoo_intraday_flow())
