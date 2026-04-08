import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import asyncio
import os
import argparse
from prefect import flow, task, get_run_logger

# 프로젝트 루트(src)를 path에 추가하여 절대 임포트 가능하게 함
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from runners.collect_daily_stock import collect_daily_data
from backfill.intraday_backfill_stock import backfill_stock_intraday
from bots.notifier import Notifier

@task(name="Backfill Daily Data", retries=1, retry_delay_seconds=30)
async def backfill_daily_task(symbol: str, years: int):
    logger = get_run_logger()
    # 1년 약 252 거래일 예상, 여유 있게 260일 계산
    limit = years * 260
    logger.info(f"Starting {years}-year daily data backfill for {symbol} (limit={limit})")
    await collect_daily_data([symbol], limit=limit)
    return True

@task(name="Backfill Intraday Data", retries=1, retry_delay_seconds=30)
async def backfill_intraday_task(symbol: str, days: int):
    logger = get_run_logger()
    logger.info(f"Starting {days}-day 1-min data backfill for {symbol}")
    results = await backfill_stock_intraday(symbol, days=days)
    return results

@flow(name="Stock-Backfill-Flow")
async def backfill_flow(symbol: str, daily_years: int = 5, intraday_days: int = 30):
    """
    특정 종목에 대해 일봉 N년치, 분봉 M일치 데이터를 백필(소급 수집)하는 플로우
    """
    logger = get_run_logger()
    notifier = Notifier()
    
    logger.info(f"Starting Backfill Flow for {symbol}")
    await notifier.notify_all(f"⏳ *[Backfill]* `{symbol}` 종목의 데이터 백필(일봉 {daily_years}년, 1분봉 {intraday_days}일)을 시작합니다.")
    
    try:
        # 일봉 백필 (기본 5년)
        await backfill_daily_task(symbol, daily_years)
        
        # 분봉 백필 (기본 30일)
        intraday_result = await backfill_intraday_task(symbol, intraday_days)
        
        msg = f"✅ *[Backfill]* `{symbol}` 종목의 데이터 백필이 완료되었습니다.\n"
        msg += f"• 일봉: 최대 {daily_years}년치 조회 및 갱신 완료\n"
        msg += f"• 1분봉: {intraday_result['success']}일치 성공, {intraday_result['skipped']}일 스킵"
        
        logger.info(msg.replace("\n", " "))
        await notifier.notify_all(msg)
    except Exception as e:
        error_msg = f"❌ *[Backfill]* `{symbol}` 종목 백필 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        await notifier.notify_all(error_msg)
        raise e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="백필(소급 수집) 오케스트레이터")
    parser.add_argument("--symbol", required=True, help="종목코드 (예: 005930)")
    parser.add_argument("--years", type=int, default=5, help="수집할 일봉 과거 연수 (기본 5년)")
    parser.add_argument("--days", type=int, default=30, help="수집할 1분봉 과거 일수 (최대 약 30일)")
    args = parser.parse_args()
    
    asyncio.run(backfill_flow(args.symbol, args.years, args.days))
