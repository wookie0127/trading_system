import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import asyncio
import os
from datetime import date
from prefect import flow, task, get_run_logger

# 프로젝트 루트(src)를 path에 추가하여 절대 임포트 가능하게 함
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from collectors.kr.kospi200_component_collector import collect_kospi200_components
from collectors.kr.kospi200_intraday_collector import collect_kospi200_intraday
from bots.notifier import Notifier

@task(name="Fetch KOSPI 200 Components", retries=2, retry_delay_seconds=30)
async def fetch_components_task(target_date: date):
    logger = get_run_logger()
    logger.info(f"Fetching KOSPI 200 components for {target_date}")
    await collect_kospi200_components(target_date)
    return True

@task(name="Collect Intraday Data", retries=2, retry_delay_seconds=60)
async def collect_intraday_task(target_date: date):
    logger = get_run_logger()
    logger.info(f"Starting KOSPI 200 1-min data collection for {target_date}")
    await collect_kospi200_intraday(target_date)
    return True

@flow(name="Daily-KOSPI200-Intraday-Flow")
async def daily_intraday_flow(target_date: date | None = None):
    """
    KOSPI 200 종목 당일 1분봉 데이터 및 구성종목 수집 오케스트레이션 플로우
    주장 종료 후 (예: 월~금 16:00) 실행
    """
    logger = get_run_logger()
    logger.info("Starting Daily KOSPI 200 Intraday Flow...")
    
    run_date = target_date or date.today()
    
    # 주말인 경우 실행 패스
    if run_date.weekday() >= 5:
        logger.info(f"Market closed on {run_date} (Weekend). Skipping flow.")
        return

    notifier = Notifier()
    await notifier.notify_all(f"🚀 *[Market Data]* {run_date} KOSPI 200 구성종목 및 1분봉 수집을 시작합니다.")
    try:
        await fetch_components_task(run_date)
        await collect_intraday_task(run_date)
        msg = f"✅ *[Market Data]* {run_date} KOSPI 200 구성종목 및 1분봉 수집이 성공적으로 완료되었습니다."
        logger.info(msg)
        await notifier.notify_all(msg)
    except Exception as e:
        error_msg = f"❌ *[Market Data]* {run_date} KOSPI 200 수집 중 오류 발생: {str(e)}"
        logger.error(error_msg)
        await notifier.notify_all(error_msg)
        raise e

if __name__ == "__main__":
    # 로컬 수동 테스트 실행: python src/daily_intraday_orchestrator.py
    # 스케줄러 배포: prefect deploy src/daily_intraday_orchestrator.py:daily_intraday_flow -n KOSPI-Intraday-Collector --cron "0 16 * * 1-5" --timezone "Asia/Seoul"
    asyncio.run(daily_intraday_flow())
