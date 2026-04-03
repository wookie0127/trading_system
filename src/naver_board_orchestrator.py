import asyncio
import os
from datetime import datetime
from pathlib import Path
from prefect import flow, task, get_run_logger
from loguru import logger as loguru_logger

# 프로젝트 루트(src)를 path에 추가하여 절대 임포트 가능하게 함
import sys
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from collectors.naver_board.collector import NaverBoardCollector
from notifier import Notifier

# --- Configuration ---
DEFAULT_SYMBOLS = "000660,005930" # SK Hynix, Samsung Electronics
MAX_PAGES = 5

@task(name="Fetch Symbols")
def fetch_symbols_task(symbols: str = DEFAULT_SYMBOLS):
    """수집 대상 종목 리스트 결정 (향후 KOSPI 200 등으로 확장 가능)"""
    logger = get_run_logger()
    logger.info(f"Target symbols: {symbols}")
    return symbols

@task(name="Collect Naver Board", retries=2, retry_delay_seconds=60)
async def collect_board_task(symbols: str, max_pages: int):
    """네이버 종목 토론방 수집 실행"""
    logger = get_run_logger()
    logger.info(f"Starting collection for {symbols} (max_pages={max_pages})")
    
    collector = NaverBoardCollector()
    # collector.run() 자체가 내부에서 알림을 보내지만, 
    # 흐름 제어를 위해 결과를 반환하도록 되어 있음 (이미 이전 단계에서 수정 완료)
    await collector.run(symbols=symbols, max_pages=max_pages)
    return True

@flow(name="Naver-Board-Collection-Flow")
async def naver_board_flow(symbols: str = DEFAULT_SYMBOLS, max_pages: int = MAX_PAGES):
    """
    네이버 종목 토론방 수집 오케스트레이션 플로우
    주기: 4시간 (0 */4 * * *)
    """
    logger = get_run_logger()
    logger.info("Starting Naver Board Collection Flow...")
    
    try:
        target_symbols = fetch_symbols_task(symbols)
        await collect_board_task(target_symbols, max_pages)
        logger.info("Naver Board Collection Flow completed successfully.")
    except Exception as e:
        logger.error(f"Flow failed: {e}")
        # Notifier를 통한 직접 실패 알림 (필요 시)
        notifier = Notifier()
        await notifier.notify_all(f"❌ *[Naver Board Flow]* 실행 중 오류 발생: {str(e)}")
        raise e

if __name__ == "__main__":
    # 로컬 실행: python src/naver_board_orchestrator.py
    # 배포: prefect deploy src/naver_board_orchestrator.py --name "Naver-Board-Sync" --interval 14400 (4시간)
    asyncio.run(naver_board_flow())
