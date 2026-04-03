"""
Prefect Orchestrator for Intraday Sync.
Includes Slack notifications for start/completion/failure.

Requirements:
    pip install prefect slack-sdk
"""

import os
import asyncio
import yaml
from datetime import datetime
from pathlib import Path
from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

from intraday_sync import run_sync_all
from notifier import Notifier

# --- Load Configuration ---
CURRENT_DIR = Path(__file__).parent
CONFIG_PATH = CURRENT_DIR.parent / "config.yaml"
KEY_PATH = Path.home() / ".ssh" / "kis"

if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
        for k, v in config.items():
            os.environ[k] = str(v)

load_dotenv(KEY_PATH)


@task(name="Notify Start")
async def notify_start():
    logger = get_run_logger()
    msg = f"🚀 *[KIS]* 1분봉 데이터 수집을 시작합니다. ({datetime.now().strftime('%H:%M:%S')})"
    logger.info(msg)
    notifier = Notifier()
    await notifier.notify_all(msg)

@task(name="Sync Data", retries=2, retry_delay_seconds=30)
async def sync_data_task():
    success, total = await run_sync_all()
    return success, total

@task(name="Notify Success")
async def notify_success(stats: tuple[int, int]):
    success, total = stats
    logger = get_run_logger()
    msg = f"✅ *[KIS]* 1분봉 수집이 완료되었습니다.\n• 성공: {success}/{total} 종목\n• 완료시간: {datetime.now().strftime('%H:%M:%S')}"
    logger.info(msg)
    notifier = Notifier()
    await notifier.notify_all(msg)

@task(name="Notify Failure")
async def notify_failure(error_msg: str):
    logger = get_run_logger()
    msg = f"❌ *[KIS]* 1분봉 수집 중 오류가 발생했습니다.\n• 에러: {error_msg}"
    logger.error(msg)
    notifier = Notifier()
    await notifier.notify_all(msg)

@flow(name="KIS-Intraday-Sync-Flow")
async def intraday_sync_flow():
    await notify_start()
    try:
        stats = await sync_data_task()
        await notify_success(stats)
    except Exception as e:
        await notify_failure(str(e))
        raise e

if __name__ == "__main__":
    # To run locally: python src/intraday_orchestrator.py
    # To schedule: prefect deploy src/intraday_orchestrator.py --name "Intraday-Sync" --interval 600
    asyncio.run(intraday_sync_flow())
