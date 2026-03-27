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
from slack_sdk import WebClient
from dotenv import load_dotenv

from intraday_sync import run_sync_all

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

# --- Slack Configuration ---
# You can find the Channel ID by right-clicking a channel in Slack -> Copy Link
# The ID is the last part of the URL (e.g. C0123456789)
check_collect_data_ch = "C0ANXFNETHD"
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", check_collect_data_ch) # Placeholder

def send_slack_message(text: str):
    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token or not SLACK_CHANNEL_ID:
        return
    try:
        client = WebClient(token=token)
        client.chat_postMessage(channel=SLACK_CHANNEL_ID, text=text)
    except Exception as e:
        print(f"Failed to send Slack message: {e}")

@task(name="Notify Start")
def notify_start():
    logger = get_run_logger()
    msg = f"🚀 *[KIS]* 1분봉 데이터 수집을 시작합니다. ({datetime.now().strftime('%H:%M:%S')})"
    logger.info(msg)
    send_slack_message(msg)

@task(name="Sync Data", retries=2, retry_delay_seconds=30)
async def sync_data_task():
    logger = get_run_logger()
    success, total = await run_sync_all()
    return success, total

@task(name="Notify Success")
def notify_success(stats: tuple[int, int]):
    success, total = stats
    logger = get_run_logger()
    msg = f"✅ *[KIS]* 1분봉 수집이 완료되었습니다.\n• 성공: {success}/{total} 종목\n• 완료시간: {datetime.now().strftime('%H:%M:%S')}"
    logger.info(msg)
    send_slack_message(msg)

@task(name="Notify Failure")
def notify_failure(error_msg: str):
    logger = get_run_logger()
    msg = f"❌ *[KIS]* 1분봉 수집 중 오류가 발생했습니다.\n• 에러: {error_msg}"
    logger.error(msg)
    send_slack_message(msg)

@flow(name="KIS-Intraday-Sync-Flow")
async def intraday_sync_flow():
    notify_start()
    try:
        stats = await sync_data_task()
        notify_success(stats)
    except Exception as e:
        notify_failure(str(e))
        raise e

if __name__ == "__main__":
    # To run locally: python src/intraday_orchestrator.py
    # To schedule: prefect deploy src/intraday_orchestrator.py --name "Intraday-Sync" --interval 600
    asyncio.run(intraday_sync_flow())
