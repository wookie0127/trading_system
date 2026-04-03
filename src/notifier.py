import os
import httpx
from slack_sdk import WebClient
from loguru import logger
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Load configuration (Reuse logic from other handlers)
CURRENT_DIR = Path(__file__).resolve().parent
KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(KEY_PATH)

CONFIG_PATH = CURRENT_DIR.parent / "config.yaml"
config = {}
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
        for k, v in config.items():
            os.environ[k] = str(v)

class Notifier:
    def __init__(self):
        self.slack_token = os.environ.get("SLACK_BOT_TOKEN")
        self.slack_channel = os.environ.get("SLACK_CHANNEL_ID", "C0ANXFNETHD") # Default from orchestrator
        self.discord_webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
        
        self.slack_client = WebClient(token=self.slack_token) if self.slack_token else None

    def send_slack(self, text: str):
        if not self.slack_client or not self.slack_channel:
            logger.warning("Slack notification skipped: Token or Channel ID missing.")
            return False
        try:
            self.slack_client.chat_postMessage(channel=self.slack_channel, text=text)
            logger.info("Slack message sent.")
            return True
        except Exception as e:
            logger.error(f"Failed to send Slack message: {e}")
            return False

    async def send_discord_async(self, text: str):
        if not self.discord_webhook_url:
            logger.warning("Discord notification skipped: Webhook URL missing.")
            return False
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(self.discord_webhook_url, json={"content": text})
                response.raise_for_status()
                logger.info("Discord message sent.")
                return True
        except Exception as e:
            logger.error(f"Failed to send Discord message: {e}")
            return False

    async def notify_all(self, text: str):
        """Slack과 Discord 양쪽으로 알림 전송"""
        self.send_slack(text)
        await self.send_discord_async(text)

if __name__ == "__main__":
    import asyncio
    n = Notifier()
    test_msg = "🚀 **[Test]** 통합 알림 시스템 테스트 중입니다."
    asyncio.run(n.notify_all(test_msg))
