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

CONFIG_PATH = CURRENT_DIR.parents[1] / "config.yaml"
config = {}
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        for k, v in (config or {}).items():
            os.environ[k] = str(v)

class Notifier:
    def __init__(self):
        self.slack_token = os.environ.get("SLACK_BOT_TOKEN")
        self.slack_channel = os.environ.get("SLACK_CHANNEL_ID", "C0ANXFNETHD") # Default from orchestrator
        self.discord_webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
        self.discord_token = os.environ.get("DISCORD_TOKEN")
        self.discord_channel_id = os.environ.get("DISCORD_CHANNEL_ID")
        
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
        # 1. Webhook 방식 (우선 적용)
        if self.discord_webhook_url:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(self.discord_webhook_url, json={"content": text})
                    response.raise_for_status()
                    logger.info("Discord message sent via Webhook.")
                    return True
            except Exception as e:
                logger.error(f"Failed to send Discord message via Webhook: {e}")
                return False
                
        # 2. Bot REST API 방식 (가이드에 따라 봇을 생성했을 경우)
        if self.discord_token and self.discord_channel_id:
            try:
                # 봇의 경우 token 앞에 "Bot " 접두사를 붙여야 합니다.
                headers = {"Authorization": f"Bot {self.discord_token}"}
                url = f"https://discord.com/api/v10/channels/{self.discord_channel_id}/messages"
                
                async with httpx.AsyncClient() as client:
                    response = await client.post(url, headers=headers, json={"content": text})
                    response.raise_for_status()
                    logger.info("Discord message sent via Bot REST API.")
                    return True
            except Exception as e:
                logger.error(f"Failed to send Discord message via Bot REST API: {e}")
                return False

        logger.warning("Discord notification skipped: Both Webhook URL and Bot Token/Channel ID are missing.")
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
