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

API_KEYS_PATH = Path.home() / ".ssh" / "apikeys"
if API_KEYS_PATH.exists():
    load_dotenv(API_KEYS_PATH)

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
        self.diary_channel_id = os.environ.get("DIARY_CHANNEL_ID", "1500995130604130445")
        
        # DISCORD_BOT_TOKEN 명칭 호환성 처리
        if not self.discord_token:
            self.discord_token = os.environ.get("DISCORD_BOT_TOKEN")
        
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

    async def send_discord_async(self, text: str, channel_id: str | None = None):
        # 1. Webhook 방식 (channel_id가 없을 때만 사용)
        if self.discord_webhook_url and not channel_id:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(self.discord_webhook_url, json={"content": text})
                    response.raise_for_status()
                    logger.info("Discord message sent via Webhook.")
                    return True
            except Exception as e:
                logger.error(f"Failed to send Discord message via Webhook: {e}")
                
        # 2. Bot REST API 방식
        target_channel = channel_id or self.discord_channel_id
        if self.discord_token and target_channel:
            try:
                headers = {"Authorization": f"Bot {self.discord_token}"}
                url = f"https://discord.com/api/v10/channels/{target_channel}/messages"
                
                async with httpx.AsyncClient() as client:
                    response = await client.post(url, headers=headers, json={"content": text})
                    response.raise_for_status()
                    logger.info(f"Discord message sent to channel {target_channel}.")
                    return True
            except Exception as e:
                logger.error(f"Failed to send Discord message to {target_channel}: {e}")
                return False

        logger.warning("Discord notification skipped: Missing credentials or channel ID.")
        return False

    async def notify_diary(self, text: str):
        """#dante_invest_diary 채널에 기록합니다."""
        if self.diary_channel_id:
            await self.send_discord_async(text, channel_id=self.diary_channel_id)
        else:
            # 다이어리 채널이 없으면 기본 채널로 전송
            await self.send_discord_async(f"📔 **[Diary]** {text}")

    async def notify_all(self, text: str):
        """Slack과 Discord 양쪽으로 알림 전송"""
        self.send_slack(text)
        await self.send_discord_async(text)

if __name__ == "__main__":
    import asyncio
    n = Notifier()
    test_msg = "🚀 **[Test]** 통합 알림 시스템 테스트 중입니다."
    asyncio.run(n.notify_all(test_msg))
