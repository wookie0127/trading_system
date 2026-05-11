import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable

from dotenv import load_dotenv
from loguru import logger
import discord
import anyio

from follow_dante_reading.signal_schema import ReadingMessage, TelegramDialog  # ty:ignore[unresolved-import]

try:
    from telethon import TelegramClient, events
    from telethon.tl.custom.dialog import Dialog
    from telethon.tl.custom.message import Message
except ImportError as exc:  # pragma: no cover
    raise RuntimeError(
        "Telethon is required for follow_dante_reading. Install project dependencies first."
    ) from exc


CURRENT_DIR = Path(__file__).resolve().parent
TELEGRAM_ENV_PATH = Path.home() / ".ssh" / "telegram"
load_dotenv(TELEGRAM_ENV_PATH)
load_dotenv(CURRENT_DIR.parents[2] / ".env")

API_KEYS_PATH = Path.home() / ".ssh" / "apikeys"
if API_KEYS_PATH.exists():
    load_dotenv(API_KEYS_PATH)

SESSION_BASENAME = CURRENT_DIR / "test_check"

async def get_discord_input(
    prompt: str,
    *,
    preferred_channel_name: str | None = None,
    channel_id: str | None = None,
    request_label: str = "📢 **[Input Request]**",
) -> str:
    """Discord 채널로부터 입력을 대기합니다."""
    token = os.environ.get("DISCORD_TOKEN") or os.environ.get("DISCORD_BOT_TOKEN")
    target_channel_id = channel_id or os.environ.get("DISCORD_CHANNEL_ID")
    
    if not token or not target_channel_id:
        # 폴백: Discord 정보가 없으면 터미널 입력 사용
        logger.warning("DISCORD_TOKEN or DISCORD_CHANNEL_ID missing. Falling back to terminal input.")
        return input(f"{prompt}: ")

    intents = discord.Intents.default()
    intents.message_content = True
    client = discord.Client(intents=intents)
    selected_channel = {"id": None}
    
    # anyio의 MemoryObjectStream을 사용하여 값을 전달받음 (Future 대체)
    send_stream, receive_stream = anyio.create_memory_object_stream(1)

    @client.event
    async def on_ready():
        logger.info("Auth-helper Discord client ready.")

        # 1. 이름 기준 채널 우선 탐색
        channel = None
        if preferred_channel_name:
            for guild in client.guilds:
                channel = discord.utils.get(guild.text_channels, name=preferred_channel_name)
                if channel:
                    logger.info(
                        "Found Discord input channel by name '{}' in guild '{}'",
                        preferred_channel_name,
                        guild.name,
                    )
                    break

        # 2. 이름 기반 탐색 실패 시 기본 채널 ID 사용
        if not channel and target_channel_id:
            channel = client.get_channel(int(target_channel_id))
            if channel:
                logger.info(f"Using Discord input channel ID: {target_channel_id}")

        if channel:
            selected_channel["id"] = channel.id
            await channel.send(f"{request_label} {prompt}를 입력해주세요.")
        else:
            logger.error(
                "Could not find Discord input channel (name={} or id={})",
                preferred_channel_name,
                target_channel_id,
            )
            await send_stream.send(None) # 에러 상황 알림

    @client.event
    async def on_message(message):
        if message.author.bot:
            return

        if selected_channel["id"] is None or message.channel.id != selected_channel["id"]:
            return

        content = message.content.strip()
        if content:
            logger.info(f"Received input from Discord: {content}")
            async with send_stream:
                await send_stream.send(content)
            await client.close()

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(client.start, token)

            with anyio.fail_after(300.0):
                async with receive_stream:
                    result = await receive_stream.receive()
                    if result is None:
                        raise RuntimeError("Discord auth channel not found")
                    return result
    except TimeoutError:
        logger.error("Timed out waiting for Discord input.")
        raise RuntimeError("Discord input timeout")
    finally:
        if not client.is_closed():
            await client.close()


class TelegramReadingClient:
    def __init__(
        self,
        api_id: int | None = None,
        api_hash: str | None = None,
        session_path: str | Path = SESSION_BASENAME,
        use_temp_session: bool = False,
    ):
        resolved_api_id = api_id or _read_api_id_from_env()
        resolved_api_hash = api_hash or _read_api_hash_from_env()

        if resolved_api_id is None or resolved_api_hash is None:
            raise ValueError(
                "Telegram API credentials are missing. "
                "Set api_id/api_hash or TELEGRAM_API_ID/TELEGRAM_API_HASH in ~/.ssh/telegram."
            )

        self.api_id = resolved_api_id
        self.api_hash = resolved_api_hash
        original_session_path = Path(session_path)
        self._temp_session_dir: tempfile.TemporaryDirectory[str] | None = None
        self.session_path = original_session_path

        if use_temp_session:
            self._temp_session_dir = tempfile.TemporaryDirectory(prefix="telegram_session_")
            temp_base = Path(self._temp_session_dir.name) / original_session_path.name
            _copy_telethon_session(original_session_path, temp_base)
            self.session_path = temp_base

        self.client = TelegramClient(str(self.session_path), self.api_id, self.api_hash)

    async def interactive_login(self, via_discord: bool = False) -> None:
        """대화형 로그인을 수행합니다. 세션이 없거나 만료된 경우 핸드폰 번호와 코드를 입력받습니다."""
        logger.info(f"Starting interactive login (via_discord={via_discord})...")

        if via_discord:
            await self.client.start(
                phone=lambda: get_discord_input(
                    "핸드폰 번호 (ex: +821012345678)",
                    preferred_channel_name="telegram_client",
                    request_label="🔐 **[Telegram Auth]**",
                ),
                code_callback=lambda: get_discord_input(
                    "인증 코드 (숫자 5자리)",
                    preferred_channel_name="telegram_client",
                    request_label="🔐 **[Telegram Auth]**",
                ),
                password=lambda: get_discord_input(
                    "2단계 인증 비밀번호 (설정된 경우만)",
                    preferred_channel_name="telegram_client",
                    request_label="🔐 **[Telegram Auth]**",
                )
            )
        else:
            await self.client.start()

        me = await self.client.get_me()
        if me:
            logger.info(f"Signed in successfully as {getattr(me, 'first_name', None)} (ID: {me.id})")
        else:
            logger.error("Failed to sign in. Please check your credentials.")

    async def ensure_authorized(self, interactive: bool = False, via_discord: bool = False) -> None:
        """
        클라이언트가 연결되어 있고 인증되었는지 확인합니다.
        interactive=True인 경우 인증되지 않았을 때 로그인을 시도합니다.
        """
        if not self.client.is_connected():
            await self.client.connect()

        is_authorized = await self.client.is_user_authorized()
        if is_authorized:
            return

        if interactive:
            logger.warning("Telegram session is not authorized. Starting interactive login...")
            await self.interactive_login(via_discord=via_discord)
        else:
            raise RuntimeError(
                "Telegram session is not authorized. "
                "Run login mode once in an interactive terminal to create or refresh the session."
            )

    async def check_session(self) -> bool:
        await self.client.connect()
        return await self.client.is_user_authorized()

    async def get_me_summary(self) -> dict[str, str | int | None]:
        await self.ensure_authorized()
        me = await self.client.get_me()
        return {
            "id": getattr(me, "id", None),
            "first_name": getattr(me, "first_name", None),
            "last_name": getattr(me, "last_name", None),
            "username": getattr(me, "username", None),
            "phone": getattr(me, "phone", None),
        }

    async def list_dialogs(
        self,
        limit: int = 100,
        query: str | None = None,
    ) -> list[TelegramDialog]:
        await self.ensure_authorized()
        dialogs: list[TelegramDialog] = []

        async for dialog in self.client.iter_dialogs(limit=limit):
            item = self._to_dialog_summary(dialog)
            if query and not self._matches_dialog_query(item, query):
                continue
            dialogs.append(item)

        return dialogs

    async def resolve_entity(
        self,
        chat: str | int,
    ):
        await self.ensure_authorized()
        return await self.client.get_entity(chat)

    async def fetch_chat_history(
        self,
        chat: str | int,
        limit: int = 100,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        download_media: bool = True,
        media_dir: str | Path | None = None,
    ) -> list[ReadingMessage]:
        await self.ensure_authorized()
        entity = await self.resolve_entity(chat)
        results: list[ReadingMessage] = []
        iter_limit = None if limit <= 0 else limit

        normalized_start = _normalize_boundary(start_date)
        normalized_end = _normalize_boundary(end_date)

        async for msg in self.client.iter_messages(entity, limit=iter_limit):
            posted_at = _normalize_boundary(getattr(msg, "date", None))
            if posted_at is None:
                continue

            if normalized_end and posted_at > normalized_end:
                continue

            if normalized_start and posted_at < normalized_start:
                break

            parsed = await self._to_reading_message(msg, download_media, media_dir)
            if parsed:
                results.append(parsed)

        results.sort(key=lambda item: item.posted_at)
        return results

    async def run_realtime(
        self,
        chat: str | int,
        on_message: Callable[[ReadingMessage], Awaitable[None]],
        download_media: bool = True,
        media_dir: str | Path | None = None,
    ) -> None:
        await self.ensure_authorized()
        entity = await self.resolve_entity(chat)
        logger.info(f"Listening for new Telegram messages from: {chat}")

        @self.client.on(events.NewMessage(chats=entity))
        async def _handler(event):
            parsed = await self._to_reading_message(event.message, download_media, media_dir)
            if parsed:
                await on_message(parsed)

        await self.client.run_until_disconnected()

    async def close(self) -> None:
        await self.client.disconnect()
        if self._temp_session_dir is not None:
            self._temp_session_dir.cleanup()
            self._temp_session_dir = None

    async def _to_reading_message(
        self,
        message: Message,
        download_media: bool,
        media_dir: str | Path | None,
    ) -> ReadingMessage | None:
        text = (message.message or "").strip()
        logger.debug(f"DEBUG: Processing raw message {message.id}. Text length: {len(text)}, Media: {bool(message.media)}")

        if not text and not message.media:
            logger.debug(f"DEBUG: Message {message.id} skipped (no text/media)")
            return None

        media_path = None
        if message.media and download_media:
            media_path = await self._download_media(message, media_dir)

        reactions = {}
        if message.reactions and getattr(message.reactions, "results", None):
            reactions = {
                reaction.reaction.emoticon: reaction.count
                for reaction in message.reactions.results
                if getattr(reaction.reaction, "emoticon", None)
            }

        chat = await message.get_chat()
        posted_at = message.date
        if posted_at.tzinfo is None:
            posted_at = posted_at.replace(tzinfo=timezone.utc)

        return ReadingMessage(
            source="telegram:dante",
            chat_id=getattr(chat, "id", None),
            chat_title=getattr(chat, "title", None),
            message_id=message.id,
            posted_at=posted_at.astimezone(),
            text=text,
            raw_text=text,
            has_media=bool(message.media),
            media_path=media_path,
            view_count=getattr(message, "views", None),
            forward_count=getattr(message, "forwards", None),
            reactions=reactions,
        )

    async def _download_media(self, message: Message, media_dir: str | Path | None) -> str | None:
        directory = Path(media_dir) if media_dir else CURRENT_DIR / "downloads"
        directory.mkdir(parents=True, exist_ok=True)
        file_stem = f"{message.id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        path = await self.client.download_media(message, file=directory / file_stem)
        return str(path) if path else None

    @staticmethod
    def _to_dialog_summary(dialog: Dialog) -> TelegramDialog:
        entity = dialog.entity
        return TelegramDialog(
            chat_id=getattr(entity, "id", 0),
            title=getattr(dialog, "title", None),
            username=getattr(entity, "username", None),
            entity_type=entity.__class__.__name__,
        )

    @staticmethod
    def _matches_dialog_query(dialog: TelegramDialog, query: str) -> bool:
        needle = query.casefold()
        fields = [
            str(dialog.chat_id),
            dialog.title or "",
            dialog.username or "",
        ]
        return any(needle in field.casefold() for field in fields)


def _read_api_id_from_env() -> int | None:
    raw = os.getenv("api_id") or os.getenv("TELEGRAM_API_ID")
    return int(raw) if raw else None


def _read_api_hash_from_env() -> str | None:
    return os.getenv("api_hash") or os.getenv("TELEGRAM_API_HASH")


def _normalize_boundary(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _copy_telethon_session(source_base: Path, target_base: Path) -> None:
    source_session = source_base.with_suffix(".session")
    if not source_session.exists():
        raise FileNotFoundError(f"Telegram session file not found: {source_session}")

    target_base.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_session, target_base.with_suffix(".session"))
