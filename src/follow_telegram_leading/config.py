from pathlib import Path

import yaml


CURRENT_DIR = Path(__file__).resolve().parent
CHAT_CONFIG_PATH = CURRENT_DIR / "chats.yaml"


def load_chat_aliases(config_path: str | Path = CHAT_CONFIG_PATH) -> dict[str, dict]:
    path = Path(config_path)
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp) or {}

    chats = payload.get("chats", {})
    if not isinstance(chats, dict):
        return {}

    return chats


def resolve_chat_reference(chat: str | int, aliases: dict[str, dict]) -> str | int:
    if isinstance(chat, int):
        return chat

    raw = str(chat).strip()
    if not raw:
        raise ValueError("chat reference is empty")

    if raw.lstrip("-").isdigit():
        return int(raw)

    matched = aliases.get(raw)
    if not matched:
        return raw

    if matched.get("chat_id") is not None:
        return int(matched["chat_id"])
    if matched.get("username"):
        return str(matched["username"])

    raise ValueError(f"Chat alias '{raw}' exists but has no chat_id or username")
