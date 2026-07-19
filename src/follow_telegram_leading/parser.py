import re
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any
import httpx
from loguru import logger
from dotenv import load_dotenv

from follow_telegram_leading.signal_schema import (
    ReadingMessage,
    ReadingSignal,
    project_root,
)


load_dotenv(project_root() / ".env")

CHANNEL_STRATEGY_BY_CHAT_ID = {
    3875818348: "cafe_share",
    3956165696: "chart_master_kospi",
}
CHANNEL_STRATEGY_BY_TITLE = {
    "[N]카페 정보공유 소통채널": "cafe_share",
    "차트마스터 코스피방": "chart_master_kospi",
}
STOP_LOSS_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*%\s*손절")
FUTURES_QUANTITY_PATTERN = re.compile(r"(\d+)\s*계약")
COMPANY_LINE_PATTERN = re.compile(r"^[A-Za-z0-9가-힣&\-\.\(\)]+$")
COMPANY_LABEL_PATTERN = re.compile(
    r"(?:종목명|종목)\s*[:：]\s*([A-Za-z0-9가-힣&\-\.\(\)]+)"
)

BUY_PHRASES = (
    "들어가셔도",
    "매수",
    "매수 가능",
    "매수 관점",
    "비중",
    "담아",
)
SELL_PHRASES = (
    "매도",
    "익절",
    "정리",
    "청산",
    "비중 축소",
)
WATCH_PHRASES = (
    "지지",
    "괜찮을",
    "좋아보",
    "좋네요",
    "관심",
)
DAYTRADE_PHRASES = (
    "단타",
    "장중",
    "당일",
    "오늘",
    "돌파",
    "급등",
    "거래량",
)
SWING_PHRASES = (
    "스윙",
    "눌림",
    "며칠",
    "추세",
    "중기",
    "보유",
    "섹터",
)


def parse_reading_signal(message: ReadingMessage) -> ReadingSignal | None:
    text = (message.text or "").strip()
    if not text:
        return None

    strategy_name = resolve_channel_strategy_name(message)
    company_name = _extract_company_name(text)
    stop_loss_pct = _extract_stop_loss_pct(text)
    action = _infer_action(text, stop_loss_pct, strategy_name=strategy_name)
    confidence = _infer_confidence(text, company_name, stop_loss_pct, action)

    if (
        not company_name
        and action == "ignore"
        and strategy_name != "chart_master_kospi"
    ):
        return None

    return ReadingSignal(
        source=message.source,
        message_id=message.message_id,
        posted_at=message.posted_at,
        chat_id=message.chat_id,
        chat_title=message.chat_title,
        company_name=company_name,
        action=action,
        confidence=confidence,
        stop_loss_pct=stop_loss_pct,
        entry_hint=_infer_entry_hint(text),
        rationale_text=text,
        summary=text[:100].replace("\n", " "),  # 룰 기반 요약 (첫 100자)
        raw_text=message.raw_text,
        trade_style=_infer_trade_style(text),
        media_path=message.media_path,
        strategy_name=strategy_name,
    )


def resolve_channel_strategy_name(message: ReadingMessage) -> str:
    if message.chat_id in CHANNEL_STRATEGY_BY_CHAT_ID:
        return CHANNEL_STRATEGY_BY_CHAT_ID[message.chat_id]

    title = (message.chat_title or "").strip()
    if title in CHANNEL_STRATEGY_BY_TITLE:
        return CHANNEL_STRATEGY_BY_TITLE[title]

    if title:
        return (
            re.sub(r"[^0-9A-Za-z가-힣_]+", "_", title).strip("_").lower()
            or "telegram_stock_leading"
        )
    return "telegram_stock_leading"


def _extract_company_name(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    for line in lines[:5]:
        label_match = COMPANY_LABEL_PATTERN.search(line)
        if label_match:
            return label_match.group(1).strip()

    first_line = _normalize_company_line(lines[0])
    if len(first_line) <= 20 and COMPANY_LINE_PATTERN.match(first_line):
        return first_line
    return None


def _normalize_company_line(line: str) -> str:
    line = line.strip()
    line = re.sub(r"^[^A-Za-z0-9가-힣]+", "", line)
    return line.lstrip("#-• ").strip()


def _extract_stop_loss_pct(text: str) -> float | None:
    normalized = text.replace("손절라인", "손절")
    match = STOP_LOSS_PATTERN.search(normalized)
    if not match:
        return None

    value = float(match.group(1))
    return value / 100.0


def _infer_action(
    text: str, stop_loss_pct: float | None, strategy_name: str | None = None
) -> str:
    if strategy_name == "chart_master_kospi":
        if any(phrase in text for phrase in SELL_PHRASES) or "청산" in text:
            return "sell"
        if (
            any(phrase in text for phrase in BUY_PHRASES)
            or "진입" in text
            or "롱" in text
        ):
            return "buy_candidate"
        if any(phrase in text for phrase in WATCH_PHRASES):
            return "watch"
        return "ignore"

    if any(phrase in text for phrase in SELL_PHRASES):
        return "sell"
    if any(phrase in text for phrase in BUY_PHRASES):
        return "buy_candidate" if stop_loss_pct is not None else "watch"
    if any(phrase in text for phrase in WATCH_PHRASES):
        return "watch"
    return "ignore"


def _infer_entry_hint(text: str) -> str | None:
    if "돌파" in text:
        return "breakout"
    if "지지" in text or "눌림" in text:
        return "pullback"
    if "들어가셔도" in text or "매수" in text:
        return "current_price"
    return None


def _infer_confidence(
    text: str,
    company_name: str | None,
    stop_loss_pct: float | None,
    action: str,
) -> float:
    score = 0.15
    if company_name:
        score += 0.3
    if stop_loss_pct is not None:
        score += 0.25
    if action in {"buy_candidate", "sell"}:
        score += 0.2
    if "펀더멘탈" in text or "지지" in text:
        score += 0.05
    return min(score, 0.95)


def _infer_trade_style(text: str) -> str:
    daytrade_hits = sum(1 for phrase in DAYTRADE_PHRASES if phrase in text)
    swing_hits = sum(1 for phrase in SWING_PHRASES if phrase in text)
    if daytrade_hits > swing_hits:
        return "daytrade"
    if swing_hits > daytrade_hits:
        return "swing"
    return "unknown"


LLM_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "company_name": {"type": ["string", "null"]},
        "action": {
            "type": "string",
            "enum": ["buy_candidate", "sell", "watch", "ignore"],
        },
        "trade_style": {"type": "string", "enum": ["daytrade", "swing", "unknown"]},
        "stop_loss_pct": {"type": ["number", "string", "null"]},
        "entry_hint": {
            "type": ["string", "null"],
            "enum": ["breakout", "pullback", "current_price", None],
        },
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "rationale_text": {"type": "string"},
    },
    "required": [
        "summary",
        "company_name",
        "action",
        "trade_style",
        "stop_loss_pct",
        "entry_hint",
        "confidence",
        "rationale_text",
    ],
    "additionalProperties": False,
}


def parse_reading_signal_with_llm(
    message: ReadingMessage, model: str | None = None
) -> ReadingSignal | None:
    """LLM(Codex/Gemini)을 사용하여 메시지를 파싱합니다."""
    text = (message.text or "").strip()
    if not text:
        return None

    backend = os.getenv("TLEADING_LLM_BACKEND", "codex").strip().lower()
    strategy_name = resolve_channel_strategy_name(message)
    prompt = _render_llm_prompt(text, strategy_name=strategy_name)

    try:
        if backend == "gemini":
            llm_out = _run_gemini_api(prompt, model)
        elif backend == "rule":
            return parse_reading_signal(message)
        else:
            llm_out = _run_codex_cli(prompt, model)

        logger.debug(f"{backend} output: {llm_out}")

        json_str = _extract_json(llm_out)
        data = json.loads(json_str)
        return _build_signal_from_llm_data(message, data)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        detail = stderr or stdout or str(e)
        logger.error(
            f"LLM Parsing failed: {detail}. Falling back to rule-based parser."
        )
        return parse_reading_signal(message)
    except Exception as e:
        logger.error(f"LLM Parsing failed: {e}. Falling back to rule-based parser.")
        return parse_reading_signal(message)


def _render_llm_prompt(text: str, strategy_name: str | None = None) -> str:
    strategy_context = ""
    if strategy_name == "chart_master_kospi":
        strategy_context = (
            "\n"
            "추가 규칙:\n"
            "- 이 메시지는 차트마스터 코스피방 신호일 수 있습니다.\n"
            "- 코스피 지수선물/선물 진입과 청산 메시지는 actionable signal로 분류하세요.\n"
            "- company_name은 null이어도 됩니다.\n"
            "- 예: '코스피 2계약 매수진입'은 buy_candidate, '청산하겠습니다'는 sell로 분류하세요.\n"
        )
    return f"""
아래는 주식 리딩방의 텔레그램 메시지입니다.
{strategy_context}

목표:
1. 메시지 핵심 내용을 1~2문장으로 요약하세요. (summary)
2. 한국 주식 종목명, 액션, 매매 스타일, 손절률, 매수 힌트, 판단 근거를 추출하세요.
3. 기본적으로는 개별 주식 종목이 아니면 company_name은 null로 두고 action은 ignore로 판단하세요.
4. 다만 chart_master_kospi 전략에서는 코스피 지수선물/선물 메시지를 actionable signal로 판단하세요.
4. stop_loss_pct는 비율값이 아니라 퍼센트 포인트로 출력하세요. 예: "5% 손절"은 5.0, "3.5% 손절"은 3.5입니다.
5. 최종 응답은 반드시 JSON 객체 하나만 출력하세요. 코드블록이나 설명 문장은 금지합니다.

허용 값:
- action: buy_candidate, sell, watch, ignore
- trade_style: daytrade, swing, unknown
- entry_hint: breakout, pullback, current_price, null
- confidence: 0.0 ~ 1.0

[메시지 본문]
{text}
"""


def _run_codex_cli(prompt: str, model: str | None = None) -> str:
    cli_command = os.getenv("CODEX_CLI_COMMAND", "codex").strip() or "codex"
    codex_model = model or os.getenv("CODEX_MODEL", "").strip()
    timeout_seconds = int(os.getenv("CODEX_TIMEOUT_SECONDS", "120"))

    with tempfile.TemporaryDirectory(prefix="tleading-codex-") as temp_dir:
        temp_path = Path(temp_dir)
        schema_path = temp_path / "reading_signal.schema.json"
        output_path = temp_path / "reading_signal.json"
        schema_path.write_text(
            json.dumps(LLM_OUTPUT_SCHEMA, ensure_ascii=False), encoding="utf-8"
        )

        cmd = [
            cli_command,
            "exec",
            "--ephemeral",
            "--sandbox",
            "read-only",
            "--ask-for-approval",
            "never",
            "--cd",
            str(project_root()),
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(output_path),
        ]
        if codex_model:
            cmd.extend(["--model", codex_model])
        cmd.append("-")

        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout_seconds,
            cwd=project_root(),
        )
        if output_path.exists():
            output = output_path.read_text(encoding="utf-8").strip()
            if output:
                return output
        return result.stdout.strip()


def _run_gemini_api(prompt: str, model: str | None = None) -> str:
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY or GOOGLE_API_KEY is required for TLEADING_LLM_BACKEND=gemini"
        )

    gemini_model = model or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    timeout_seconds = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "30"))
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "responseMimeType": "application/json",
        },
    }

    response = httpx.post(
        endpoint,
        params={"key": api_key},
        json=payload,
        timeout=timeout_seconds,
    )
    if response.status_code != 200:
        try:
            error_payload = response.json().get("error", {})
            detail = error_payload.get("message") or response.text[:500]
        except Exception:
            detail = response.text[:500]
        raise RuntimeError(
            f"Gemini API request failed: HTTP {response.status_code}: {detail}"
        )

    data = response.json()
    candidates = data.get("candidates") or []
    if not candidates:
        raise RuntimeError("Gemini API response did not include candidates")

    parts = candidates[0].get("content", {}).get("parts") or []
    text = "".join(str(part.get("text") or "") for part in parts).strip()
    if not text:
        raise RuntimeError("Gemini API response did not include text")
    return text


def _build_signal_from_llm_data(
    message: ReadingMessage, data: dict[str, Any]
) -> ReadingSignal:
    text = (message.text or "").strip()
    strategy_name = resolve_channel_strategy_name(message)
    action = _normalize_action(data.get("action"))
    action = _refine_action_for_strategy(text, action, strategy_name=strategy_name)
    confidence = _coerce_confidence(data.get("confidence"))

    return ReadingSignal(
        source=message.source,
        message_id=message.message_id,
        posted_at=message.posted_at,
        chat_id=message.chat_id,
        chat_title=message.chat_title,
        company_name=_normalize_optional_string(data.get("company_name")),
        action=action,
        confidence=confidence,
        stop_loss_pct=_coerce_stop_loss_pct(data.get("stop_loss_pct")),
        entry_hint=_normalize_entry_hint(data.get("entry_hint")),
        rationale_text=_normalize_optional_string(
            data.get("rationale_text") or data.get("rationale")
        )
        or text[:200],
        summary=_normalize_optional_string(data.get("summary")) or text[:100],
        raw_text=message.raw_text,
        trade_style=_normalize_trade_style(data.get("trade_style")),
        media_path=message.media_path,
        strategy_name=strategy_name,
    )


def _extract_json(text: str) -> str:
    # ```json ... ``` 블록 추출 시도
    match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
    if match:
        return match.group(1)
    # 그냥 JSON 중괄호 추출 시도
    match = re.search(r"({.*})", text, re.DOTALL)
    if match:
        return match.group(1)
    return text


def _coerce_stop_loss_pct(raw_value) -> float | None:
    if raw_value in (None, "", "null"):
        return None

    if isinstance(raw_value, str):
        normalized = raw_value.strip().replace("%", "")
    else:
        normalized = str(raw_value)

    try:
        return float(normalized) / 100.0
    except (TypeError, ValueError):
        return None


def _normalize_optional_string(raw_value) -> str | None:
    if raw_value in (None, "", "null"):
        return None
    value = str(raw_value).strip()
    return value or None


def _normalize_action(raw_value) -> str:
    normalized = str(raw_value or "").strip().lower()
    if normalized in {"buy_candidate", "sell", "watch", "ignore"}:
        return normalized
    return "ignore"


def _normalize_entry_hint(raw_value) -> str | None:
    normalized = str(raw_value or "").strip().lower()
    if normalized in {"breakout", "pullback", "current_price"}:
        return normalized
    return None


def _coerce_confidence(raw_value) -> float:
    try:
        return max(0.0, min(float(raw_value), 1.0))
    except (TypeError, ValueError):
        return 0.5


def _normalize_trade_style(raw_value) -> str:
    normalized = str(raw_value or "").strip().lower()
    if normalized in {"daytrade", "short_term", "short-term", "단타"}:
        return "daytrade"
    if normalized in {"swing", "스윙"}:
        return "swing"
    return "unknown"


def _refine_action_for_strategy(text: str, action: str, *, strategy_name: str) -> str:
    if strategy_name != "chart_master_kospi":
        return action

    if action in {"buy_candidate", "sell"}:
        return action

    if any(phrase in text for phrase in SELL_PHRASES) or "청산" in text:
        return "sell"

    if any(phrase in text for phrase in BUY_PHRASES) or "진입" in text or "롱" in text:
        return "buy_candidate"

    if any(phrase in text for phrase in WATCH_PHRASES):
        return "watch"

    return action
