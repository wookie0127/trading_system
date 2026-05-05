import re
import json
import os
import subprocess
from pathlib import Path
from loguru import logger

from follow_dante_reading.signal_schema import ReadingMessage, ReadingSignal  


STOP_LOSS_PATTERN = re.compile(r"(-?\d+(?:\.\d+)?)\s*%\s*손절")
COMPANY_LINE_PATTERN = re.compile(r"^[A-Za-z0-9가-힣&\-\.\(\)]+$")

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
    "비중 축소",
)
WATCH_PHRASES = (
    "지지",
    "괜찮을",
    "좋아보",
    "좋네요",
    "관심",
)


def parse_reading_signal(message: ReadingMessage) -> ReadingSignal | None:
    text = (message.text or "").strip()
    if not text:
        return None

    company_name = _extract_company_name(text)
    stop_loss_pct = _extract_stop_loss_pct(text)
    action = _infer_action(text, stop_loss_pct)
    confidence = _infer_confidence(text, company_name, stop_loss_pct, action)

    if not company_name and action == "ignore":
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
        summary=text[:100].replace("\n", " "), # 룰 기반 요약 (첫 100자)
        raw_text=message.raw_text,
        media_path=message.media_path,
    )


def _extract_company_name(text: str) -> str | None:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return None

    first_line = lines[0].lstrip("#-• ").strip()
    if len(first_line) <= 20 and COMPANY_LINE_PATTERN.match(first_line):
        return first_line
    return None


def _extract_stop_loss_pct(text: str) -> float | None:
    normalized = text.replace("손절라인", "손절")
    match = STOP_LOSS_PATTERN.search(normalized)
    if not match:
        return None

    value = float(match.group(1))
    return value / 100.0


def _infer_action(text: str, stop_loss_pct: float | None) -> str:
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


def parse_reading_signal_with_llm(message: ReadingMessage, model: str = "gemini") -> ReadingSignal | None:
    """LLM(Gemini/Codex)을 사용하여 메시지를 파싱합니다."""
    text = (message.text or "").strip()
    if not text:
        return None

    cli_command = os.getenv("GEMINI_CLI_COMMAND", model).strip() or model
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    approval_mode = os.getenv("GEMINI_APPROVAL_MODE", "yolo")
    timeout_seconds = int(os.getenv("GEMINI_TIMEOUT_SECONDS", "30"))

    prompt = f"""
    아래는 주식 리딩방의 텔레그램 메시지입니다. 
    1. 이 메시지의 핵심 내용을 1~2문장으로 요약하세요. (summary)
    2. 종목명, 액션(buy_candidate, sell, watch, ignore), 손절가(%), 매수 힌트, 그리고 분석 근거를 추출하여 JSON 형식으로 응답하세요.
    3. 응답은 반드시 JSON만 출력하세요. 코드블록이나 설명 문장은 금지합니다.
    
    [응답 JSON 형식]
    {{
        "summary": "메시지 핵심 요약",
        "company_name": "종목명",
        "action": "buy_candidate" | "sell" | "watch" | "ignore",
        "stop_loss_pct": "손절가 (예: 5.0)",
        "entry_hint": "breakout" | "pullback" | "current_price" | null,
        "confidence": 0.0 ~ 1.0,
        "rationale_text": "짧은 분석 요약"
    }}

    [메시지 본문]
    {text}
    """

    try:
        cmd = [
            cli_command,
            "--prompt", prompt,
            "--approval-mode", approval_mode,
            "--output-format", "text",
            "--skip-trust",
            "--model", gemini_model
        ]

        # ~/.gemini 디렉토리가 있으면 그곳에서 실행 (기존 오케스트레이터 방식)
        gemini_home = Path.home() / ".gemini"
        run_cwd = gemini_home if gemini_home.exists() else Path.cwd()

        # 30초 타임아웃 추가하여 무한 대기 방지
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=30,
            cwd=run_cwd
        )
        llm_out = result.stdout.strip()
        logger.debug(f"Gemini output: {llm_out}")

        json_str = _extract_json(llm_out)
        data = json.loads(json_str)
        stop_loss_pct = _coerce_stop_loss_pct(data.get("stop_loss_pct"))

        return ReadingSignal(
            source=message.source,
            message_id=message.message_id,
            posted_at=message.posted_at,
            chat_id=message.chat_id,
            chat_title=message.chat_title,
            company_name=data.get("company_name"),
            action=data.get("action", "ignore"),
            confidence=float(data.get("confidence", 0.5)),
            stop_loss_pct=stop_loss_pct,
            entry_hint=data.get("entry_hint"),
            rationale_text=data.get("rationale_text") or text[:200],
            summary=data.get("summary") or text[:100],
            raw_text=message.raw_text,
            media_path=message.media_path,
        )
    except Exception as e:
        logger.error(f"LLM Parsing failed: {e}. Falling back to rule-based parser.")
        return parse_reading_signal(message)


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
