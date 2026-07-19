import arrow
import asyncio
import email.utils
import os
import shlex
import shutil
import subprocess
import sys
import yaml
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import yaml
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task


def read_yaml(yaml_path: str) -> dict:
    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
        return config


CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from news.cmux_gemini import run_cmux_gemini_pane

# 환경 설정 로드
KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(str(KEY_PATH))
load_dotenv(str(Path.home() / ".ssh" / "apikeys"))
load_dotenv(str(SRC_DIR / ".env"))

DEFAULT_FEEDS = [
    "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/headlines/section/topic/WORLD?hl=ko&gl=KR&ceid=KR:ko",
]
DEFAULT_BACKEND = "openai"
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_CODEX_MODEL = ""
DEFAULT_AGY_MODEL = ""
DEFAULT_CODEX_COMMAND = "codex"
DEFAULT_AGY_COMMAND = "agy"
DEFAULT_CMUX_COMMAND = "cmux"
DEFAULT_TIMEOUT_SECONDS = 180
DEFAULT_OUTPUT_DIR = Path("data/news_summaries")
DEFAULT_OBSIDIAN_SUBDIR = "뉴스요약"
DEFAULT_MAX_ITEMS = 20
DEFAULT_NEWS_TIMEZONE = "Asia/Seoul"

NEWS_CONFIG_PATH = SRC_DIR / "config.yaml"
NEWS_CONFIGS = read_yaml(str(NEWS_CONFIG_PATH)) if NEWS_CONFIG_PATH.exists() else {}

KOREAN_WEEKDAYS = ["월", "화", "수", "목", "금", "토", "일"]


@dataclass
class FeedItem:
    title: str
    link: str
    published_at: str
    source: str


def _news_timezone() -> ZoneInfo:
    return ZoneInfo(os.getenv("NEWS_TIMEZONE", DEFAULT_NEWS_TIMEZONE))


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _config_list(name: str, default: list[str]) -> list[str]:
    value = NEWS_CONFIGS.get(name)
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [item.strip() for item in value.split(",") if item.strip()]
    return _env_list(name, default)


def _config_value(name: str, default: str | int | None = None):
    val = NEWS_CONFIGS.get(name)
    if val is None:
        val = os.getenv(name)
    if val is None:
        return default
    return val


def _current_news_datetime() -> datetime:
    return datetime.now(_news_timezone())


def _parse_pub_date(raw: str | None) -> datetime:
    if not raw:
        return datetime.min
    try:
        return email.utils.parsedate_to_datetime(raw).astimezone()
    except Exception:
        return datetime.min


def _extract_source(item: ET.Element) -> str:
    source = item.findtext("source")
    if source:
        return source.strip()

    title = (item.findtext("title") or "").strip()
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return "Unknown"


def _extract_output_text(payload: dict) -> str:
    output_text = payload.get("output_text")
    if output_text:
        return output_text.strip()

    parts: list[str] = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") == "output_text" and content.get("text"):
                parts.append(content["text"])
    return "\n".join(parts).strip()


async def fetch_feed_items(feed_url: str) -> list[FeedItem]:
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.get(feed_url)
        response.raise_for_status()

    root = ET.fromstring(response.text)
    items: list[FeedItem] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        if not title or not link:
            continue
        pub_date = _parse_pub_date(item.findtext("pubDate"))
        items.append(
            FeedItem(
                title=title,
                link=link,
                published_at=pub_date.isoformat() if pub_date != datetime.min else "",
                source=_extract_source(item),
            )
        )
    return items


# ==========================================
# 1. OpenAI 백엔드 요약 기능
# ==========================================
async def summarize_with_openai(model: str, prompt: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "reasoning": {"effort": "low"},
        "instructions": (
            "You create concise Korean markdown briefings from news headlines. "
            "Return valid markdown only. "
            "Do not invent facts beyond the provided headlines. "
            "If evidence is weak, explicitly say so."
        ),
        "input": prompt,
    }

    async with httpx.AsyncClient(timeout=90.0) as client:
        response = await client.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()

    summary = _extract_output_text(response.json())
    if not summary:
        raise RuntimeError("OpenAI response did not contain output_text")
    return summary


# ==========================================
# 2. Gemini 백엔드 요약 기능
# ==========================================
def _gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API") or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API, GEMINI_API_KEY, or GOOGLE_API_KEY is required for NEWS_LLM_BACKEND=gemini")
    return api_key


def _extract_gemini_text(payload: dict) -> str:
    candidates = payload.get("candidates") or []
    if not candidates:
        raise RuntimeError("Gemini API response did not include candidates")

    parts = candidates[0].get("content", {}).get("parts") or []
    return "".join(str(part.get("text") or "") for part in parts).strip()


async def summarize_with_gemini_api(model: str, prompt: str, timeout_seconds: int) -> str:
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "text/plain",
        },
    }

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(
            endpoint,
            params={"key": _gemini_api_key()},
            json=payload,
        )

    if response.status_code != 200:
        try:
            error_payload = response.json().get("error", {})
            detail = error_payload.get("message") or response.text[:500]
        except Exception:
            detail = response.text[:500]
        raise RuntimeError(f"Gemini API request failed: HTTP {response.status_code}: {detail}")

    summary = _extract_gemini_text(response.json())
    if not summary:
        raise RuntimeError("Gemini API response did not include text")
    return summary


# ==========================================
# 3. CLI 백엔드 요약 기능
# ==========================================
def run_codex_cli(prompt: str, command: str, model: str, timeout_seconds: int) -> str:
    command_parts = shlex.split(command)
    executable = command_parts[0]
    if shutil.which(executable) is None and not Path(executable).exists():
        raise RuntimeError(f"Codex CLI executable not found: {executable}")

    # --ask-for-approval is a global option and must precede the 'exec' subcommand
    cli_args = [
        executable,
        "--ask-for-approval",
        "never",
    ]
    if len(command_parts) > 1:
        cli_args.extend(command_parts[1:])

    cli_args.extend([
        "exec",
        "--sandbox",
        "read-only",
    ])
    if model:
        cli_args.extend(["--model", model])
    cli_args.append("-")

    completed = subprocess.run(
        cli_args,
        input=prompt,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Codex CLI failed with exit status "
            f"{completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
        )

    output = completed.stdout.strip()
    if not output:
        raise RuntimeError("Codex CLI returned empty output")
    return output


def run_agy_cli(prompt: str, command: str, model: str, timeout_seconds: int) -> str:
    command_parts = shlex.split(command)
    executable = command_parts[0]
    if shutil.which(executable) is None and not Path(executable).exists():
        raise RuntimeError(f"agy CLI executable not found: {executable}")

    cli_args = command_parts[:]
    if model:
        cli_args.extend(["--model", model])
    cli_args.extend([
        "--sandbox",
        "--print",
        prompt,
        "--print-timeout",
        f"{timeout_seconds}s",
    ])

    completed = subprocess.run(
        cli_args,
        capture_output=True,
        text=True,
        timeout=timeout_seconds + 10,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "agy CLI failed with exit status "
            f"{completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
        )

    output = completed.stdout.strip()
    if not output:
        raise RuntimeError("agy CLI returned empty output")
    return output


# ==========================================
# 프롬프트 렌더링 템플릿 정의
# ==========================================
def output_frame_template(run_date: str) -> str:
    return (
        f"# 📊 {run_date} (요일) 오늘의 뉴스 요약\n\n"
        "## 🌍 국제 및 외교\n"
        "* **주요 이슈 1:** 내용\n"
        "* **주요 이슈 2:** 내용\n\n"
        "## 🇰🇷 국내 정치 및 사회\n"
        "* **주요 이슈 1:** 내용\n"
        "* **주요 이슈 2:** 내용\n\n"
        "## 📈 경제 및 금융\n"
        "* **주요 이슈 1:** 내용\n"
        "* **주요 이슈 2:** 내용\n\n"
        "## 🌦️ 날씨 및 기타\n"
        "* **날씨 또는 기타:** 내용\n"
    )


def render_prompt(items: list[FeedItem], run_date: str, style: str = "default") -> str:
    lines = []
    for index, item in enumerate(items, start=1):
        lines.append(
            f"{index}. [{item.source}] {item.title}\n"
            f"   - published_at: {item.published_at or 'unknown'}\n"
            f"   - link: {item.link}"
        )
    headline_block = "\n".join(lines)

    if style == "compact":
        return (
            "뉴스요약해줘.\n\n"
            f"기준일: {run_date}\n"
            "역할: 한국어 아침 뉴스 브리핑 작성자\n"
            "입력은 RSS 헤드라인 목록뿐이다. 제공된 헤드라인 밖의 사실을 추가하지 말고, "
            "불확실하면 '헤드라인 기준 추정'이라고 명시하라.\n"
            "반드시 Markdown 본문만 출력하고 코드블록, YAML frontmatter, 메타데이터 섹션, "
            "원문 링크 목록, 체크포인트 섹션은 넣지 말라.\n"
            "아래 템플릿의 프레임만 그대로 따르고, 각 항목 내용만 채워라.\n"
            "출력 템플릿:\n"
            f"{output_frame_template(run_date)}\n"
            "규칙:\n"
            "- 각 항목은 `* **제목:** 내용` 형식으로 작성\n"
            "- 제목 1줄, 섹션 4개, 마지막 구분선과 생성 문구를 제외한 다른 블록은 만들지 말 것\n"
            "- 국제 및 외교: 2~4개 bullet\n"
            "- 국내 정치 및 사회: 2~4개 bullet\n"
            "- 경제 및 금융: 2~4개 bullet\n"
            "- 날씨 및 기타: 정확히 1개 bullet\n"
            "- 헤드라인을 묶어 자연스럽게 요약하되 과장하지 말 것\n"
            "- 날짜 제목의 요일은 한국어 한 글자 요일로 쓸 것\n\n"
            f"뉴스 목록:\n{headline_block}\n"
        )

    # default style
    return (
        f"기준일: {run_date}\n"
        "아래 뉴스 헤드라인만 근거로 한국어 아침 브리핑을 작성하라.\n"
        "형식:\n"
        "# {YYYY-MM-DD} 주요 뉴스 브리핑\n"
        "## 핵심 요약\n"
        "- 3~5개 불릿으로 오늘의 큰 흐름 정리\n"
        "## 분야별 정리\n"
        "### 국내\n"
        "### 글로벌\n"
        "### 금융/증시\n"
        "각 섹션은 핵심만 2~4개 불릿으로 정리\n"
        "## 체크포인트\n"
        "- 시장 관점에서 오늘 볼 포인트 3개\n"
        "## 원문 목록\n"
        "- 각 뉴스 제목을 출처와 함께 링크 마크다운으로 재정리\n"
        "주의:\n"
        "- 제공된 헤드라인 밖의 사실을 추가하지 말 것\n"
        "- 중복 이슈는 묶어서 요약할 것\n"
        "- 불확실하면 '헤드라인 기준 추정'이라고 표시할 것\n\n"
        f"뉴스 목록:\n{headline_block}"
    )


# ==========================================
# 4. 저장 레이어 정의 (Local & Obsidian)
# ==========================================
def build_archive_markdown(
    run_date: str,
    generated_at: str,
    model: str,
    feeds: list[str],
    summary_body: str,
    backend: str,
) -> str:
    feed_lines = "\n".join(f"- {feed}" for feed in feeds)
    return (
        f"<!-- generated_at: {generated_at} -->\n"
        f"<!-- model: {model} -->\n"
        f"<!-- backend: {backend} -->\n"
        f"<!-- source_type: rss-headlines -->\n\n"
        f"{summary_body.strip()}\n\n"
        "## 생성 정보\n"
        f"- 기준일: {run_date}\n"
        f"- 생성시각: {generated_at}\n"
        f"- 백엔드: `{backend}` (모델: `{model}`)\n"
        "- 입력 소스:\n"
        f"{feed_lines}\n"
    )


def build_obsidian_markdown(
    run_date: str,
    summary_body: str,
    backend: str,
) -> str:
    weekday = KOREAN_WEEKDAYS[datetime.fromisoformat(run_date).weekday()]
    normalized = summary_body.strip()
    expected_title_prefix = f"# 📊 {run_date} ({weekday}) 오늘의 뉴스 요약"

    if normalized.startswith("# "):
        lines = normalized.splitlines()
        lines[0] = expected_title_prefix
        normalized = "\n".join(lines).strip()
    else:
        normalized = f"{expected_title_prefix}\n\n{normalized}"

    generated_at = _current_news_datetime().isoformat(timespec="seconds")
    timezone_label = getattr(_news_timezone(), "key", str(_news_timezone()))
    return f"{normalized}\n\n---\n*Generated by {backend} at {generated_at} ({timezone_label})*\n"


@task(name="Fetch News Headlines", retries=2, retry_delay_seconds=30)
async def fetch_news_task(feed_urls: list[str], max_items: int) -> list[FeedItem]:
    logger = get_run_logger()
    collected: list[FeedItem] = []

    for feed_url in feed_urls:
        logger.info(f"Fetching RSS feed: {feed_url}")
        try:
            collected.extend(await fetch_feed_items(feed_url))
        except Exception as exc:
            logger.warning(f"Failed to fetch {feed_url}: {exc}")

    deduped: dict[str, FeedItem] = {}
    for item in collected:
        dedupe_key = item.link or item.title
        deduped.setdefault(dedupe_key, item)

    sorted_items = sorted(
        deduped.values(),
        key=lambda item: item.published_at,
        reverse=True,
    )
    limited = sorted_items[:max_items]
    logger.info(f"Collected {len(limited)} news items after deduplication")
    return limited


@task(name="Summarize News Multi-Backend", retries=2, retry_delay_seconds=60)
async def summarize_news_task(
    items: list[FeedItem],
    backend: str,
    model: str,
    run_date: str,
    style: str,
    codex_command: str,
    agy_command: str,
    cmux_command: str,
    cmux_workspace: str | None,
    cmux_surface: str | None,
    timeout_seconds: int,
) -> str:
    logger = get_run_logger()
    if not items:
        raise RuntimeError("No news items were collected from RSS feeds")

    prompt = render_prompt(items, run_date, style=style)
    logger.info(f"Sending {len(items)} headlines to {backend} (model={model or 'default'})")

    if backend == "openai":
        return await summarize_with_openai(model=model, prompt=prompt)
    elif backend == "gemini":
        return await summarize_with_gemini_api(model=model, prompt=prompt, timeout_seconds=timeout_seconds)
    elif backend == "cmux-gemini":
        return run_cmux_gemini_pane(
            prompt=prompt,
            workspace=cmux_workspace,
            surface=cmux_surface,
            cmux_command=cmux_command,
            timeout_seconds=timeout_seconds,
        )
    elif backend == "codex":
        return run_codex_cli(
            prompt=prompt,
            command=codex_command,
            model=model,
            timeout_seconds=timeout_seconds,
        )
    elif backend == "agy":
        return run_agy_cli(
            prompt=prompt,
            command=agy_command,
            model=model,
            timeout_seconds=timeout_seconds,
        )
    else:
        raise ValueError(f"Unsupported LLM backend: {backend}")


@task(name="Persist Summary (Local & Obsidian)")
def persist_summary_task(
    markdown_text: str,
    output_dir: str,
    obsidian_root: str | None,
    obsidian_subdir: str,
    run_date: str,
    backend: str,
    model: str,
    feeds: list[str],
) -> str:
    logger = get_run_logger()
    generated_at = _current_news_datetime().isoformat(timespec="seconds")

    # 1. Obsidian Vault로 저장
    if obsidian_root:
        root = Path(obsidian_root)
        if not root.is_absolute():
            root = Path.cwd() / root
        target_dir = root / obsidian_subdir
        target_dir.mkdir(parents=True, exist_ok=True)

        full_markdown = build_obsidian_markdown(
            run_date=run_date,
            summary_body=markdown_text,
            backend=backend,
        )
        target_path = target_dir / f"{run_date}.md"
        target_path.write_text(full_markdown, encoding="utf-8")
        logger.info(f"Saved Obsidian markdown briefing to {target_path}")
        return str(target_path)

    # 2. 로컬 일반 아카이브 저장
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    full_markdown = build_archive_markdown(
        run_date=run_date,
        generated_at=generated_at,
        model=model,
        feeds=feeds,
        summary_body=markdown_text,
        backend=backend,
    )
    target_path = target_dir / f"{run_date}.md"
    target_path.write_text(full_markdown, encoding="utf-8")
    logger.info(f"Saved archive markdown briefing to {target_path}")
    return str(target_path)


@flow(name="Daily-News-Summary-Flow")
async def daily_news_summary_flow(
    feed_urls: list[str] | None = None,
    backend: str | None = None,
    model: str | None = None,
    output_dir: str | None = None,
    obsidian_root: str | None = None,
    obsidian_subdir: str | None = None,
    max_items: int | None = None,
    prompt_style: str | None = None,
    gemini_command: str | None = None,
    codex_command: str | None = None,
    agy_command: str | None = None,
    cmux_command: str | None = None,
    cmux_workspace: str | None = None,
    cmux_surface: str | None = None,
    approval_mode: str | None = None,
    timeout_seconds: int | None = None,
):
    logger = get_run_logger()
    resolved_backend = (backend or str(_config_value("NEWS_LLM_BACKEND", DEFAULT_BACKEND))).strip().lower()
    resolved_feeds = feed_urls or _config_list("NEWS_RSS_FEEDS", DEFAULT_FEEDS)
    resolved_output_dir = output_dir or str(_config_value("NEWS_SUMMARY_DIR", DEFAULT_OUTPUT_DIR))
    summary_setting = NEWS_CONFIGS.get("NEWS_SUMMARY_SETTING") or {}
    resolved_obsidian_root = obsidian_root or os.getenv("OBSIDIAN_VAULT_DIR") or summary_setting.get("ROOT")
    resolved_obsidian_subdir = (
        obsidian_subdir
        or os.getenv("NEWS_OBSIDIAN_SUBDIR")
        or summary_setting.get("SUBDIR")
        or DEFAULT_OBSIDIAN_SUBDIR
    )
    resolved_max_items = max_items or int(_config_value("NEWS_MAX_ITEMS", DEFAULT_MAX_ITEMS))
    resolved_style = prompt_style or str(
        _config_value("NEWS_PROMPT_STYLE", "default" if resolved_backend == "openai" else "compact")
    )

    if resolved_backend == "openai":
        val = model or _config_value("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
        resolved_model = str(val) if val is not None else ""
    elif resolved_backend in {"gemini", "cmux-gemini"}:
        val = model or _config_value("GEMINI_MODEL", DEFAULT_GEMINI_MODEL)
        resolved_model = str(val) if val is not None else ""
    elif resolved_backend == "codex":
        val = model or _config_value("CODEX_MODEL", DEFAULT_CODEX_MODEL)
        resolved_model = str(val) if val is not None else ""
    elif resolved_backend == "agy":
        val = model or _config_value("AGY_MODEL", DEFAULT_AGY_MODEL)
        resolved_model = str(val) if val is not None else ""
    else:
        resolved_model = str(model) if model is not None else ""

    resolved_codex_cmd = codex_command or str(_config_value("CODEX_CLI_COMMAND", DEFAULT_CODEX_COMMAND))
    resolved_agy_cmd = agy_command or str(_config_value("AGY_CLI_COMMAND", DEFAULT_AGY_COMMAND))
    resolved_cmux_cmd = cmux_command or str(_config_value("CMUX_COMMAND", DEFAULT_CMUX_COMMAND))
    resolved_cmux_workspace = cmux_workspace or os.getenv("CMUX_GEMINI_WORKSPACE") or NEWS_CONFIGS.get("CMUX_GEMINI_WORKSPACE")
    resolved_cmux_surface = cmux_surface or os.getenv("CMUX_GEMINI_SURFACE") or NEWS_CONFIGS.get("CMUX_GEMINI_SURFACE")
    resolved_timeout = timeout_seconds or int(_config_value("TIMEOUT_SECONDS", _config_value("GEMINI_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)))

    run_date = arrow.now(tz=_news_timezone().key).format("YYYY-MM-DD")

    logger.info(
        "Starting Daily News Summary Flow "
        f"(backend={resolved_backend}, model={resolved_model or 'default'}, "
        f"style={resolved_style}, feeds={len(resolved_feeds)}, max_items={resolved_max_items})"
    )

    items = await fetch_news_task(resolved_feeds, resolved_max_items)
    summary = await summarize_news_task(
        items=items,
        backend=resolved_backend,
        model=resolved_model,
        run_date=run_date,
        style=resolved_style,
        codex_command=resolved_codex_cmd,
        agy_command=resolved_agy_cmd,
        cmux_command=resolved_cmux_cmd,
        cmux_workspace=resolved_cmux_workspace,
        cmux_surface=resolved_cmux_surface,
        timeout_seconds=resolved_timeout,
    )

    return persist_summary_task(
        markdown_text=summary,
        output_dir=resolved_output_dir,
        obsidian_root=resolved_obsidian_root,
        obsidian_subdir=resolved_obsidian_subdir,
        run_date=run_date,
        backend=resolved_backend,
        model=resolved_model,
        feeds=resolved_feeds,
    )


if __name__ == "__main__":
    result = asyncio.run(daily_news_summary_flow())
    print(result)
