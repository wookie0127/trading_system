import asyncio
import email.utils
import os
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx
import yaml
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

DEFAULT_FEEDS = [
    "https://news.google.com/rss?hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/headlines/section/topic/BUSINESS?hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/headlines/section/topic/WORLD?hl=ko&gl=KR&ceid=KR:ko",
]
DEFAULT_MODEL = "gpt-5-mini"
DEFAULT_OUTPUT_DIR = Path("data/news_summaries")
DEFAULT_MAX_ITEMS = 20

KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(KEY_PATH)
load_dotenv()

CONFIG_PATH = SRC_DIR / "config.yaml"
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}
        for key, value in config.items():
            os.environ.setdefault(key, str(value))


@dataclass
class FeedItem:
    title: str
    link: str
    published_at: str
    source: str


def _env_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


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


def render_prompt(items: list[FeedItem], run_date: str) -> str:
    lines = []
    for index, item in enumerate(items, start=1):
        lines.append(
            f"{index}. [{item.source}] {item.title}\n"
            f"   - published_at: {item.published_at or 'unknown'}\n"
            f"   - link: {item.link}"
        )

    headline_block = "\n".join(lines)
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


def build_archive_markdown(
    run_date: str,
    generated_at: str,
    model: str,
    feeds: list[str],
    summary_body: str,
) -> str:
    feed_lines = "\n".join(f"- {feed}" for feed in feeds)
    return (
        f"<!-- generated_at: {generated_at} -->\n"
        f"<!-- model: {model} -->\n"
        "<!-- source_type: rss-headlines -->\n\n"
        f"{summary_body.strip()}\n\n"
        "## 생성 정보\n"
        f"- 기준일: {run_date}\n"
        f"- 생성시각: {generated_at}\n"
        f"- 모델: `{model}`\n"
        "- 입력 소스:\n"
        f"{feed_lines}\n"
    )


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


@task(name="Summarize News", retries=2, retry_delay_seconds=60)
async def summarize_news_task(items: list[FeedItem], model: str, run_date: str) -> str:
    logger = get_run_logger()
    if not items:
        raise RuntimeError("No news items were collected from RSS feeds")

    prompt = render_prompt(items, run_date)
    logger.info(f"Sending {len(items)} headlines to OpenAI model {model}")
    return await summarize_with_openai(model=model, prompt=prompt)


@task(name="Persist Markdown Summary")
def persist_summary_task(markdown_text: str, output_dir: str, run_date: str, model: str, feeds: list[str]) -> str:
    logger = get_run_logger()
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    generated_at = datetime.now().astimezone().isoformat(timespec="seconds")
    full_markdown = build_archive_markdown(
        run_date=run_date,
        generated_at=generated_at,
        model=model,
        feeds=feeds,
        summary_body=markdown_text,
    )

    target_path = target_dir / f"{run_date}.md"
    target_path.write_text(full_markdown, encoding="utf-8")
    logger.info(f"Saved markdown briefing to {target_path}")
    return str(target_path)


@flow(name="Daily-News-Summary-Flow")
async def daily_news_summary_flow(
    feed_urls: list[str] | None = None,
    model: str | None = None,
    output_dir: str | None = None,
    max_items: int | None = None,
):
    logger = get_run_logger()
    resolved_feeds = feed_urls or _env_list("NEWS_RSS_FEEDS", DEFAULT_FEEDS)
    resolved_model = model or os.getenv("OPENAI_MODEL", DEFAULT_MODEL)
    resolved_output_dir = output_dir or os.getenv("NEWS_SUMMARY_DIR", str(DEFAULT_OUTPUT_DIR))
    resolved_max_items = max_items or int(os.getenv("NEWS_MAX_ITEMS", str(DEFAULT_MAX_ITEMS)))
    run_date = datetime.now().astimezone().date().isoformat()

    logger.info(
        "Starting daily news summary flow "
        f"(feeds={len(resolved_feeds)}, model={resolved_model}, max_items={resolved_max_items})"
    )

    items = await fetch_news_task(resolved_feeds, resolved_max_items)
    summary = await summarize_news_task(items, resolved_model, run_date)
    return persist_summary_task(summary, resolved_output_dir, run_date, resolved_model, resolved_feeds)


if __name__ == "__main__":
    result = asyncio.run(daily_news_summary_flow())
    print(result)
