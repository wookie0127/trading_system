import asyncio
import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from news.daily_news_orchestrator import (  # noqa: E402
    DEFAULT_BACKEND,
    DEFAULT_FEEDS,
    DEFAULT_MAX_ITEMS,
    DEFAULT_OBSIDIAN_SUBDIR,
    DEFAULT_OUTPUT_DIR,
    NEWS_CONFIGS,
    _config_list,
    _config_value,
    _current_news_datetime,
    build_archive_markdown,
    build_obsidian_markdown,
    fetch_feed_items,
    render_prompt,
    run_agy_cli,
    run_codex_cli,
    summarize_with_gemini_api,
    summarize_with_openai,
)


async def collect_items(feed_urls: list[str], max_items: int):
    collected = []
    for feed_url in feed_urls:
        collected.extend(await fetch_feed_items(feed_url))

    deduped = {}
    for item in collected:
        deduped.setdefault(item.link or item.title, item)

    return sorted(deduped.values(), key=lambda item: item.published_at, reverse=True)[
        :max_items
    ]


async def summarize(backend: str, model: str, prompt: str, timeout_seconds: int) -> str:
    if backend == "openai":
        return await summarize_with_openai(model=model, prompt=prompt)
    if backend == "gemini":
        return await summarize_with_gemini_api(
            model=model, prompt=prompt, timeout_seconds=timeout_seconds
        )
    if backend == "codex":
        return run_codex_cli(
            prompt,
            str(_config_value("CODEX_CLI_COMMAND", "codex")),
            model,
            timeout_seconds,
        )
    if backend == "agy":
        return run_agy_cli(
            prompt, str(_config_value("AGY_CLI_COMMAND", "agy")), model, timeout_seconds
        )
    raise ValueError(f"Unsupported direct-run backend: {backend}")


async def main() -> str:
    backend = str(_config_value("NEWS_LLM_BACKEND", DEFAULT_BACKEND)).strip().lower()
    feeds = _config_list("NEWS_RSS_FEEDS", DEFAULT_FEEDS)
    max_items = int(_config_value("NEWS_MAX_ITEMS", DEFAULT_MAX_ITEMS))
    timeout_seconds = int(
        _config_value("TIMEOUT_SECONDS", _config_value("GEMINI_TIMEOUT_SECONDS", 180))
    )
    run_date = _current_news_datetime().date().isoformat()
    style = str(_config_value("NEWS_PROMPT_STYLE", "compact"))

    if backend == "openai":
        model = str(_config_value("OPENAI_MODEL", "gpt-5-mini"))
    elif backend == "gemini":
        model = str(_config_value("GEMINI_MODEL", "gemini-2.5-flash"))
    elif backend == "codex":
        model = str(_config_value("CODEX_MODEL", ""))
    elif backend == "agy":
        model = str(_config_value("AGY_MODEL", "") or "")
    else:
        model = ""

    items = await collect_items(feeds, max_items)
    if not items:
        raise RuntimeError("No news items were collected from RSS feeds")

    prompt = render_prompt(items, run_date, style=style)
    summary = await summarize(backend, model, prompt, timeout_seconds)

    summary_setting = NEWS_CONFIGS.get("NEWS_SUMMARY_SETTING") or {}
    obsidian_root = summary_setting.get("ROOT")
    obsidian_subdir = summary_setting.get("SUBDIR") or DEFAULT_OBSIDIAN_SUBDIR

    if obsidian_root:
        target_dir = Path(obsidian_root) / obsidian_subdir
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{run_date}.md"
        target_path.write_text(
            build_obsidian_markdown(run_date, summary, backend), encoding="utf-8"
        )
        return str(target_path)

    output_dir = Path(str(_config_value("NEWS_SUMMARY_DIR", DEFAULT_OUTPUT_DIR)))
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = _current_news_datetime().isoformat(timespec="seconds")
    target_path = output_dir / f"{run_date}.md"
    target_path.write_text(
        build_archive_markdown(run_date, generated_at, model, feeds, summary, backend),
        encoding="utf-8",
    )
    return str(target_path)


if __name__ == "__main__":
    print(asyncio.run(main()))
