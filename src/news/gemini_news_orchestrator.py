"""Gemini News Orchestrator (Consolidated / Deprecated Alias).

This module is retained for backward compatibility. It delegates directly to
the unified `daily_news_summary_flow` in `news.daily_news_orchestrator`.
"""

import asyncio
import sys
from pathlib import Path
from prefect import flow

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from news.daily_news_orchestrator import daily_news_summary_flow  # noqa: E402


@flow(name="Daily-News-Summary-Gemini-Flow")
async def daily_news_summary_gemini_flow(
    feed_urls: list[str] | None = None,
    obsidian_root: str | None = None,
    obsidian_subdir: str | None = None,
    max_items: int | None = None,
    gemini_model: str | None = None,
    timeout_seconds: int | None = None,
):
    """Delegates to daily_news_summary_flow with backend='gemini'."""
    return await daily_news_summary_flow(
        feed_urls=feed_urls,
        backend="gemini",
        model=gemini_model,
        obsidian_root=obsidian_root,
        obsidian_subdir=obsidian_subdir,
        max_items=max_items,
        timeout_seconds=timeout_seconds,
    )


if __name__ == "__main__":
    result = asyncio.run(daily_news_summary_gemini_flow())
    print(result)
