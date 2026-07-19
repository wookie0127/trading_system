#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export PYTHONPATH="$ROOT_DIR/src"
export PATH="/opt/zerobrew/bin:$HOME/.cargo/bin:$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"
export NEWS_LLM_BACKEND="${NEWS_LLM_BACKEND:-agy}"
export AGY_CLI_COMMAND="${AGY_CLI_COMMAND:-/Users/giwooklee/.local/bin/agy}"
export OBSIDIAN_VAULT_DIR="${OBSIDIAN_VAULT_DIR:-/Users/giwooklee/Documents/Obsidian Vault/TradingSystem}"
export NEWS_OBSIDIAN_SUBDIR="${NEWS_OBSIDIAN_SUBDIR:-뉴스요약}"

mkdir -p logs
uv run python src/news/daily_news_orchestrator.py
