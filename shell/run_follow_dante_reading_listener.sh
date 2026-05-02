#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHAT_REF="${1:-dante}"
SESSION_NAME="${2:-follow_dante_reading}"
NOTIFY_FLAG="${NOTIFY_FLAG:-true}"
DOWNLOAD_MEDIA_FLAG="${DOWNLOAD_MEDIA_FLAG:-true}"
RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-5}"

cd "$ROOT_DIR"

mkdir -p logs/follow_dante_reading

TMUX_CMD="cd \"$ROOT_DIR\" && PYTHONPATH=src uv run python src/follow_dante_reading/orchestrator.py serve --chat \"$CHAT_REF\" --notify \"$NOTIFY_FLAG\" --download_media \"$DOWNLOAD_MEDIA_FLAG\" --retry_delay_seconds \"$RETRY_DELAY_SECONDS\""

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "tmux session '$SESSION_NAME' already exists"
  echo "attach with: tmux attach -t $SESSION_NAME"
  exit 0
fi

tmux new-session -d -s "$SESSION_NAME" "$TMUX_CMD"

echo "started tmux session: $SESSION_NAME"
echo "chat reference: $CHAT_REF"
echo "attach with: tmux attach -t $SESSION_NAME"
