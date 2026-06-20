from __future__ import annotations

import argparse

from follow_dante_reading.compact import DanteHistoryCompactor


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact Dante Telegram, signal, decision, and KOSPI context logs.")
    parser.add_argument("--date", help="Compact target date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--output-dir", help="Directory for compact JSON/Markdown outputs.")
    args = parser.parse_args()

    result = DanteHistoryCompactor(output_dir=args.output_dir).compact(args.date)
    print(f"markdown={result.markdown_path}")
    print(f"json={result.json_path}")
    print(f"messages={result.message_count} signals={result.signal_count} journal={result.journal_count}")


if __name__ == "__main__":
    main()
