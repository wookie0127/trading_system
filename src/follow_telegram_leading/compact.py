from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import FinanceDataReader as fdr
from loguru import logger
from pykrx import stock as pykrx_stock

from follow_telegram_leading.store import ReadingStore

MARKET_TIMEZONE = ZoneInfo("Asia/Seoul")


@dataclass(slots=True)
class CompactResult:
    compact_date: date
    json_path: Path
    markdown_path: Path
    message_count: int
    signal_count: int
    journal_count: int


class TleadingHistoryCompactor:
    def __init__(
        self,
        base_dir: str | Path | None = None,
        output_dir: str | Path | None = None,
        kospi_provider=None,
    ):
        self.store = ReadingStore(base_dir=base_dir)
        self.output_dir = (
            Path(output_dir) if output_dir else self.store.base_dir / "compact"
        )
        self.kospi_provider = kospi_provider or fetch_kospi_snapshot

    def compact(self, compact_date: date | str | None = None) -> CompactResult:
        run_date = _coerce_date(compact_date) if compact_date else date.today()
        messages = _filter_records_by_date(
            _load_jsonl(self.store.messages_path),
            run_date,
            "posted_at",
        )
        signals = _filter_records_by_date(
            _load_jsonl(self.store.signals_path),
            run_date,
            "posted_at",
        )
        journal = _filter_records_by_date(
            _load_jsonl(self.store.base_dir / "investment_journal.jsonl"),
            run_date,
            "posted_at",
        )
        compact_records = _build_compact_records(messages, signals, journal)
        kospi_snapshot = self.kospi_provider(run_date)

        payload = {
            "date": run_date.isoformat(),
            "generated_at": datetime.now().isoformat(),
            "kospi": kospi_snapshot,
            "counts": {
                "messages": len(messages),
                "signals": len(signals),
                "journal": len(journal),
                "compact_records": len(compact_records),
            },
            "records": compact_records,
        }

        self.output_dir.mkdir(parents=True, exist_ok=True)
        json_path = self.output_dir / f"{run_date.isoformat()}.json"
        markdown_path = self.output_dir / f"{run_date.isoformat()}.md"
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        markdown_path.write_text(_render_markdown(payload), encoding="utf-8")

        logger.info(
            "Wrote compact Tleading history for {}: {} records -> {}",
            run_date,
            len(compact_records),
            markdown_path,
        )
        return CompactResult(
            compact_date=run_date,
            json_path=json_path,
            markdown_path=markdown_path,
            message_count=len(messages),
            signal_count=len(signals),
            journal_count=len(journal),
        )


def fetch_kospi_snapshot(target_date: date) -> dict[str, Any]:
    start = (target_date - timedelta(days=7)).strftime("%Y%m%d")
    end = target_date.strftime("%Y%m%d")
    try:
        frame = pykrx_stock.get_index_ohlcv_by_date(start, end, "1001")
    except Exception as exc:
        logger.warning("Failed to fetch KOSPI snapshot for {}: {}", target_date, exc)
        return _fetch_kospi_snapshot_from_fdr(target_date, pykrx_error=str(exc))

    if frame is None or frame.empty:
        return {
            "symbol": "KOSPI",
            "date": target_date.isoformat(),
            "source": "pykrx",
            "status": "empty",
        }

    row = frame.iloc[-1]
    index_date = frame.index[-1]
    if hasattr(index_date, "date"):
        index_date = index_date.date()

    close = _to_number(row.get("종가"))
    open_ = _to_number(row.get("시가"))
    previous_close = None
    if len(frame) >= 2:
        previous_close = _to_number(frame.iloc[-2].get("종가"))
    change = (
        close - previous_close
        if close is not None and previous_close is not None
        else None
    )
    change_pct = (
        change / previous_close if change is not None and previous_close else None
    )

    return {
        "symbol": "KOSPI",
        "date": index_date.isoformat()
        if hasattr(index_date, "isoformat")
        else str(index_date),
        "source": "pykrx",
        "status": "ok",
        "open": open_,
        "high": _to_number(row.get("고가")),
        "low": _to_number(row.get("저가")),
        "close": close,
        "volume": _to_number(row.get("거래량")),
        "change": change,
        "change_pct": change_pct,
    }


def _fetch_kospi_snapshot_from_fdr(
    target_date: date, pykrx_error: str | None = None
) -> dict[str, Any]:
    start = target_date - timedelta(days=7)
    try:
        frame = fdr.DataReader("KS11", start.isoformat(), target_date.isoformat())
    except Exception as exc:
        logger.warning(
            "Failed to fetch KOSPI snapshot from FDR for {}: {}", target_date, exc
        )
        return {
            "symbol": "KOSPI",
            "date": target_date.isoformat(),
            "source": "pykrx,fdr",
            "status": "unavailable",
            "error": str(exc),
            "pykrx_error": pykrx_error,
        }

    if frame is None or frame.empty:
        return {
            "symbol": "KOSPI",
            "date": target_date.isoformat(),
            "source": "fdr",
            "status": "empty",
            "pykrx_error": pykrx_error,
        }

    row = frame.iloc[-1]
    index_date = frame.index[-1]
    if hasattr(index_date, "date"):
        index_date = index_date.date()

    close = _to_number(row.get("Close"))
    open_ = _to_number(row.get("Open"))
    previous_close = None
    if len(frame) >= 2:
        previous_close = _to_number(frame.iloc[-2].get("Close"))
    change = (
        close - previous_close
        if close is not None and previous_close is not None
        else _to_number(row.get("Change"))
    )
    change_pct = (
        change / previous_close if change is not None and previous_close else None
    )

    return {
        "symbol": "KOSPI",
        "date": index_date.isoformat()
        if hasattr(index_date, "isoformat")
        else str(index_date),
        "source": "fdr",
        "status": "ok",
        "open": open_,
        "high": _to_number(row.get("High")),
        "low": _to_number(row.get("Low")),
        "close": close,
        "volume": _to_number(row.get("Volume")),
        "change": change,
        "change_pct": change_pct,
        "pykrx_error": pykrx_error,
    }


def _build_compact_records(
    messages: list[dict],
    signals: list[dict],
    journal: list[dict],
) -> list[dict]:
    messages_by_key = {_record_key(record): record for record in messages}
    signals_by_key = {_record_key(record): record for record in signals}
    journal_by_key: dict[tuple[str, str], list[dict]] = {}
    for record in journal:
        journal_by_key.setdefault(_record_key(record), []).append(record)

    keys = sorted(
        set(messages_by_key) | set(signals_by_key) | set(journal_by_key),
        key=lambda key: _sort_time(
            messages_by_key.get(key),
            signals_by_key.get(key),
            journal_by_key.get(key, [{}])[0],
        ),
    )

    compact_records = []
    for key in keys:
        message = messages_by_key.get(key, {})
        signal = signals_by_key.get(key, {})
        decisions = journal_by_key.get(key, [])
        latest_decision = decisions[-1] if decisions else {}
        compact_records.append(
            {
                "chat_id": _first_present(message, signal, latest_decision, "chat_id"),
                "chat_title": _first_present(
                    message, signal, latest_decision, "chat_title"
                ),
                "strategy_name": _first_present(
                    signal, latest_decision, "strategy_name"
                ),
                "message_id": _first_present(
                    message, signal, latest_decision, "message_id"
                ),
                "posted_at": _first_present(
                    message, signal, latest_decision, "posted_at"
                ),
                "company": _first_present_key(
                    (signal, latest_decision), ("company_name", "company")
                ),
                "action": _first_present(signal, latest_decision, "action"),
                "trade_style": _first_present(signal, latest_decision, "trade_style"),
                "confidence": _first_present(signal, latest_decision, "confidence"),
                "decision": latest_decision.get("decision"),
                "reason": latest_decision.get("reason"),
                "summary": _first_present(signal, latest_decision, "summary"),
                "rationale_text": _first_present(
                    signal, latest_decision, "rationale_text"
                ),
                "raw_text": message.get("raw_text")
                or signal.get("raw_text")
                or latest_decision.get("raw_text"),
                "decisions": decisions,
            }
        )
    return compact_records


def _render_markdown(payload: dict) -> str:
    kospi = payload["kospi"]
    counts = payload["counts"]
    lines = [
        "---",
        "type: tleading-compact-history",
        f"date: {payload['date']}",
        "system: tleading-llm-autonomous",
        "---",
        "",
        f"# Tleading Compact History - {payload['date']}",
        "",
        "## KOSPI Context",
        "",
        _format_kospi(kospi),
        "",
        "## Counts",
        "",
        f"- Messages: {counts['messages']}",
        f"- Signals: {counts['signals']}",
        f"- Decisions: {counts['journal']}",
        "",
        "## Decision Timeline",
        "",
        "| 시간 | 방 | 채널전략 | 종목 | 액션 | 스타일 | 신뢰도 | 판단 | 사유 | 요약 |",
        "|---|---|---|---|---|---|---:|---|---|---|",
    ]
    for record in payload["records"]:
        lines.append(
            "| "
            f"{_hhmm(record.get('posted_at'))} | "
            f"{_md(record.get('chat_title') or record.get('chat_id') or '-')} | "
            f"{_md(record.get('strategy_name') or '-')} | "
            f"{_md(record.get('company') or '-')} | "
            f"{_md(record.get('action') or '-')} | "
            f"{_md(record.get('trade_style') or '-')} | "
            f"{float(record.get('confidence') or 0):.2f} | "
            f"{_md(record.get('decision') or '-')} | "
            f"{_md(record.get('reason') or '-')} | "
            f"{_md(record.get('summary') or '-')} |"
        )

    lines.extend(["", "## Raw Message Evidence", ""])
    for record in payload["records"]:
        raw_text = (record.get("raw_text") or "").strip()
        if not raw_text:
            continue
        lines.extend(
            [
                f"### {_hhmm(record.get('posted_at'))} {_md(record.get('chat_title') or '-')}",
                "",
                f"- Message ID: `{record.get('message_id')}`",
                f"- Decision: `{record.get('decision') or '-'}` / Reason: `{record.get('reason') or '-'}`",
                "",
                "```text",
                raw_text[:2000],
                "```",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _format_kospi(kospi: dict) -> str:
    if kospi.get("status") != "ok":
        return f"- KOSPI: unavailable ({kospi.get('status')}, source={kospi.get('source')})"

    change = kospi.get("change")
    change_pct = kospi.get("change_pct")
    change_text = "-"
    if change is not None and change_pct is not None:
        change_text = f"{change:+.2f} ({change_pct:+.2%})"
    return (
        f"- Date: {kospi.get('date')}\n"
        f"- Open/High/Low/Close: "
        f"{_fmt_num(kospi.get('open'))} / {_fmt_num(kospi.get('high'))} / "
        f"{_fmt_num(kospi.get('low'))} / {_fmt_num(kospi.get('close'))}\n"
        f"- Change: {change_text}\n"
        f"- Volume: {_fmt_num(kospi.get('volume'), decimals=0)}"
    )


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                logger.warning("Skipping invalid JSONL line in {}", path)
    return records


def _filter_records_by_date(
    records: list[dict], target_date: date, field: str
) -> list[dict]:
    return [
        record for record in records if _record_date(record.get(field)) == target_date
    ]


def _record_date(raw_value) -> date | None:
    if not raw_value:
        return None
    try:
        value = datetime.fromisoformat(str(raw_value))
    except ValueError:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=MARKET_TIMEZONE)
    return value.astimezone(MARKET_TIMEZONE).date()


def _record_key(record: dict) -> tuple[str, str]:
    return (str(record.get("chat_id") or ""), str(record.get("message_id") or ""))


def _sort_time(*records: dict) -> str:
    for record in records:
        value = record.get("posted_at") or record.get("recorded_at")
        if value:
            return str(value)
    return ""


def _first_present(*items):
    *records, key = items
    return _first_present_key(records, (key,))


def _first_present_key(records, keys):
    for record in records:
        if not isinstance(record, dict):
            continue
        for key in keys:
            value = record.get(key)
            if value not in (None, ""):
                return value
    return None


def _coerce_date(raw_value: date | str) -> date:
    if isinstance(raw_value, date):
        return raw_value
    return datetime.strptime(str(raw_value), "%Y-%m-%d").date()


def _to_number(value) -> float | int | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _fmt_num(value, decimals: int = 2) -> str:
    if value is None:
        return "-"
    if decimals == 0:
        return f"{int(value):,}"
    return f"{float(value):,.{decimals}f}"


def _hhmm(raw_value) -> str:
    if not raw_value:
        return "-"
    try:
        value = datetime.fromisoformat(str(raw_value))
    except ValueError:
        return "-"
    if value.tzinfo is None:
        value = value.replace(tzinfo=MARKET_TIMEZONE)
    return value.astimezone(MARKET_TIMEZONE).strftime("%H:%M")


def _md(value) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")[:180]
