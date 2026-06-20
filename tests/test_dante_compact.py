import json
from datetime import date

import pandas as pd

from follow_dante_reading import compact as compact_module
from follow_dante_reading.compact import DanteHistoryCompactor


def _append_jsonl(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


def test_compact_history_joins_message_signal_decision_with_kospi(tmp_path, monkeypatch):
    base_dir = tmp_path / "follow_dante_reading"
    compact_date = date(2026, 6, 20)

    _append_jsonl(
        base_dir / "telegram_messages.jsonl",
        {
            "chat_id": 3956165696,
            "chat_title": "차트마스터 코스피방",
            "message_id": 7,
            "posted_at": "2026-06-20T09:10:00+09:00",
            "raw_text": "삼성전자 단타 관찰",
            "text": "삼성전자 단타 관찰",
        },
    )
    _append_jsonl(
        base_dir / "reading_signals.jsonl",
        {
            "chat_id": 3956165696,
            "chat_title": "차트마스터 코스피방",
            "message_id": 7,
            "posted_at": "2026-06-20T09:10:00+09:00",
            "company_name": "삼성전자",
            "action": "buy_candidate",
            "trade_style": "daytrade",
            "confidence": 0.91,
            "summary": "삼성전자 단타 후보",
            "rationale_text": "분봉 흐름 확인",
            "raw_text": "삼성전자 단타 관찰",
        },
    )

    def fake_kospi_provider(target_date):
        assert target_date == compact_date
        return {
            "symbol": "KOSPI",
            "date": "2026-06-20",
            "source": "test",
            "status": "ok",
            "open": 3000.0,
            "high": 3010.0,
            "low": 2990.0,
            "close": 3005.0,
            "volume": 123,
            "change": 5.0,
            "change_pct": 0.0017,
        }

    journal_path = base_dir / "investment_journal.jsonl"
    _append_jsonl(
        journal_path,
        {
            "chat_id": 3956165696,
            "chat_title": "차트마스터 코스피방",
            "message_id": 7,
            "posted_at": "2026-06-20T09:10:00+09:00",
            "recorded_at": "2026-06-20T09:10:05+09:00",
            "company": "삼성전자",
            "action": "buy_candidate",
            "trade_style": "daytrade",
            "confidence": 0.91,
            "decision": "auto_buy_attempt",
            "reason": "auto_gate_passed",
            "summary": "삼성전자 단타 후보",
        },
    )

    compactor = DanteHistoryCompactor(
        base_dir=base_dir,
        output_dir=tmp_path / "compact",
        kospi_provider=fake_kospi_provider,
    )

    result = compactor.compact(compact_date)

    payload = json.loads(result.json_path.read_text(encoding="utf-8"))
    assert payload["kospi"]["close"] == 3005.0
    assert payload["records"][0]["decision"] == "auto_buy_attempt"
    assert payload["records"][0]["company"] == "삼성전자"
    markdown = result.markdown_path.read_text(encoding="utf-8")
    assert "차트마스터 코스피방" in markdown
    assert "KOSPI Context" in markdown


def test_fetch_kospi_snapshot_falls_back_to_fdr(monkeypatch):
    monkeypatch.setattr(
        compact_module.pykrx_stock,
        "get_index_ohlcv_by_date",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyError("지수명")),
    )
    frame = pd.DataFrame(
        [
            {"Open": 3000.0, "High": 3010.0, "Low": 2990.0, "Close": 3001.0, "Volume": 100},
            {"Open": 3001.0, "High": 3020.0, "Low": 3000.0, "Close": 3011.0, "Volume": 200},
        ],
        index=pd.to_datetime(["2026-06-18", "2026-06-19"]),
    )
    monkeypatch.setattr(compact_module.fdr, "DataReader", lambda *args, **kwargs: frame)

    snapshot = compact_module.fetch_kospi_snapshot(date(2026, 6, 19))

    assert snapshot["status"] == "ok"
    assert snapshot["source"] == "fdr"
    assert snapshot["close"] == 3011
    assert snapshot["change"] == 10
