from datetime import datetime, timezone

from follow_telegram_leading import parser
from follow_telegram_leading.parser import parse_reading_signal, resolve_channel_strategy_name
from follow_telegram_leading.signal_schema import ReadingMessage


def _message(*, chat_id: int | None, chat_title: str | None, text: str) -> ReadingMessage:
    return ReadingMessage(
        source="telegram:dante",
        chat_id=chat_id,
        chat_title=chat_title,
        message_id=1,
        posted_at=datetime(2026, 6, 30, 9, 0, tzinfo=timezone.utc),
        text=text,
        raw_text=text,
        has_media=False,
    )


def test_resolve_channel_strategy_name_for_known_telegram_channels():
    assert (
        resolve_channel_strategy_name(
            _message(chat_id=3875818348, chat_title="[N]카페 정보공유 소통채널", text="삼성전자 3% 손절 매수")
        )
        == "cafe_share"
    )
    assert (
        resolve_channel_strategy_name(
            _message(chat_id=3956165696, chat_title="차트마스터 코스피방", text="코스피 2계약 매도진입")
        )
        == "chart_master_kospi"
    )


def test_parse_reading_signal_persists_channel_strategy_name():
    signal = parse_reading_signal(
        _message(chat_id=3875818348, chat_title="[N]카페 정보공유 소통채널", text="삼성전자\n3% 손절 매수 가능")
    )

    assert signal is not None
    assert signal.strategy_name == "cafe_share"
    assert signal.company_name == "삼성전자"
    assert signal.action == "buy_candidate"


def test_parse_chart_master_kospi_signal_as_futures_action():
    buy_signal = parse_reading_signal(
        _message(chat_id=3956165696, chat_title="차트마스터 코스피방", text="코스피 2계약 매수진입")
    )
    assert buy_signal is not None
    assert buy_signal.strategy_name == "chart_master_kospi"
    assert buy_signal.company_name is None
    assert buy_signal.action == "buy_candidate"

    sell_signal = parse_reading_signal(
        _message(chat_id=3956165696, chat_title="차트마스터 코스피방", text="청산하겠습니다")
    )
    assert sell_signal is not None
    assert sell_signal.strategy_name == "chart_master_kospi"
    assert sell_signal.action == "sell"


def test_gemini_backend_uses_api_key_without_cli(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": (
                                        '{"summary":"삼성전자 매수 후보","company_name":"삼성전자",'
                                        '"action":"buy_candidate","trade_style":"daytrade",'
                                        '"stop_loss_pct":3.0,"entry_hint":"current_price",'
                                        '"confidence":0.9,"rationale_text":"3% 손절 매수 가능"}'
                                    )
                                }
                            ]
                        }
                    }
                ]
            }

    def fake_post(endpoint, *, params, json, timeout):
        captured["endpoint"] = endpoint
        captured["params"] = params
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setenv("DANTE_LLM_BACKEND", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(parser.httpx, "post", fake_post)

    signal = parser.parse_reading_signal_with_llm(
        _message(chat_id=3875818348, chat_title="[N]카페 정보공유 소통채널", text="삼성전자\n3% 손절 매수 가능")
    )

    assert signal is not None
    assert signal.company_name == "삼성전자"
    assert signal.action == "buy_candidate"
    assert signal.strategy_name == "cafe_share"
    assert captured["params"] == {"key": "test-key"}
    assert "generateContent" in captured["endpoint"]
