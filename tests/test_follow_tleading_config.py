from follow_telegram_leading.orchestrator import TleadingReadingOrchestrator


def test_resolve_chat_list_supports_comma_separated_aliases():
    orchestrator = TleadingReadingOrchestrator.__new__(TleadingReadingOrchestrator)
    orchestrator.chat_aliases = {
        "cafe_share": {"chat_id": 3875818348},
        "chart_master_kospi": {"chat_id": 3956165696},
    }

    assert orchestrator._resolve_chat_list("cafe_share, chart_master_kospi") == [
        3875818348,
        3956165696,
    ]
    assert orchestrator._resolve_chat_list(("cafe_share", "chart_master_kospi")) == [
        3875818348,
        3956165696,
    ]
