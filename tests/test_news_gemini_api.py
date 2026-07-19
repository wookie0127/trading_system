import asyncio

from news import daily_news_orchestrator as orchestrator


class _FakeResponse:
    status_code = 200
    text = ""

    def json(self):
        return {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "# 요약\n"},
                            {"text": "- 내용"},
                        ]
                    }
                }
            ]
        }


class _FakeAsyncClient:
    last_request = None

    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def post(self, endpoint, params, json):
        _FakeAsyncClient.last_request = {
            "endpoint": endpoint,
            "params": params,
            "json": json,
            "timeout": self.timeout,
        }
        return _FakeResponse()


def test_summarize_with_gemini_api_uses_apikeys_gemini_api(monkeypatch):
    monkeypatch.setenv("GEMINI_API", "test-gemini-api")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setattr(orchestrator.httpx, "AsyncClient", _FakeAsyncClient)

    summary = asyncio.run(
        orchestrator.summarize_with_gemini_api(
            model="gemini-test",
            prompt="뉴스요약해줘",
            timeout_seconds=12,
        )
    )

    assert summary == "# 요약\n- 내용"
    assert _FakeAsyncClient.last_request["endpoint"].endswith(
        "/models/gemini-test:generateContent"
    )
    assert _FakeAsyncClient.last_request["params"] == {"key": "test-gemini-api"}
    assert _FakeAsyncClient.last_request["timeout"] == 12
    assert (
        _FakeAsyncClient.last_request["json"]["contents"][0]["parts"][0]["text"]
        == "뉴스요약해줘"
    )


def test_run_agy_cli_uses_print_mode(monkeypatch):
    captured = {}

    def fake_run(args, capture_output, text, timeout, check):
        captured["args"] = args
        captured["capture_output"] = capture_output
        captured["text"] = text
        captured["timeout"] = timeout
        captured["check"] = check

        class Result:
            returncode = 0
            stdout = "# 요약"
            stderr = ""

        return Result()

    monkeypatch.setattr(
        orchestrator.shutil, "which", lambda executable: "/usr/local/bin/agy"
    )
    monkeypatch.setattr(orchestrator.subprocess, "run", fake_run)

    output = orchestrator.run_agy_cli(
        prompt="뉴스요약해줘",
        command="agy",
        model="agy-model",
        timeout_seconds=30,
    )

    assert output == "# 요약"
    assert captured["args"] == [
        "agy",
        "--model",
        "agy-model",
        "--sandbox",
        "--print",
        "뉴스요약해줘",
        "--print-timeout",
        "30s",
    ]
    assert captured["timeout"] == 40
