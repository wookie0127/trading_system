import subprocess
import re

import pytest

from news.cmux_gemini import run_cmux_gemini_pane


def test_run_cmux_gemini_pane_sends_prompt_and_extracts_markdown(monkeypatch):
    calls = []
    envs = []

    def fake_run(args, **kwargs):
        calls.append(args)
        envs.append(kwargs["env"])
        if args[1] == "read-screen":
            prompt = next(part for call in calls for part in call if "<<CMUX_NEWS_DONE:" in part)
            marker = re.search(r"<<CMUX_NEWS_DONE:[a-f0-9]+>>", prompt).group(0)
            return subprocess.CompletedProcess(
                args,
                0,
                f"뉴스요약해줘\n{marker}\n# 오늘 뉴스\n- 요약\n{marker}\n",
                "",
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setenv("CMUX_SOCKET_PATH", "/tmp/cmux-test.sock")
    monkeypatch.setattr("time.sleep", lambda _: None)

    output = run_cmux_gemini_pane(
        "뉴스요약해줘",
        workspace="workspace:1",
        surface="surface:2",
        cmux_command="cmux",
        timeout_seconds=5,
    )

    assert output == "# 오늘 뉴스\n- 요약"
    assert calls[0] == ["cmux", "clear-history", "--workspace", "workspace:1", "--surface", "surface:2"]
    assert calls[1][0:4] == ["cmux", "set-buffer", "--name", "news-briefing-prompt"]
    assert calls[2] == [
        "cmux",
        "paste-buffer",
        "--name",
        "news-briefing-prompt",
        "--workspace",
        "workspace:1",
        "--surface",
        "surface:2",
    ]
    assert calls[3] == ["cmux", "send-key", "--workspace", "workspace:1", "--surface", "surface:2", "Enter"]
    assert all(env["CMUX_SOCKET_PATH"] == "/tmp/cmux-test.sock" for env in envs)


def test_run_cmux_gemini_pane_times_out_without_marker(monkeypatch):
    def fake_run(args, **kwargs):
        return subprocess.CompletedProcess(args, 0, "still thinking", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("time.sleep", lambda _: None)

    with pytest.raises(TimeoutError):
        run_cmux_gemini_pane("뉴스요약해줘", cmux_command="cmux", timeout_seconds=0)
