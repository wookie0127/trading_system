import os
import subprocess
import time
import uuid
from pathlib import Path


DEFAULT_CMUX_COMMAND = "cmux"
DEFAULT_DONE_PREFIX = "<<CMUX_NEWS_DONE:"


def run_cmux_gemini_pane(
    prompt: str,
    workspace: str | None = None,
    surface: str | None = None,
    cmux_command: str | None = None,
    timeout_seconds: int = 180,
    poll_interval_seconds: float = 2.0,
) -> str:
    """Send a prompt to an already-running Gemini pane and capture its markdown answer."""
    command = cmux_command or os.getenv("CMUX_COMMAND", DEFAULT_CMUX_COMMAND)
    resolved_workspace = workspace or os.getenv("CMUX_GEMINI_WORKSPACE")
    resolved_surface = surface or os.getenv("CMUX_GEMINI_SURFACE")
    done_marker = f"{DEFAULT_DONE_PREFIX}{uuid.uuid4().hex}>>"
    terminal_prompt = _append_done_marker_instruction(prompt, done_marker)

    target_args = _target_args(resolved_workspace, resolved_surface)
    _run_cmux([command, "clear-history", *target_args], timeout_seconds=20)
    _run_cmux(
        [command, "set-buffer", "--name", "news-briefing-prompt", terminal_prompt],
        timeout_seconds=20,
    )
    _run_cmux(
        [command, "paste-buffer", "--name", "news-briefing-prompt", *target_args],
        timeout_seconds=20,
    )
    _run_cmux([command, "send-key", *target_args, "Enter"], timeout_seconds=20)

    deadline = time.monotonic() + timeout_seconds
    last_screen = ""
    while time.monotonic() < deadline:
        completed = _run_cmux(
            [command, "read-screen", "--scrollback", "--lines", "400", *target_args],
            timeout_seconds=20,
        )
        last_screen = completed.stdout
        if done_marker in last_screen:
            return _extract_markdown_from_screen(last_screen, done_marker)
        time.sleep(poll_interval_seconds)

    raise TimeoutError(
        "Timed out waiting for Gemini pane response marker "
        f"after {timeout_seconds}s. Last captured screen:\n{last_screen[-2000:]}"
    )


def _append_done_marker_instruction(prompt: str, done_marker: str) -> str:
    return (
        f"{prompt.rstrip()}\n\n"
        "응답이 끝나면 마지막 줄에 아래 완료 마커만 그대로 출력하라.\n"
        f"{done_marker}"
    )


def _target_args(workspace: str | None, surface: str | None) -> list[str]:
    args: list[str] = []
    if workspace:
        args.extend(["--workspace", workspace])
    if surface:
        args.extend(["--surface", surface])
    return args


def _run_cmux(
    args: list[str], timeout_seconds: int
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("CMUX_SOCKET_PATH", _discover_cmux_socket_path())
    completed = subprocess.run(
        args,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        env=env,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"cmux command failed ({' '.join(args)}): "
            f"{completed.stderr.strip() or completed.stdout.strip()}"
        )
    return completed


def _discover_cmux_socket_path() -> str:
    state_dir = Path.home() / ".local" / "state" / "cmux"
    last_socket_path = state_dir / "last-socket-path"
    if last_socket_path.exists():
        socket_path = last_socket_path.read_text(encoding="utf-8").strip()
        if socket_path:
            return socket_path
    return str(state_dir / "cmux.sock")


def _extract_markdown_from_screen(screen: str, done_marker: str) -> str:
    before_marker = screen.rsplit(done_marker, 1)[0]
    if done_marker in before_marker:
        before_marker = before_marker.split(done_marker, 1)[-1]

    lines = before_marker.splitlines()
    start_index = 0
    for index, line in enumerate(lines):
        if line.lstrip().startswith("# "):
            start_index = index
            break

    markdown = "\n".join(lines[start_index:]).strip()
    if not markdown:
        raise RuntimeError(
            "Gemini pane response marker was found, but markdown output was empty"
        )
    return markdown
