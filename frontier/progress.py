from __future__ import annotations

import os
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager


def configure_stdio() -> None:
    """Make prove logs show up immediately in GitHub Actions."""
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(line_buffering=True)
        except (AttributeError, OSError, ValueError):
            pass


def elapsed_ms(started: float) -> int:
    return max(0, round((time.perf_counter() - started) * 1000))


def failure_status(error: BaseException) -> str:
    return f"failed:{type(error).__name__}"


def log_step(message: str, *, duration_ms: int | None = None, status: str | None = None) -> None:
    """Flush a prove progress line. Never include secrets or entity IDs."""
    parts = [f"prove: {message}"]
    if status:
        parts.append(status)
    if duration_ms is not None:
        parts.append(f"{duration_ms} ms")
    print(" ".join(parts), flush=True)


@contextmanager
def logged_step(started_message: str, completed_message: str) -> Iterator[None]:
    started = time.perf_counter()
    log_step(started_message)
    try:
        yield
    except BaseException as error:
        log_step(
            completed_message,
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step(completed_message, duration_ms=elapsed_ms(started), status="ok")


def env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default
