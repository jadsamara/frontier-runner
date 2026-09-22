from __future__ import annotations

import os
import re
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


_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SECRET_SHAPED = re.compile(
    r"(?i)^(password|secret|token|api[_-]?key|private[_-]?key|credential|frn_|bearer\b|sk-|pk_)"
)
_LONG_DIGIT_RUN = re.compile(r"\b\d{6,}\b")


def _keep_quoted_diagnostic(value: str) -> bool:
    text = value.strip()
    if not text or _SECRET_SHAPED.search(text):
        return False
    if _UUID.fullmatch(text) or _SAFE_IDENT.fullmatch(text):
        return True
    if re.fullmatch(r"[0-9A-Z]{5}", text):
        return True
    return False


def _redact_quoted(match: re.Match[str]) -> str:
    quote = match.group(1)
    inner = match.group(2)
    if _keep_quoted_diagnostic(inner):
        return f"{quote}{inner}{quote}"
    return f"{quote}***{quote}"


def redact_failure_reason(error: BaseException) -> str:
    """Keep structural diagnostics; strip secrets, credentials, and entity values."""
    text = f"{type(error).__name__}: {error}"
    text = re.sub(
        r"(?i)((?:password|secret|token|api[_-]?key|private[_-]?key|credential)\s*[=:]\s*)(['\"])([^'\"]*)\2",
        r"\1\2***\2",
        text,
    )
    text = re.sub(r"(['\"])([^'\"]{0,400})\1", _redact_quoted, text)
    text = _LONG_DIGIT_RUN.sub("***", text)
    lowered = text.lower()
    for part in ("password", "token", "secret", "private_key", "api_key"):
        if part in lowered:
            text = re.sub(part, "***", text, flags=re.I)
    return text[:512]


def log_step(
    message: str,
    *,
    duration_ms: int | None = None,
    status: str | None = None,
    prefix: str = "prove",
) -> None:
    """Flush a progress line. Never include secrets or entity IDs."""
    parts = [f"{prefix}: {message}"]
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
