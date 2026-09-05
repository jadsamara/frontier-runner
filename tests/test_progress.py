from __future__ import annotations

import os

from frontier.progress import configure_stdio, failure_status, log_step


def test_configure_stdio_sets_unbuffered(monkeypatch) -> None:
    monkeypatch.delenv("PYTHONUNBUFFERED", raising=False)
    configure_stdio()
    assert os.environ.get("PYTHONUNBUFFERED") == "1"


def test_log_step_flushes_without_secrets(capsys) -> None:
    log_step("Snowflake connection started")
    log_step("Snowflake connected", duration_ms=12, status="ok")
    out = capsys.readouterr().out
    assert out == (
        "prove: Snowflake connection started\n"
        "prove: Snowflake connected ok 12 ms\n"
    )


def test_failure_status_is_exception_type_only() -> None:
    status = failure_status(RuntimeError("password=super-secret-password entity=123"))
    assert status == "failed:RuntimeError"
    assert "password" not in status
    assert "123" not in status
