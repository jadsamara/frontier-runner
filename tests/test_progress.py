from __future__ import annotations

import os

from frontier.progress import configure_stdio, failure_status, log_step, redact_failure_reason


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


def test_redaction_keeps_structural_identifiers_and_query_ids() -> None:
    error = RuntimeError(
        "warehouse job: 01c73fa8-3204-9404-0008-2c320009f2e2 "
        "SQLSTATE: 42S22 SQL compilation error: invalid identifier 'CUSTOMER_NAME' "
        "model customer_repair_summary"
    )
    text = redact_failure_reason(error)
    assert "CUSTOMER_NAME" in text
    assert "01c73fa8-3204-9404-0008-2c320009f2e2" in text
    assert "42S22" in text
    assert "customer_repair_summary" in text
    assert "'***'" not in text


def test_redaction_strips_secrets_and_entity_values() -> None:
    error = RuntimeError(
        "password='hunter2' token='frn_live_abc' api_key=\"sk-secret\" "
        "entity 'Acme Corp' id 1234567"
    )
    text = redact_failure_reason(error)
    assert "hunter2" not in text
    assert "frn_live_abc" not in text
    assert "sk-secret" not in text
    assert "Acme Corp" not in text
    assert "1234567" not in text
    assert "***" in text
