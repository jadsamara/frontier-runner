from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urljoin

from frontier.config import ConfigError, is_secret_key

DEFAULT_API_URL = "http://127.0.0.1:3000"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
DEFAULT_UPLOAD_ATTEMPTS = 4


def api_key_from_env() -> tuple[str, str]:
    """Return (key, env_var_name). FRONTIER_API_KEY wins over FRONTIER_DEMO_API_KEY."""
    for name in ("FRONTIER_API_KEY", "FRONTIER_DEMO_API_KEY"):
        value = os.environ.get(name)
        if value and value.strip():
            return value, name
    raise ConfigError("Set FRONTIER_API_KEY or FRONTIER_DEMO_API_KEY to upload")


def redact_api_key(key: str) -> str:
    if len(key) <= 12:
        return "********"
    return f"{key[:12]}…"


def assert_payload_has_no_secrets(payload: dict[str, Any], *, path: tuple[str, ...] = ()) -> None:
    for key, value in payload.items():
        next_path = path + (key,)
        if is_secret_key(key):
            raise ConfigError(
                "Refusing to upload a payload that contains credential field "
                + ".".join(next_path),
            )
        if isinstance(value, dict):
            assert_payload_has_no_secrets(value, path=next_path)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    assert_payload_has_no_secrets(item, path=next_path)


def assert_no_raw_rows(payload: dict[str, Any]) -> None:
    forbidden = {"rows", "records", "warehouseRows", "rawRows"}
    extra = forbidden.intersection(payload)
    if extra:
        raise ConfigError(f"Refusing to upload raw warehouse rows ({sorted(extra)})")


def build_ingest_payload(
    *,
    external_run_id: str,
    project: str,
    environment: str,
    database: str,
    schema: str,
    model_unique_id: str,
    model_name: str,
    entity_type: str,
    entity_key: str,
    grain: str,
    metrics: dict[str, Any],
    change_events: list[dict[str, Any]],
    affected_entities: list[dict[str, Any]],
    validation_results: list[dict[str, Any]],
    evidence_level: str,
    status: str,
    git: dict[str, Any] | None = None,
    entity_ids_hashed: bool = False,
    warehouse_type: str = "snowflake",
) -> dict[str, Any]:
    payload = {
        "externalRunId": external_run_id,
        "project": project,
        "environment": environment,
        "warehouse": {
            "type": warehouse_type,
            "database": database,
            "schema": schema,
        },
        "model": {
            "uniqueId": model_unique_id,
            "name": model_name,
            "entityType": entity_type,
            "entityKey": entity_key,
            "grain": grain,
        },
        "metrics": metrics,
        "changeEvents": change_events,
        "affectedEntities": affected_entities,
        "validationResults": validation_results,
        "evidenceLevel": evidence_level,
        "status": status,
        "entityIdsHashed": bool(entity_ids_hashed),
    }
    if git:
        payload["git"] = git
    assert_payload_has_no_secrets(payload)
    assert_no_raw_rows(payload)
    return payload


def _retry_after_seconds(error: urllib.error.HTTPError, attempt: int) -> float:
    header = error.headers.get("Retry-After") if error.headers else None
    if header:
        try:
            return max(0.0, float(header))
        except ValueError:
            pass
    return min(8.0, 0.25 * (2**attempt))


def upload_run(
    payload: dict[str, Any],
    *,
    api_url: str,
    api_key: str,
    timeout_seconds: int = 30,
    max_attempts: int = DEFAULT_UPLOAD_ATTEMPTS,
) -> dict[str, Any]:
    assert_payload_has_no_secrets(payload)
    assert_no_raw_rows(payload)
    url = urljoin(api_url.rstrip("/") + "/", "api/v1/runs")
    body = json.dumps(payload).encode("utf-8")
    last_error: Exception | None = None
    attempts = max(1, max_attempts)

    for attempt in range(attempts):
        request = urllib.request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Idempotency-Key": str(payload["externalRunId"]),
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                parsed = json.loads(raw) if raw else {}
                parsed["_httpStatus"] = response.status
                return parsed
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            last_error = error
            if error.code in RETRYABLE_STATUS and attempt < attempts - 1:
                time.sleep(_retry_after_seconds(error, attempt))
                continue
            raise ConfigError(f"Upload failed with HTTP {error.code}: {detail}") from error
        except urllib.error.URLError as error:
            last_error = error
            if attempt < attempts - 1:
                time.sleep(min(8.0, 0.25 * (2**attempt)))
                continue
            raise ConfigError(f"Upload failed: {error.reason}") from error

    raise ConfigError(f"Upload failed: {last_error}") from last_error
