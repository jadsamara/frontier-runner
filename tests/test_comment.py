from __future__ import annotations

import json
from typing import Any

import pytest

from frontier.comment import (
    COMMENT_MARKER,
    dashboard_run_url,
    format_pr_comment,
    maybe_upsert_pr_comment,
    upsert_pr_comment,
)
from frontier.config import ConfigError

PASSED_PAYLOAD = {
    "status": "passed",
    "evidenceLevel": "empirically_validated",
    "model": {
        "name": "customer_summary",
        "entityType": "customer",
        "entityKey": "customer_id",
    },
    "metrics": {
        "fullEntityCount": 150_000,
        "frontierEntityCount": 3,
        "percentRowsAvoided": 99.998,
    },
    "changeEvents": [
        {"eventId": "event_001", "entityValue": "1"},
        {"eventId": "event_002", "entityValue": "370"},
        {"eventId": "event_003", "entityValue": "-1", "priorEntityValue": "781"},
    ],
    "affectedEntities": [
        {"entityValue": "370", "reason": "Direct customer key"},
        {"entityValue": "781", "reason": "Before-image supplied customer"},
        {"entityValue": "36901", "reason": "Order 1 belongs to customer"},
    ],
    "validationResults": [
        {"testName": "assert_frontier_events_resolve", "status": "passed", "differenceCount": 0},
        {
            "testName": "assert_frontier_matches_full_mart",
            "status": "passed",
            "differenceCount": 0,
        },
    ],
}

FAILED_PAYLOAD = {
    **PASSED_PAYLOAD,
    "status": "failed",
    "evidenceLevel": "equivalence",
    "validationResults": [
        {
            "testName": "assert_repaired_equals_reference",
            "status": "failed",
            "differenceCount": 2,
            "message": "customer 370 mismatched",
        }
    ],
}


def test_dashboard_run_url_strips_trailing_slash() -> None:
    assert (
        dashboard_run_url("https://frontier.example/", "11111111-1111-4111-8111-111111111111")
        == "https://frontier.example/runs/11111111-1111-4111-8111-111111111111"
    )


def test_passed_comment_matches_acceptance_shape() -> None:
    body = format_pr_comment(
        PASSED_PAYLOAD,
        run_url="https://frontier.example/runs/11111111-1111-4111-8111-111111111111",
    )
    assert COMMENT_MARKER in body
    assert "Frontier impact assessment: PASSED" in body
    assert "Model: customer_summary" in body
    assert "Changed source events: 3" in body
    assert "Affected customers: 3 of 150,000" in body
    assert "Rows avoided: 99.998%" in body
    assert "Validation differences: 0" in body
    assert "Evidence: empirically validated" in body
    assert (
        "View full assessment: https://frontier.example/runs/11111111-1111-4111-8111-111111111111"
        in body
    )
    assert "370" not in body
    assert "781" not in body
    assert "36901" not in body
    assert "customer_id" not in body
    assert "Direct customer key" not in body


def test_comment_includes_artifact_fingerprints() -> None:
    payload = {
        **PASSED_PAYLOAD,
        "sqlComparison": {
            "base": {
                "fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "modelCount": 4,
            },
            "pr": {
                "fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "modelCount": 4,
            },
            "added": [],
            "removed": [],
            "modified": [
                {
                    "name": "stg_orders",
                    "changeKinds": ["FILTER_CHANGED"],
                    "unsafe": False,
                }
            ],
            "narrowFrontierSafe": True,
        },
    }
    body = format_pr_comment(payload, run_url="https://frontier.example/runs/1")
    assert "SQL models changed: 1 modified, 0 added, 0 removed" in body
    assert "SQL change kinds: FILTER_CHANGED" in body
    assert "Narrow frontier: not allowed" not in body
    assert "Base artifact: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" in body
    assert "PR artifact: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" in body
    assert "370" not in body


def test_comment_marks_unsafe_sql_without_entity_ids() -> None:
    payload = {
        **FAILED_PAYLOAD,
        "sqlComparison": {
            "base": {
                "fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "modelCount": 4,
            },
            "pr": {
                "fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "modelCount": 4,
            },
            "added": [],
            "removed": [],
            "modified": [
                {
                    "name": "customer_summary",
                    "changeKinds": ["GROUPING_CHANGED"],
                    "unsafe": True,
                }
            ],
            "narrowFrontierSafe": False,
        },
    }
    body = format_pr_comment(payload, run_url="https://frontier.example/runs/1")
    assert "SQL change kinds: GROUPING_CHANGED" in body
    assert "Narrow frontier: not allowed" in body
    assert "370" not in body


def test_failed_comment_is_prominent_without_customer_data() -> None:
    body = format_pr_comment(
        FAILED_PAYLOAD,
        run_url="https://frontier.example/runs/run-2",
    )
    assert "**Frontier impact assessment: FAILED**" in body
    assert "Failed checks:" in body
    assert "- assert_repaired_equals_reference (differences=2)" in body
    assert "customer 370 mismatched" not in body
    assert "370" not in body


class _FakeResponse:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self.status = status
        self._payload = payload

    def read(self) -> bytes:
        if self._payload is None:
            return b""
        if isinstance(self._payload, bytes):
            return self._payload
        return json.dumps(self._payload).encode()

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_upsert_creates_then_updates(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_urlopen(request, timeout=30):
        calls.append((request.get_method(), request.full_url))
        if request.get_method() == "GET":
            if any(item[0] == "POST" for item in calls[:-1]):
                return _FakeResponse(
                    [{"id": 99, "body": f"{COMMENT_MARKER}\nold\n"}],
                )
            return _FakeResponse([])
        return _FakeResponse({"id": 99}, status=201)

    monkeypatch.setattr("frontier.comment.urllib.request.urlopen", fake_urlopen)
    first = upsert_pr_comment(
        body=format_pr_comment(PASSED_PAYLOAD, run_url="https://example/runs/1"),
        repository="acme/jaffle_shop",
        pr_number=42,
        github_token="ghs_test_token_not_for_saas",
    )
    second = upsert_pr_comment(
        body=format_pr_comment(PASSED_PAYLOAD, run_url="https://example/runs/2"),
        repository="acme/jaffle_shop",
        pr_number=42,
        github_token="ghs_test_token_not_for_saas",
    )
    assert first == "created"
    assert second == "updated"
    methods = [item[0] for item in calls]
    assert methods == ["GET", "POST", "GET", "PATCH"]
    assert all("ghs_test_token_not_for_saas" not in url for _method, url in calls)


def test_maybe_upsert_skips_outside_github_actions(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_should_not_be_used")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/jaffle_shop")
    monkeypatch.setenv("FRONTIER_PULL_REQUEST", "42")
    assert (
        maybe_upsert_pr_comment(
            PASSED_PAYLOAD,
            api_url="https://frontier.example",
            run_id="11111111-1111-4111-8111-111111111111",
        )
        is None
    )


def test_upsert_surfaces_http_errors(monkeypatch) -> None:
    import io
    import urllib.error

    def boom(request, timeout=30):
        raise urllib.error.HTTPError(
            request.full_url,
            403,
            "Forbidden",
            hdrs=None,  # type: ignore[arg-type]
            fp=io.BytesIO(b'{"message":"Resource not accessible by integration"}'),
        )

    monkeypatch.setattr("frontier.comment.urllib.request.urlopen", boom)
    with pytest.raises(ConfigError, match="GitHub PR comment failed"):
        upsert_pr_comment(
            body="x",
            repository="acme/jaffle_shop",
            pr_number=1,
            github_token="ghs_test",
        )
