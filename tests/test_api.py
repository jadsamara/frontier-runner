from __future__ import annotations

import json
from pathlib import Path

import pytest

from frontier.api import build_ingest_payload, upload_run, api_key_from_env
from frontier.config import ConfigError, load_frontier_config
from frontier.dbt_artifacts import load_manifest
from frontier.frontier import (
    frontier_result_to_dict,
    load_change_events_csv,
    run_frontier,
)
from frontier.hashing import hmac_equal
from frontier.snowflake import FakeWarehouse, describe_connection, load_snowflake_config
from tests.conftest import FIXTURES


def test_profile_password_is_redacted(dbt_project: Path) -> None:
    config = load_snowflake_config(
        dbt_project,
        profiles_path=FIXTURES / "profiles.yml",
    )
    dumped = repr(config)
    assert "super-secret-password" not in dumped
    assert "********" in dumped
    summary = json.dumps(describe_connection(config))
    assert "super-secret-password" not in summary
    assert summary.count("********") >= 1


def test_hashed_upload_omits_raw_entity_ids() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3)],
                "order_id in (1)": [(36901,)],
                "order_id in (-1)": [],
            }
        ),
    )
    hashed = frontier_result_to_dict(
        result,
        config=config,
        include_entity_ids=False,
        hash_key="test-only-frontier-entity-hash-key",
    )
    values = [entity["entityValue"] for entity in hashed["affectedEntities"]]
    event_values = [event["entityValue"] for event in hashed["changeEvents"]]
    priors = [
        event["priorEntityValue"]
        for event in hashed["changeEvents"]
        if "priorEntityValue" in event
    ]
    assert "370" not in values
    assert "781" not in values
    assert "36901" not in values
    assert all(len(value) == 64 for value in values + event_values + priors)
    assert all(not hmac_equal(value, "370") for value in values)
    assert "Order 1 belongs to customer" not in {entity["reason"] for entity in hashed["affectedEntities"]}
    raw = frontier_result_to_dict(
        result,
        config=config,
        include_entity_ids=True,
        hash_key=None,
    )
    assert "370" in {entity["entityValue"] for entity in raw["affectedEntities"]}


def test_payload_rejects_credential_fields() -> None:
    payload = build_ingest_payload(
        external_run_id="snowflake-demo-001",
        project="jaffle_shop",
        environment="dev",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        model_unique_id="model.jaffle_shop.customer_summary",
        model_name="customer_summary",
        entity_type="customer",
        entity_key="customer_id",
        grain="one_row_per_customer",
        metrics={"fullEntityCount": 150000, "frontierEntityCount": 3, "percentRowsAvoided": 99.998},
        change_events=[
            {
                "eventId": "event_001",
                "sourceModel": "stg_orders",
                "operation": "UPDATE",
                "entityKey": "order_id",
                "entityValue": "1",
            }
        ],
        affected_entities=[
            {
                "entityType": "customer",
                "entityKey": "customer_id",
                "entityValue": "370",
                "reason": "Direct customer key",
            }
        ],
        validation_results=[
            {"testName": "assert_frontier_events_resolve", "status": "passed", "differenceCount": 0}
        ],
        evidence_level="empirically_validated",
        status="passed",
    )
    payload["warehouse"]["password"] = "super-secret-password"
    with pytest.raises(ConfigError, match="credential"):
        upload_run(payload, api_url="http://127.0.0.1:9", api_key="frn_test")


def test_upload_posts_aggregates(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeResponse:
        status = 201

        def read(self) -> bytes:
            return json.dumps({"id": "11111111-1111-4111-8111-111111111111", "created": True, "externalRunId": "run-1"}).encode()

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request, timeout=30):  # noqa: ANN001
        captured["url"] = request.full_url
        captured["auth"] = request.get_header("Authorization")
        captured["idempotency"] = request.get_header("Idempotency-key")
        captured["body"] = json.loads(request.data.decode())
        return FakeResponse()

    monkeypatch.setattr("frontier.api.urllib.request.urlopen", fake_urlopen)
    payload = build_ingest_payload(
        external_run_id="snowflake-demo-001",
        project="jaffle_shop",
        environment="dev",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        model_unique_id="model.jaffle_shop.customer_summary",
        model_name="customer_summary",
        entity_type="customer",
        entity_key="customer_id",
        grain="one_row_per_customer",
        metrics={"fullEntityCount": 150000, "frontierEntityCount": 3, "percentRowsAvoided": 99.998},
        change_events=[
            {
                "eventId": "event_001",
                "sourceModel": "stg_orders",
                "operation": "UPDATE",
                "entityKey": "order_id",
                "entityValue": "1",
            }
        ],
        affected_entities=[
            {
                "entityType": "customer",
                "entityKey": "customer_id",
                "entityValue": "370",
                "reason": "Direct customer key",
            }
        ],
        validation_results=[
            {"testName": "assert_frontier_events_resolve", "status": "passed", "differenceCount": 0},
            {"testName": "assert_frontier_matches_full_mart", "status": "passed", "differenceCount": 0},
        ],
        evidence_level="empirically_validated",
        status="passed",
    )
    response = upload_run(payload, api_url="http://127.0.0.1:3000", api_key="frn_demo")
    assert captured["url"] == "http://127.0.0.1:3000/api/v1/runs"
    assert captured["idempotency"] == "snowflake-demo-001"
    assert "password" not in json.dumps(captured["body"])
    assert "rows" not in captured["body"]
    assert response["created"] is True


def test_api_key_prefers_frontier_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRONTIER_API_KEY", "your-milestone-placeholder-key")
    monkeypatch.setenv("FRONTIER_DEMO_API_KEY", "frn_demo_jaffle_shop_local_only")
    key, source = api_key_from_env()
    assert key == "your-milestone-placeholder-key"
    assert source == "FRONTIER_API_KEY"


def test_api_key_falls_back_to_demo_when_api_key_blank(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRONTIER_API_KEY", "   ")
    monkeypatch.setenv("FRONTIER_DEMO_API_KEY", "frn_demo_jaffle_shop_local_only")
    key, source = api_key_from_env()
    assert key == "frn_demo_jaffle_shop_local_only"
    assert source == "FRONTIER_DEMO_API_KEY"


def test_api_key_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FRONTIER_API_KEY", raising=False)
    monkeypatch.delenv("FRONTIER_DEMO_API_KEY", raising=False)
    with pytest.raises(ConfigError, match="FRONTIER_API_KEY"):
        api_key_from_env()

