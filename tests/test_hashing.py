from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from frontier.api import build_ingest_payload
from frontier.config import ConfigError, load_frontier_config
from frontier.dbt_artifacts import load_manifest
from frontier.frontier import (
    frontier_result_to_dict,
    load_change_events_csv,
    run_frontier,
)
from frontier.hashing import (
    ENTITY_HASH_KEY_ENV,
    canonical_entity_message,
    entity_hash_key_from_env,
    hmac_entity_id,
    hmac_equal,
    normalize_entity_value,
)
from frontier.snowflake import FakeWarehouse
from tests.conftest import FIXTURES

TEST_HASH_KEY = "test-only-frontier-entity-hash-key"
SHA256_OF_ONE = "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"
RAW_DEMO_VALUES = ("1", "-1", "370", "781", "36901")
FIXTURE_RUN_FILE = FIXTURES / "frontier-run.json"


def _plain_sha256(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _hmac(
    value: str,
    *,
    secret: str = TEST_HASH_KEY,
    project: str = "jaffle_shop",
    entity_type: str = "customer",
    entity_key: str = "customer_id",
) -> str:
    return hmac_entity_id(
        secret,
        project=project,
        entity_type=entity_type,
        entity_key=entity_key,
        value=value,
    )


def recorded_warehouse() -> FakeWarehouse:
    return FakeWarehouse(
        {
            "full_entity_count": [(150_000, 3)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
        }
    )


def hashed_frontier_details(secret: str = TEST_HASH_KEY) -> dict:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=recorded_warehouse(),
    )
    return frontier_result_to_dict(
        result,
        config=config,
        include_entity_ids=False,
        hash_key=secret,
    )


def hashed_ingest_payload(secret: str = TEST_HASH_KEY) -> dict:
    details = hashed_frontier_details(secret)
    return build_ingest_payload(
        external_run_id="jaffle_shop-hmac-fixture",
        project="jaffle_shop",
        environment="dev",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        model_unique_id="model.jaffle_shop.customer_summary",
        model_name="customer_summary",
        entity_type="customer",
        entity_key="customer_id",
        grain="one_row_per_customer",
        metrics=details["metrics"],
        change_events=details["changeEvents"],
        affected_entities=details["affectedEntities"],
        validation_results=[
            {
                "testName": "assert_frontier_events_resolve",
                "status": "passed",
                "differenceCount": 0,
            }
        ],
        evidence_level="empirically_validated",
        status="passed",
    )


def test_hmac_differs_from_plain_sha256() -> None:
    digest = _hmac("1", entity_type="order", entity_key="order_id")
    assert not hmac_equal(digest, SHA256_OF_ONE)
    assert not hmac_equal(digest, _plain_sha256("1"))
    for value in RAW_DEMO_VALUES:
        hashed = _hmac(value)
        assert not hmac_equal(hashed, _plain_sha256(value))


def test_same_context_and_value_are_stable() -> None:
    first = _hmac("370")
    second = _hmac("370")
    assert hmac_equal(first, second)
    assert hmac_equal(first, _hmac("370.0"))
    assert hmac_equal(first, _hmac("0370"))
    assert hmac_equal(first, _hmac(" 370 "))
    assert canonical_entity_message(
        project="jaffle_shop",
        entity_type="customer",
        entity_key="customer_id",
        value="370.0",
    ) == "v1|jaffle_shop|customer|customer_id|370"


def test_different_project_produces_different_hash() -> None:
    left = _hmac("370", project="jaffle_shop")
    right = _hmac("370", project="other_shop")
    assert not hmac_equal(left, right)


def test_different_entity_type_or_key_produces_different_hash() -> None:
    order = _hmac("1", entity_type="order", entity_key="order_id")
    customer = _hmac("1", entity_type="customer", entity_key="customer_id")
    assert not hmac_equal(order, customer)
    other_key = _hmac("370", entity_key="user_id")
    assert not hmac_equal(_hmac("370"), other_key)


def test_changed_secret_produces_different_hash() -> None:
    left = _hmac("370", secret=TEST_HASH_KEY)
    right = _hmac("370", secret="a-different-test-hash-key")
    assert not hmac_equal(left, right)


def test_direct_and_affected_customer_370_match() -> None:
    details = hashed_frontier_details()
    expected = _hmac("370")
    event_002 = next(event for event in details["changeEvents"] if event["eventId"] == "event_002")
    affected_370 = next(
        entity for entity in details["affectedEntities"] if hmac_equal(entity["entityValue"], expected)
    )
    assert hmac_equal(event_002["entityValue"], expected)
    assert hmac_equal(affected_370["entityValue"], expected)
    assert affected_370["entityType"] == "customer"
    assert affected_370["entityKey"] == "customer_id"


def test_prior_entity_value_uses_customer_context() -> None:
    details = hashed_frontier_details()
    delete = next(event for event in details["changeEvents"] if event["eventId"] == "event_003")
    prior_as_customer = _hmac("781", entity_type="customer", entity_key="customer_id")
    prior_as_order = _hmac("781", entity_type="order", entity_key="order_id")
    assert hmac_equal(delete["priorEntityValue"], prior_as_customer)
    assert not hmac_equal(delete["priorEntityValue"], prior_as_order)
    assert not hmac_equal(delete["entityValue"], prior_as_customer)
    affected_781 = next(
        entity
        for entity in details["affectedEntities"]
        if hmac_equal(entity["entityValue"], prior_as_customer)
    )
    assert hmac_equal(affected_781["entityValue"], prior_as_customer)


def test_missing_key_fails_unless_include_entity_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    with pytest.raises(ConfigError, match="FRONTIER_ENTITY_HASH_KEY"):
        entity_hash_key_from_env()
    monkeypatch.setenv(ENTITY_HASH_KEY_ENV, "   ")
    with pytest.raises(ConfigError, match="FRONTIER_ENTITY_HASH_KEY"):
        entity_hash_key_from_env()

    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=recorded_warehouse(),
    )
    with pytest.raises(ConfigError, match="FRONTIER_ENTITY_HASH_KEY"):
        frontier_result_to_dict(
            result,
            config=config,
            include_entity_ids=False,
            hash_key=None,
        )
    raw = frontier_result_to_dict(
        result,
        config=config,
        include_entity_ids=True,
        hash_key=None,
    )
    assert "370" in {entity["entityValue"] for entity in raw["affectedEntities"]}


def test_normalize_does_not_change_non_integer_identity() -> None:
    assert normalize_entity_value("CUST-370") == "CUST-370"
    assert normalize_entity_value("370.1") == "370.1"


def test_hashed_payload_omits_raw_ids_and_plain_sha256() -> None:
    payload = hashed_ingest_payload()
    serialized = json.dumps(payload)
    assert SHA256_OF_ONE not in serialized
    for value in RAW_DEMO_VALUES:
        assert _plain_sha256(value) not in serialized
    entity_values = [entity["entityValue"] for entity in payload["affectedEntities"]]
    event_values = [event["entityValue"] for event in payload["changeEvents"]]
    priors = [
        event["priorEntityValue"]
        for event in payload["changeEvents"]
        if "priorEntityValue" in event
    ]
    assert set(RAW_DEMO_VALUES).isdisjoint(entity_values + event_values + priors)
    assert "Order 1 belongs to customer" not in serialized
    assert "Order belongs to customer" in serialized
    assert TEST_HASH_KEY not in serialized
    assert ENTITY_HASH_KEY_ENV not in serialized


def test_fixture_run_file_has_no_plain_hashes() -> None:
    assert FIXTURE_RUN_FILE.is_file(), "Regenerate runner/tests/fixtures/frontier-run.json"
    text = FIXTURE_RUN_FILE.read_text()
    payload = hashed_ingest_payload()
    assert json.loads(text) == payload
    assert SHA256_OF_ONE not in text
    for value in RAW_DEMO_VALUES:
        assert _plain_sha256(value) not in text
    assert TEST_HASH_KEY not in text
