from __future__ import annotations

import pytest

from frontier.cdc.config import load_cdc_config
from frontier.cdc.consume import consume_all, consume_source
from frontier.cdc.store import FakeCdcStore
from frontier.config import ConfigError
from tests.conftest import FIXTURES

ORDERS = "DATA_AGENT_DEV.FRONTIER_CDC.ORDERS_STREAM"
CUSTOMERS = "DATA_AGENT_DEV.FRONTIER_CDC.CUSTOMER_STREAM"


def _config():
    return load_cdc_config(FIXTURES / "frontier-cdc.yml")


def _update_pair(*, row_id: str = "rid-1", customer_before: str = "370", customer_after: str = "370") -> list[dict]:
    return [
        {
            "action": "DELETE",
            "is_update": True,
            "row_id": row_id,
            "primary_key": "1",
            "target_key": customer_before,
        },
        {
            "action": "INSERT",
            "is_update": True,
            "row_id": row_id,
            "primary_key": "1",
            "target_key": customer_after,
        },
    ]


def test_empty_stream_creates_no_batch() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: [], CUSTOMERS: []})
    results = consume_all(config, store=store, project_name="jaffle_shop")
    by_name = {item.stream_name: item for item in results}
    assert by_name["ORDERS_STREAM"].status == "EMPTY"
    assert by_name["CUSTOMER_STREAM"].status == "EMPTY"
    assert by_name["ORDERS_STREAM"].batch_id is None
    assert store.batches == []
    assert store.events == []


def test_one_insert() -> None:
    config = _config()
    store = FakeCdcStore(
        {
            ORDERS: [
                {
                    "action": "INSERT",
                    "is_update": False,
                    "row_id": "ins-1",
                    "primary_key": "99",
                    "target_key": "4",
                }
            ],
            CUSTOMERS: [],
        }
    )
    result = consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert result.status == "CAPTURED"
    assert result.raw_record_count == 1
    assert result.logical_event_count == 1
    assert result.operation_counts["inserts"] == 1
    assert store.events[0][1].operation == "INSERT"


def test_one_delete() -> None:
    config = _config()
    store = FakeCdcStore(
        {
            ORDERS: [
                {
                    "action": "DELETE",
                    "is_update": False,
                    "row_id": "del-1",
                    "primary_key": "5",
                    "target_key": "781",
                }
            ]
        }
    )
    result = consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert result.operation_counts["deletes"] == 1
    assert store.events[0][1].operation == "DELETE"


def test_one_update_pair() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair(), CUSTOMERS: []})
    results = consume_all(config, store=store, project_name="jaffle_shop")
    orders = next(item for item in results if item.stream_name == "ORDERS_STREAM")
    customers = next(item for item in results if item.stream_name == "CUSTOMER_STREAM")
    assert orders.status == "CAPTURED"
    assert orders.raw_record_count == 2
    assert orders.logical_event_count == 1
    assert orders.operation_counts == {"inserts": 0, "updates": 1, "deletes": 0}
    assert customers.status == "EMPTY"
    assert not store.stream_has_data(ORDERS)
    event = store.events[0][1]
    assert event.operation == "UPDATE"
    assert event.primary_key_before == "1"
    assert event.primary_key_after == "1"


def test_unpaired_update_rolls_back() -> None:
    config = _config()
    rows = [
        {
            "action": "DELETE",
            "is_update": True,
            "row_id": "orphan",
            "primary_key": "1",
            "target_key": "370",
        }
    ]
    store = FakeCdcStore({ORDERS: rows})
    with pytest.raises(ConfigError, match="unpaired"):
        consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert store.stream_has_data(ORDERS)
    assert store.events == []
    assert store.claims == {}


def test_changed_target_ownership() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair(customer_before="370", customer_after="781")})
    result = consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert result.logical_event_count == 1
    event = store.events[0][1]
    assert event.target_key_before == "370"
    assert event.target_key_after == "781"


def test_retry_after_capture_reuses_batch() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair(), CUSTOMERS: []})
    first = consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    second = consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert first.status == "CAPTURED"
    assert second.status == "REUSED"
    assert second.batch_id == first.batch_id
    assert len(store.events) == 1
    assert len([batch for batch in store.batches if batch.status == "CAPTURED"]) == 1


def test_concurrent_consumer_claim() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair()})
    store.block_claims = True
    with pytest.raises(ConfigError, match="already claimed"):
        consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert store.stream_has_data(ORDERS)
    assert store.events == []


def test_rollback_before_committed_capture() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair()})
    store.fail_capture = True
    with pytest.raises(ConfigError, match="capture DML failed"):
        consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert store.stream_has_data(ORDERS)
    assert store.events == []
    assert store.claims == {}
    assert not any(batch.status == "CAPTURED" for batch in store.batches)


def test_duplicate_event_fingerprint_fails_closed() -> None:
    config = _config()
    store = FakeCdcStore({ORDERS: _update_pair(), CUSTOMERS: []})
    consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    store.streams[ORDERS] = _update_pair()
    with pytest.raises(ConfigError, match="duplicate CDC event fingerprint"):
        consume_source(config.sources[0], store=store, config=config, project_name="jaffle_shop")
    assert len(store.events) == 1
