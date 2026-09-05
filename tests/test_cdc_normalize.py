from __future__ import annotations

import pytest

from frontier.cdc.config import load_cdc_config
from frontier.cdc.normalize import normalize_stream_records, operation_counts, records_from_rows
from frontier.config import ConfigError
from tests.conftest import FIXTURES


def _source():
    return load_cdc_config(FIXTURES / "frontier-cdc.yml").sources[0]


def _row(*, action: str, is_update: bool, row_id: str, order_id: str, customer_id: str) -> dict:
    return {
        "METADATA$ACTION": action,
        "METADATA$ISUPDATE": is_update,
        "METADATA$ROW_ID": row_id,
        "primary_key": order_id,
        "target_key": customer_id,
    }


def test_insert_normalizes_to_insert() -> None:
    events = normalize_stream_records(
        records_from_rows([_row(action="INSERT", is_update=False, row_id="r1", order_id="9", customer_id="2")]),
        source=_source(),
        provider="snowflake_stream",
    )
    assert len(events) == 1
    assert events[0].operation == "INSERT"
    assert events[0].primary_key_after == "9"
    assert events[0].target_key_after == "2"
    assert events[0].is_update is False


def test_delete_normalizes_to_delete() -> None:
    events = normalize_stream_records(
        records_from_rows([_row(action="DELETE", is_update=False, row_id="r2", order_id="5", customer_id="781")]),
        source=_source(),
        provider="snowflake_stream",
    )
    assert events[0].operation == "DELETE"
    assert events[0].primary_key_before == "5"
    assert events[0].target_key_before == "781"


def test_update_pair_is_one_logical_event() -> None:
    rows = [
        _row(action="DELETE", is_update=True, row_id="rid-1", order_id="1", customer_id="370"),
        _row(action="INSERT", is_update=True, row_id="rid-1", order_id="1", customer_id="370"),
    ]
    events = normalize_stream_records(records_from_rows(rows), source=_source(), provider="snowflake_stream")
    assert len(events) == 1
    event = events[0]
    assert event.operation == "UPDATE"
    assert event.is_update is True
    assert event.primary_key_before == "1"
    assert event.primary_key_after == "1"
    assert event.target_key_before == "370"
    assert event.target_key_after == "370"
    assert operation_counts(events) == {"inserts": 0, "updates": 1, "deletes": 0}


def test_changed_target_ownership_stays_one_update() -> None:
    rows = [
        _row(action="DELETE", is_update=True, row_id="rid-2", order_id="1", customer_id="370"),
        _row(action="INSERT", is_update=True, row_id="rid-2", order_id="1", customer_id="781"),
    ]
    events = normalize_stream_records(records_from_rows(rows), source=_source(), provider="snowflake_stream")
    assert len(events) == 1
    assert events[0].operation == "UPDATE"
    assert events[0].target_key_before == "370"
    assert events[0].target_key_after == "781"


def test_unpaired_update_fails_closed() -> None:
    rows = [_row(action="DELETE", is_update=True, row_id="rid-3", order_id="1", customer_id="370")]
    with pytest.raises(ConfigError, match="unpaired"):
        normalize_stream_records(records_from_rows(rows), source=_source(), provider="snowflake_stream")


def test_duplicate_update_pair_fails_closed() -> None:
    rows = [
        _row(action="DELETE", is_update=True, row_id="rid-4", order_id="1", customer_id="370"),
        _row(action="INSERT", is_update=True, row_id="rid-4", order_id="1", customer_id="370"),
        _row(action="DELETE", is_update=True, row_id="rid-4", order_id="1", customer_id="370"),
        _row(action="INSERT", is_update=True, row_id="rid-4", order_id="1", customer_id="370"),
    ]
    with pytest.raises(ConfigError, match="duplicate"):
        normalize_stream_records(records_from_rows(rows), source=_source(), provider="snowflake_stream")


def test_delete_missing_before_target_key_fails_closed() -> None:
    row = _row(action="DELETE", is_update=False, row_id="r5", order_id="5", customer_id="781")
    row["target_key"] = None
    with pytest.raises(ConfigError, match="before target key"):
        normalize_stream_records(records_from_rows([row]), source=_source(), provider="snowflake_stream")
