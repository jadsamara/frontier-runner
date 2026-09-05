from __future__ import annotations

from frontier.cdc.config import load_cdc_config
from frontier.cdc.normalize import normalize_stream_records, records_from_rows
from frontier.cdc.route import route_events
from frontier.config import ConfigError
from tests.conftest import FIXTURES
import pytest


def _source():
    return load_cdc_config(FIXTURES / "frontier-cdc.yml")


def _update(customer_before: str, customer_after: str):
    rows = [
        {
            "METADATA$ACTION": "DELETE",
            "METADATA$ISUPDATE": True,
            "METADATA$ROW_ID": "rid-1",
            "primary_key": "1",
            "target_key": customer_before,
        },
        {
            "METADATA$ACTION": "INSERT",
            "METADATA$ISUPDATE": True,
            "METADATA$ROW_ID": "rid-1",
            "primary_key": "1",
            "target_key": customer_after,
        },
    ]
    config = _source()
    return normalize_stream_records(
        records_from_rows(rows),
        source=config.sources[0],
        provider="snowflake_stream",
    )


def test_update_same_ownership_routes_one_candidate() -> None:
    events = _update("370", "370")
    routed = route_events(events, _source())
    assert routed.event_count == 1
    assert routed.keys == ("370",)
    assert routed.candidates[0].event_count == 1
    assert routed.candidates[0].origin == "event"


def test_ownership_change_routes_two_candidates() -> None:
    events = _update("370", "781")
    routed = route_events(events, _source())
    assert routed.event_count == 1
    assert set(routed.keys) == {"370", "781"}
    assert all(item.event_count == 1 for item in routed.candidates)


def test_insert_uses_target_key_after() -> None:
    config = _source()
    events = normalize_stream_records(
        records_from_rows(
            [
                {
                    "action": "INSERT",
                    "is_update": False,
                    "row_id": "ins",
                    "primary_key": "9",
                    "target_key": "4",
                }
            ]
        ),
        source=config.sources[0],
        provider="snowflake_stream",
    )
    routed = route_events(events, config)
    assert routed.keys == ("4",)


def test_delete_uses_target_key_before() -> None:
    config = _source()
    events = normalize_stream_records(
        records_from_rows(
            [
                {
                    "action": "DELETE",
                    "is_update": False,
                    "row_id": "del",
                    "primary_key": "5",
                    "target_key": "781",
                }
            ]
        ),
        source=config.sources[0],
        provider="snowflake_stream",
    )
    routed = route_events(events, config)
    assert routed.keys == ("781",)


def test_unresolved_before_key_fails() -> None:
    config = _source()
    events = normalize_stream_records(
        records_from_rows(
            [
                {
                    "action": "DELETE",
                    "is_update": False,
                    "row_id": "del",
                    "primary_key": "5",
                    "target_key": "781",
                }
            ]
        ),
        source=config.sources[0],
        provider="snowflake_stream",
    )
    broken = events[0]
    object.__setattr__(broken, "target_key_before", None)
    with pytest.raises(ConfigError, match="before target key"):
        route_events([broken], config)
