from __future__ import annotations

from pathlib import Path

import pytest

from frontier.config import ConfigError, load_frontier_config
from frontier.dbt_artifacts import load_manifest
from frontier.frontier import (
    compile_route_sql,
    load_change_events_csv,
    percent_rows_avoided,
    run_frontier,
)
from frontier.warehouse import FakeWarehouse
from tests.conftest import FIXTURES


def recorded_warehouse() -> FakeWarehouse:
    return FakeWarehouse(
        {
            "full_entity_count": [(150_000, 3)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
        }
    )


def test_percent_matches_verified_tpch() -> None:
    assert percent_rows_avoided(150_000, 3) == 99.998
    assert percent_rows_avoided(150_000, 8) == 99.995
    assert percent_rows_avoided(150_000, 12) == 99.992
    assert percent_rows_avoided(150_000, 99_621) == 33.586


def test_change_events_csv_is_optional(tmp_path: Path) -> None:
    missing = tmp_path / "change_events.csv"
    assert load_change_events_csv(missing, required=False) == []


def test_run_frontier_obtains_verified_metrics() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=recorded_warehouse(),
    )
    assert result.full_entity_count == 150_000
    assert result.frontier_entity_count == 3
    assert result.percent_rows_avoided == 99.998
    values = {entity.entity_value for entity in result.affected_entities}
    assert values == {"370", "781", "36901"}
    reasons = {entity.reason for entity in result.affected_entities}
    assert "Direct customer key" in reasons
    assert "Before-image supplied customer" in reasons
    assert "Order 1 belongs to customer" in reasons


def test_route_query_must_be_select() -> None:
    manifest = load_manifest(FIXTURES / "manifest.json")
    with pytest.raises(ConfigError, match="SELECT"):
        compile_route_sql(
            "insert into {{ ref('stg_orders') }} select 1",
            manifest=manifest,
            changed_values=["1"],
        )


def test_local_result_keeps_raw_ids() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=recorded_warehouse(),
    )
    assert {entity.entity_value for entity in result.affected_entities} == {"370", "781", "36901"}
    assert "Order 1 belongs to customer" in {entity.reason for entity in result.affected_entities}
