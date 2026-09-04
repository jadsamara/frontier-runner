from __future__ import annotations

from frontier.config import load_frontier_config
from frontier.dbt_artifacts import load_manifest, load_run_results
from frontier.frontier import load_change_events_csv, run_frontier
from frontier.warehouse import FakeWarehouse
from frontier.validation import (
    collect_validation_results,
    evidence_level,
    sql_change_narrow_frontier_result,
)
from tests.conftest import FIXTURES


def test_reads_validation_status_from_run_results() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    warehouse = FakeWarehouse(
        {
            "full_entity_count": [(150_000, 3)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
        }
    )
    result = run_frontier(config, manifest=manifest, events=events, warehouse=warehouse)
    validations = collect_validation_results(
        config=config,
        manifest=manifest,
        run_results=load_run_results(FIXTURES / "run_results.json"),
        events=events,
        result=result,
        warehouse=warehouse,
    )
    by_name = {item.test_name: item for item in validations}
    assert by_name["assert_frontier_events_resolve"].status == "passed"
    assert by_name["assert_frontier_matches_full_mart"].status == "passed"
    assert by_name["assert_frontier_events_resolve"].difference_count == 0
    assert evidence_level(validations) == "empirically_validated"


def test_sql_change_validation_fails_closed_without_empty_affected_set() -> None:
    passed = sql_change_narrow_frontier_result(
        {
            "modified": [
                {
                    "name": "stg_orders",
                    "changeKinds": ["FILTER_CHANGED"],
                    "unsafe": False,
                }
            ],
            "narrowFrontierSafe": True,
        }
    )
    assert passed is not None
    assert passed.status == "passed"
    assert passed.difference_count == 0

    failed = sql_change_narrow_frontier_result(
        {
            "modified": [
                {
                    "name": "customer_summary",
                    "changeKinds": ["GROUPING_CHANGED"],
                    "unsafe": True,
                }
            ],
            "narrowFrontierSafe": False,
        }
    )
    assert failed is not None
    assert failed.status == "failed"
    assert failed.difference_count >= 1
    assert "GROUPING_CHANGED" in (failed.message or "")
    assert sql_change_narrow_frontier_result(None) is None

    rebuild = sql_change_narrow_frontier_result(
        {
            "modified": [
                {
                    "name": "int_customer_orders",
                    "changeKinds": ["FILTER_CHANGED"],
                    "unsafe": False,
                    "impactStatus": "FULL_REBUILD_REQUIRED",
                    "impactReasons": ["RECURSIVE_CTE"],
                }
            ],
            "narrowFrontierSafe": False,
        }
    )
    assert rebuild is not None
    assert rebuild.status == "failed"
    assert "RECURSIVE_CTE" in (rebuild.message or "")
    assert "FILTER_CHANGED" in (rebuild.message or "")


def test_sql_change_assessment_skips_event_validations() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    manifest = load_manifest(FIXTURES / "manifest.json")
    warehouse = FakeWarehouse({"full_entity_count": [(150_000, 8)]})
    result = run_frontier(config, manifest=manifest, events=[], warehouse=warehouse)
    validations = collect_validation_results(
        config=config,
        manifest=manifest,
        run_results=load_run_results(FIXTURES / "run_results.json"),
        events=[],
        result=result,
        warehouse=warehouse,
    )
    assert validations == []
