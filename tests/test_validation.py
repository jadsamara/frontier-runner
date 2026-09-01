from __future__ import annotations

from frontier.config import load_frontier_config
from frontier.dbt_artifacts import load_manifest, load_run_results
from frontier.frontier import load_change_events_csv, run_frontier
from frontier.snowflake import FakeWarehouse
from frontier.validation import collect_validation_results, evidence_level
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
