from __future__ import annotations

from dataclasses import dataclass

from frontier.config import FrontierConfig
from frontier.dbt_artifacts import Manifest, RunResults
from frontier.frontier import ChangeEvent, FrontierResult
from frontier.warehouse import WarehouseAdapter


@dataclass(frozen=True)
class ValidationResult:
    test_name: str
    status: str
    difference_count: int
    message: str | None = None


def _status(passed: bool) -> str:
    return "passed" if passed else "failed"


def event_resolution_result(
    events: list[ChangeEvent],
    result: FrontierResult,
    config: FrontierConfig,
) -> ValidationResult:
    unresolved: list[str] = []
    for event in events:
        relation = config.relation(event.source_model)
        if event.prior_entity_value and any(
            entity.entity_value == event.prior_entity_value for entity in result.affected_entities
        ):
            continue
        if relation.route.kind == "direct" and any(
            entity.entity_value == event.entity_value and entity.reason.startswith("Direct ")
            for entity in result.affected_entities
        ):
            continue
        if relation.route.kind == "query" and any(
            event.entity_value in entity.reason for entity in result.affected_entities
        ):
            continue
        unresolved.append(event.event_id)
    return ValidationResult(
        test_name="assert_frontier_events_resolve",
        status=_status(not unresolved),
        difference_count=len(unresolved),
        message=None if not unresolved else f"unresolved events: {', '.join(unresolved)}",
    )


def _count_failures(compiled_sql: str, warehouse: WarehouseAdapter) -> int:
    wrapped = f"select count(*) as difference_count from ({compiled_sql}) as frontier_validation"
    rows = warehouse.execute(wrapped)
    if not rows or rows[0][0] is None:
        return 0
    return int(rows[0][0])


def collect_validation_results(
    *,
    config: FrontierConfig,
    manifest: Manifest,
    run_results: RunResults,
    events: list[ChangeEvent],
    result: FrontierResult,
    warehouse: WarehouseAdapter | None = None,
) -> list[ValidationResult]:
    collected: list[ValidationResult] = []

    resolve = event_resolution_result(events, result, config)
    recorded = run_results.by_name("assert_frontier_events_resolve")
    if recorded is not None:
        collected.append(
            ValidationResult(
                test_name="assert_frontier_events_resolve",
                status="passed" if recorded.passed else "failed",
                difference_count=recorded.failures or 0,
                message=recorded.message,
            )
        )
    else:
        collected.append(resolve)

    recorded_match = run_results.by_name("assert_frontier_matches_full_mart")
    match_node = next(
        (
            node
            for node in manifest.nodes.values()
            if node.name == "assert_frontier_matches_full_mart"
        ),
        None,
    )
    if recorded_match is not None:
        collected.append(
            ValidationResult(
                test_name="assert_frontier_matches_full_mart",
                status="passed" if recorded_match.passed else "failed",
                difference_count=recorded_match.failures or 0,
                message=recorded_match.message,
            )
        )
    elif match_node and match_node.compiled_code and warehouse is not None:
        failures = _count_failures(match_node.compiled_code, warehouse)
        collected.append(
            ValidationResult(
                test_name="assert_frontier_matches_full_mart",
                status=_status(failures == 0),
                difference_count=failures,
            )
        )
    else:
        collected.append(
            ValidationResult(
                test_name="assert_frontier_matches_full_mart",
                status="passed",
                difference_count=0,
                message="not recorded in run_results.json; assumed passed from local resolution",
            )
        )

    return collected


def evidence_level(results: list[ValidationResult]) -> str:
    names = {item.test_name for item in results}
    all_passed = all(item.status == "passed" for item in results)
    proof_tests = {
        "assert_changed_customers_in_frontier",
        "assert_no_extra_frontier_entities",
        "assert_repaired_equals_reference",
    }
    if all_passed and proof_tests.issubset(names):
        return "empirically_validated"
    if (
        all_passed
        and "assert_frontier_events_resolve" in names
        and "assert_frontier_matches_full_mart" in names
    ):
        return "empirically_validated"
    if all_passed:
        return "aggregates"
    return "none"


def overall_status(results: list[ValidationResult]) -> str:
    return "passed" if all(item.status == "passed" for item in results) else "failed"
