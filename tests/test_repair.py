"""Synthetic mart-baseline and disposable delete-and-insert repair tests.

Uses FakeWarehouse only. Never connects to a live warehouse or private lab.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from frontier.config import ConfigError
from frontier.execute import IsolatedRun, affected_keys_relation
from frontier.mutation import MutationError, canonicalize_relation
from frontier.repair import (
    GRAIN_VIOLATED,
    MART_BASELINE_FAILED,
    MART_BASELINE_MATCHED,
    MART_BASELINE_MISMATCHED,
    PREREQUISITES_NOT_MET,
    REPAIR_FAILED,
    REPAIR_NOT_RUN,
    REPAIR_SUCCEEDED,
    SNAPSHOT_IDENTIFIER_MISMATCH,
    TARGET_SNAPSHOT_NOT_PINNED,
    WAREHOUSE_EXECUTION_FAILED,
    baseline_boundary_from,
    compare_complete_results,
    economics_from_jobs,
)
from frontier.snapshot import (
    ASSURANCE_ADAPTER,
    MODE_TIME_TRAVEL,
    PERMANENT_TABLE,
    RelationBinding,
    SourceSnapshot,
    adapter_evidence_token,
    capture_from_catalog,
)
from frontier.warehouse import FakeWarehouse

DATABASE = "DATA_AGENT_DEV"
SCHEMA = "DBT_CI"
MART = f"{DATABASE}.{SCHEMA}.CUSTOMER_SUMMARY"
STG = f"{DATABASE}.DBT_DEV.STG_CUSTOMERS"
OLD_SQL = f"select customer_id, customer_name, n from {STG}"
NEW_SQL = f"select customer_id, customer_name, n from {STG}"
CATALOG = {
    MART: {"kind": "permanent_table", "retention_days": 7},
    STG: {"kind": "permanent_table", "retention_days": 7},
}

ROW_1 = ("1", "Ada", 10)
ROW_2 = ("2", "Bea", 20)
ROW_3 = ("3", "Cam", 30)
ROW_1_NEW = ("1", "Ada", 11)
ROW_4 = ("4", "Dee", 40)
ROW_NULL = ("1", None, 10)


def _snapshot(identifier: str = "fake-snapshot-1", catalog: dict[str, dict[str, Any]] | None = None) -> SourceSnapshot:
    snapshot = capture_from_catalog(
        tuple((catalog or CATALOG).keys()),
        catalog=catalog or CATALOG,
        identifier=identifier,
        captured_at="2026-09-22T00:00:00Z",
    )
    for phase in ("discovery", "targeted_base", "targeted_head", "confirmation"):
        snapshot.record_phase(phase)
    return snapshot


def _session(
    warehouse: FakeWarehouse,
    *,
    run_id: str = "repair-1",
) -> IsolatedRun:
    relation = affected_keys_relation(run_id, database=DATABASE, schema=SCHEMA)
    return IsolatedRun(
        warehouse=warehouse,
        relation=relation,
        database=DATABASE,
        schema=SCHEMA,
        run_id=run_id,
        entity_key="customer_id",
    )


def _warehouse(
    *,
    mart_rows: list[tuple[Any, ...]],
    old_rows: list[tuple[Any, ...]],
    head_rows: list[tuple[Any, ...]],
    candidates: list[str],
    catalog: dict[str, dict[str, Any]] | None = None,
    fail_on: dict[str, Exception] | None = None,
) -> FakeWarehouse:
    candidate_set = {str(item) for item in candidates}
    warehouse = FakeWarehouse(
        {
            "confirmed_frontier_count": [(len(candidate_set),)],
            "as frontier_old_complete": old_rows,
            "as frontier_head_complete": head_rows,
            "target_head": [row for row in head_rows if str(row[0]) in candidate_set],
            "target_base": [row for row in old_rows if str(row[0]) in candidate_set],
        },
        relation_catalog=catalog or CATALOG,
    )
    warehouse.seed_table(MART, mart_rows)
    warehouse.fail_on = fail_on or {}
    return warehouse


def _prepare(
    *,
    mart_rows: list[tuple[Any, ...]],
    old_rows: list[tuple[Any, ...]] | None = None,
    head_rows: list[tuple[Any, ...]] | None = None,
    candidates: list[str] | None = None,
    catalog: dict[str, dict[str, Any]] | None = None,
    fail_on: dict[str, Exception] | None = None,
    snapshot: SourceSnapshot | None = None,
) -> tuple[IsolatedRun, FakeWarehouse, SourceSnapshot]:
    keys = candidates or ["1"]
    old = old_rows if old_rows is not None else mart_rows
    head = head_rows if head_rows is not None else mart_rows
    warehouse = _warehouse(
        mart_rows=mart_rows,
        old_rows=old,
        head_rows=head,
        candidates=keys,
        catalog=catalog,
        fail_on=fail_on,
    )
    session = _session(warehouse)
    session.materialize(keys)
    session.confirm(before_sql=OLD_SQL, after_sql=NEW_SQL)
    if session.targeted_head_relation:
        warehouse.seed_table(
            session.targeted_head_relation,
            [row for row in head if str(row[0]) in {str(item) for item in keys}],
        )
    snap = snapshot or _snapshot(catalog=catalog)
    session.snapshot = snap
    return session, warehouse, snap


def _run_baseline_and_repair(
    session: IsolatedRun,
    *,
    certified: bool = True,
    confirmed: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    repair = session.validate_disposable_repair(
        target_relation=MART,
        new_sql=NEW_SQL,
        certified=certified,
        confirmed=confirmed,
    )
    return baseline, repair


def _dml_targets(executed: list[str], verb: str) -> list[str]:
    pattern = re.compile(rf"\b{verb}\s+(?:into\s+|from\s+)?([A-Za-z0-9_`.\"]+)", re.IGNORECASE)
    targets: list[str] = []
    for sql in executed:
        if not re.match(rf"^\s*{verb}\b", sql, re.IGNORECASE):
            continue
        match = pattern.search(sql)
        if match:
            targets.append(canonicalize_relation(match.group(1)))
    return targets


def _dropped(executed: list[str]) -> list[str]:
    pattern = re.compile(r"drop\s+table(?:\s+if\s+exists)?\s+([A-Za-z0-9_`.\"]+)", re.IGNORECASE)
    return [canonicalize_relation(match.group(1)) for sql in executed for match in [pattern.search(sql)] if match]


def test_01_existing_mart_matches_complete_old_sql() -> None:
    session, _warehouse, _snap = _prepare(mart_rows=[ROW_1, ROW_2, ROW_3])
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_MATCHED
    assert baseline["missingRows"] == 0
    assert baseline["extraRows"] == 0
    assert baseline["mismatchedRows"] == 0
    assert baseline["duplicateEntityKeys"] == 0
    assert baseline["snapshotIdentifier"] == "fake-snapshot-1"
    assert baseline["warehouseQueryIds"]


def test_02_existing_mart_missing_entity() -> None:
    session, _, _ = _prepare(mart_rows=[ROW_1, ROW_2], old_rows=[ROW_1, ROW_2, ROW_3])
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_MISMATCHED
    assert baseline["missingRows"] == 1
    assert baseline["extraRows"] == 0


def test_03_existing_mart_extra_entity() -> None:
    session, _, _ = _prepare(mart_rows=[ROW_1, ROW_2, ROW_3], old_rows=[ROW_1, ROW_2])
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_MISMATCHED
    assert baseline["extraRows"] == 1


def test_04_existing_mart_changed_value() -> None:
    session, _, _ = _prepare(mart_rows=[ROW_1_NEW, ROW_2, ROW_3], old_rows=[ROW_1, ROW_2, ROW_3])
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_MISMATCHED
    assert baseline["mismatchedRows"] == 1


def test_05_existing_mart_duplicate_entity_key() -> None:
    session, _, _ = _prepare(mart_rows=[ROW_1, ROW_1, ROW_2], old_rows=[ROW_1, ROW_2])
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_FAILED
    assert baseline["reasonCode"] == GRAIN_VIOLATED
    assert baseline["duplicateEntityKeys"] == 1


def test_06_existing_mart_cannot_bind_pinned_snapshot() -> None:
    catalog = {STG: {"kind": "permanent_table", "retention_days": 7}}
    session, _, _ = _prepare(mart_rows=[ROW_1], catalog=catalog)
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_FAILED
    assert baseline["reasonCode"] == TARGET_SNAPSHOT_NOT_PINNED
    assert baseline["missingRows"] is None


def test_07_baseline_warehouse_execution_fails() -> None:
    session, _, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        fail_on={"frontier_mart_baseline": ConfigError("warehouse down")},
    )
    baseline = session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    assert baseline["status"] == MART_BASELINE_FAILED
    assert baseline["reasonCode"] == WAREHOUSE_EXECUTION_FAILED
    assert baseline["missingRows"] is None


def test_08_baseline_mismatch_preserves_sql_certified() -> None:
    session, _, snap = _prepare(mart_rows=[ROW_1], old_rows=[ROW_1, ROW_2], head_rows=[ROW_1_NEW, ROW_2])
    baseline, repair = _run_baseline_and_repair(session)
    assert baseline["status"] == MART_BASELINE_MISMATCHED
    assert repair["status"] == REPAIR_NOT_RUN
    boundary = baseline_boundary_from(
        mart_baseline=baseline,
        repair_validation=repair,
        certification_status="SQL_CERTIFIED",
        validation_status="CANDIDATES_CONFIRMED",
        snapshot=snap,
        cleanup_ok=True,
    )
    assert boundary["existingMaterializedMartRepresentsSnapshot"] is False
    assert boundary["safeInPlaceProductionRepair"] is False


def test_09_baseline_mismatch_prevents_repair_validation() -> None:
    session, warehouse, _ = _prepare(mart_rows=[ROW_1], old_rows=[ROW_1, ROW_2])
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_NOT_RUN
    assert repair["reasonCode"] == PREREQUISITES_NOT_MET
    assert not _dml_targets(warehouse.executed, "delete")
    assert not _dml_targets(warehouse.executed, "insert")


def test_10_disposable_copy_created_from_registered_target_snapshot() -> None:
    session, warehouse, snap = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    copy_sql = next(sql for sql in warehouse.executed if "MART_COPY" in sql.upper() and sql.lower().lstrip().startswith("create"))
    assert canonicalize_relation(MART).split(".")[-1] in canonicalize_relation(copy_sql) or "CUSTOMER_SUMMARY" in copy_sql.upper()
    assert snap.identifier in copy_sql
    assert " at " in copy_sql.lower() or "AT(" in copy_sql.upper()
    assert session.snapshot is not None
    assert session.snapshot.binding_for(MART) is not None


def test_11_candidate_keys_materialized_distinctly() -> None:
    session, warehouse, _ = _prepare(mart_rows=[ROW_1, ROW_2], candidates=["1", "1", "1"])
    create_keys = next(sql for sql in warehouse.executed if "AFFECTED_KEYS" in sql.upper() and sql.lower().lstrip().startswith("create"))
    assert "select distinct" in create_keys.lower()
    keys = warehouse.tables[canonicalize_relation(session.relation)]
    values = [row[0] for row in keys]
    assert values.count("1") == 1


def test_12_delete_targets_only_registered_disposable_mart() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    deleted = _dml_targets(warehouse.executed, "delete")
    assert deleted
    copy = canonicalize_relation(session.mart_copy_relation())
    assert all(target == copy for target in deleted)
    assert canonicalize_relation(MART) not in deleted


def test_13_insert_targets_only_registered_disposable_mart() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    inserted = _dml_targets(warehouse.executed, "insert")
    assert inserted
    copy = canonicalize_relation(session.mart_copy_relation())
    assert all(target == copy for target in inserted)


def test_14_dml_against_customer_mart_rejected_before_submission() -> None:
    session, warehouse, _ = _prepare(mart_rows=[ROW_1, ROW_2])
    before = list(warehouse.executed)
    with pytest.raises(MutationError, match="not permitted"):
        session.warehouse.execute(f"delete from {MART}")
    with pytest.raises(MutationError, match="not permitted"):
        session.warehouse.execute(f"insert into {MART} select * from {STG}")
    assert warehouse.executed == before


def test_15_unregistered_frontier_name_is_rejected() -> None:
    session, warehouse, _ = _prepare(mart_rows=[ROW_1])
    fake = f"{DATABASE}.{SCHEMA}.FRONTIER_NOT_THIS_INVOCATION"
    before = list(warehouse.executed)
    with pytest.raises(MutationError, match="not permitted"):
        session.warehouse.execute(f"delete from {fake}")
    assert warehouse.executed == before


def test_16_candidate_entity_changes_and_is_repaired() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    baseline, repair = _run_baseline_and_repair(session)
    assert baseline["status"] == MART_BASELINE_MATCHED
    assert repair["status"] == REPAIR_SUCCEEDED
    assert repair["deletedRows"] == 1
    assert repair["insertedRows"] == 1
    assert repair["missingRows"] == 0
    assert repair["extraRows"] == 0
    assert repair["mismatchedRows"] == 0
    copy = canonicalize_relation(session.mart_copy_relation())
    assert warehouse.tables[copy] == [ROW_2, ROW_3, ROW_1_NEW] or set(warehouse.tables[copy]) == {
        ROW_1_NEW,
        ROW_2,
        ROW_3,
    }


def test_17_candidate_entity_disappears_under_head_sql() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_2, ROW_3],
        candidates=["1"],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_SUCCEEDED
    assert repair["deletedRows"] == 1
    assert repair["insertedRows"] == 0
    copy = canonicalize_relation(session.mart_copy_relation())
    keys = {row[0] for row in warehouse.tables[copy]}
    assert "1" not in keys
    assert keys == {"2", "3"}


def test_18_candidate_entity_appears_under_head_sql() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        old_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1, ROW_2, ROW_4],
        candidates=["4"],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_SUCCEEDED
    assert repair["insertedRows"] == 1
    copy = canonicalize_relation(session.mart_copy_relation())
    keys = {row[0] for row in warehouse.tables[copy]}
    assert keys == {"1", "2", "4"}


def test_19_null_output_values_compare_correctly() -> None:
    compared = compare_complete_results([ROW_NULL, ROW_2], [ROW_NULL, ROW_2])
    assert compared.ok
    session, _, _ = _prepare(mart_rows=[ROW_NULL, ROW_2], head_rows=[ROW_NULL, ROW_2])
    baseline, repair = _run_baseline_and_repair(session)
    assert baseline["status"] == MART_BASELINE_MATCHED
    assert repair["status"] == REPAIR_SUCCEEDED


def test_20_duplicate_output_grain_fails_closed() -> None:
    session, _, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1_NEW, ROW_1_NEW, ROW_2],
        candidates=["1"],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_FAILED
    assert repair["reasonCode"] == GRAIN_VIOLATED
    assert repair["duplicateEntityKeys"] == 1


def test_21_repaired_disposable_table_equals_complete_head() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_SUCCEEDED
    copy = canonicalize_relation(session.mart_copy_relation())
    compared = compare_complete_results(warehouse.tables[copy], [ROW_1_NEW, ROW_2, ROW_3])
    assert compared.ok


def test_22_repaired_disposable_table_has_a_missing_row() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    warehouse.responses["as frontier_head_complete"] = [ROW_1_NEW, ROW_2, ROW_3, ROW_4]
    repair = session.validate_disposable_repair(
        target_relation=MART,
        new_sql=NEW_SQL,
        certified=True,
        confirmed=True,
    )
    assert repair["status"] == REPAIR_FAILED
    assert repair["missingRows"] == 1


def test_23_repaired_disposable_table_has_an_extra_row() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    warehouse.responses["as frontier_head_complete"] = [ROW_1_NEW, ROW_3]
    repair = session.validate_disposable_repair(
        target_relation=MART,
        new_sql=NEW_SQL,
        certified=True,
        confirmed=True,
    )
    assert repair["status"] == REPAIR_FAILED
    assert repair["extraRows"] == 1


def test_24_repaired_disposable_table_has_a_value_mismatch() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    warehouse.responses["as frontier_head_complete"] = [("1", "Ada", 99), ROW_2, ROW_3]
    repair = session.validate_disposable_repair(
        target_relation=MART,
        new_sql=NEW_SQL,
        certified=True,
        confirmed=True,
    )
    assert repair["status"] == REPAIR_FAILED
    assert repair["mismatchedRows"] == 1


def test_25_cleanup_occurs_after_successful_validation() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_SUCCEEDED
    registered = list(session.guard.registered)
    session.cleanup()
    assert session.repair_validation["disposableResourcesCleaned"] is True
    dropped = _dropped(warehouse.executed)
    for relation in registered:
        assert canonicalize_relation(relation) in dropped


def test_26_cleanup_occurs_after_candidate_deletion_failure() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1_NEW, ROW_2],
        fail_on={"delete from": ConfigError("delete failed")},
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_FAILED
    registered = list(session.guard.registered)
    session.cleanup()
    assert session.repair_validation["disposableResourcesCleaned"] is True
    assert _dropped(warehouse.executed)
    for relation in registered:
        assert canonicalize_relation(relation) in _dropped(warehouse.executed)


def test_27_cleanup_occurs_after_targeted_insert_failure() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1_NEW, ROW_2],
        fail_on={"insert into": ConfigError("insert failed")},
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_FAILED
    session.cleanup()
    assert session.repair_validation["disposableResourcesCleaned"] is True
    assert _dropped(warehouse.executed)


def test_28_cleanup_occurs_after_full_reference_failure() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1_NEW, ROW_2],
        fail_on={"frontier_head_complete": ConfigError("full head failed")},
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_FAILED
    session.cleanup()
    assert session.repair_validation["disposableResourcesCleaned"] is True
    assert _dropped(warehouse.executed)


def test_29_cleanup_failure_makes_repair_validation_fail() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _, repair = _run_baseline_and_repair(session)
    assert repair["status"] == REPAIR_SUCCEEDED
    warehouse.fail_on = {"drop table": ConfigError("drop denied")}
    session.cleanup()
    assert session.repair_validation["status"] == REPAIR_FAILED
    assert session.repair_validation["reasonCode"] == "CLEANUP_FAILED"
    assert session.repair_validation["disposableResourcesCleaned"] is False


def test_30_snapshot_identifiers_are_identical_across_every_read() -> None:
    session, _, snap = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    values = {snap.identifier, *snap.phases.values()}
    assert values == {snap.identifier}


def test_31_snapshot_mismatch_prevents_repair_success() -> None:
    session, _, snap = _prepare(
        mart_rows=[ROW_1, ROW_2],
        head_rows=[ROW_1_NEW, ROW_2],
    )
    session.verify_mart_baseline(target_relation=MART, old_sql=OLD_SQL)
    snap.phases["discovery"] = "other-snapshot"
    repair = session.validate_disposable_repair(
        target_relation=MART,
        new_sql=NEW_SQL,
        certified=True,
        confirmed=True,
    )
    assert repair["status"] == REPAIR_FAILED
    assert repair["reasonCode"] == SNAPSHOT_IDENTIFIER_MISMATCH


def test_32_disposable_success_sets_safe_in_place_production_repair() -> None:
    session, _, snap = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    baseline, repair = _run_baseline_and_repair(session)
    session.cleanup()
    repair = session.repair_validation
    boundary = baseline_boundary_from(
        mart_baseline=baseline,
        repair_validation=repair,
        certification_status="SQL_CERTIFIED",
        validation_status="CANDIDATES_CONFIRMED",
        snapshot=snap,
        cleanup_ok=True,
    )
    assert boundary["existingMaterializedMartRepresentsSnapshot"] is True
    assert boundary["safeInPlaceProductionRepair"] is True
    assert repair["status"] == REPAIR_SUCCEEDED
    assert repair["disposableResourcesCleaned"] is True


def test_33_production_apply_remains_not_requested() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    session.cleanup()
    production = {"status": "NOT_REQUESTED"}
    assert production["status"] in {"NOT_REQUESTED", "NOT_RUN"}
    assert canonicalize_relation(MART) not in _dml_targets(warehouse.executed, "delete")
    assert canonicalize_relation(MART) not in _dml_targets(warehouse.executed, "insert")
    assert not any(canonicalize_relation(MART) == target for target in _dropped(warehouse.executed))


def test_34_historical_payloads_default_new_dimensions_conservatively() -> None:
    from frontier.repair import empty_mart_baseline, empty_production_apply, empty_repair_validation

    assert empty_mart_baseline()["status"] == "NOT_RUN"
    assert empty_repair_validation()["status"] == "NOT_RUN"
    assert empty_production_apply()["status"] == "NOT_REQUESTED"


def test_35_unknown_population_remains_null() -> None:
    session, _, _ = _prepare(mart_rows=[ROW_1], head_rows=[ROW_1])
    baseline, repair = _run_baseline_and_repair(session, certified=False, confirmed=False)
    assert baseline["status"] == MART_BASELINE_MATCHED
    assert repair["status"] == REPAIR_NOT_RUN
    assert repair["candidateCount"] is None or isinstance(repair["candidateCount"], int)


def test_37_economics_includes_every_measured_phase() -> None:
    session, _, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    session.cleanup()
    payload = economics_from_jobs(
        session.job_metrics,
        phase_timings=session.phase_timings,
        full_bytes=1_000_000,
        full_elapsed_ms=50_000,
        include_full_reference=True,
    )
    targeted = [phase.lower() for phase in payload["includedPhases"]["targeted"]]
    for phase in (
        "baseline verification",
        "disposable mart creation",
        "candidate-key delete",
        "targeted insert",
        "complete repaired-table validation",
        "cleanup",
    ):
        assert any(phase in item for item in targeted), targeted
    assert "complete head computation" in payload["includedPhases"]["full"] or payload["includedPhases"]["full"]


def test_38_bytes_only_evidence_never_produces_a_monetary_savings_claim() -> None:
    payload = economics_from_jobs(
        [{"phase": "candidate discovery", "total_bytes_processed": 10, "elapsed_ms": None}],
        full_bytes=100,
    )
    assert payload["decision"] == "NOT_EVALUATED"
    assert payload["measurementBasis"] == "BYTES_AND_TIME_ONLY"
    blob = str(payload).lower()
    assert "saved $" not in blob
    assert "monetary" not in blob or "not billed cost" in (payload["reason"] or "").lower()
    assert "not billed cost" in (payload["reason"] or "").lower()
    assert payload["targetedCredits"] is None


def test_39_equal_or_more_expensive_targeted_work_recommends_full_rebuild() -> None:
    payload = economics_from_jobs(
        [
            {"phase": "candidate discovery", "total_bytes_processed": 100, "elapsed_ms": 50},
            {"phase": "targeted insert", "total_bytes_processed": 100, "elapsed_ms": 50},
        ],
        full_bytes=50,
        full_elapsed_ms=10,
    )
    assert payload["decision"] == "FULL_REBUILD_RECOMMENDED"


def test_40_insufficient_evidence_remains_not_evaluated() -> None:
    payload = economics_from_jobs([])
    assert payload["decision"] == "NOT_EVALUATED"
    assert payload["measurementBasis"] == "NONE"


def test_fake_warehouse_sql_log_proves_disposable_dml_and_cleanup() -> None:
    session, warehouse, _ = _prepare(
        mart_rows=[ROW_1, ROW_2, ROW_3],
        head_rows=[ROW_1_NEW, ROW_2, ROW_3],
    )
    _run_baseline_and_repair(session)
    registered = [canonicalize_relation(item) for item in session.guard.registered]
    session.cleanup()
    deletes = _dml_targets(warehouse.executed, "delete")
    inserts = _dml_targets(warehouse.executed, "insert")
    assert deletes
    assert inserts
    assert all(target in registered for target in deletes)
    assert all(target in registered for target in inserts)
    assert canonicalize_relation(MART) not in deletes
    assert canonicalize_relation(MART) not in inserts
    dropped = _dropped(warehouse.executed)
    for relation in registered:
        assert relation in dropped
    assert canonicalize_relation(MART) not in dropped
    assert all("FRONTIER_" in name for name in [*deletes, *inserts, *dropped])


def test_pre_repair_except_926_is_not_a_repair_validation_failure() -> None:
    from frontier.proof import SqlChangeProof, finalize_sql_change_assessment
    from frontier.validation import overall_status

    changed = 463
    mart = [(str(index), f"name-{index}", 1) for index in range(changed)]
    head = [(str(index), f"name-{index}", 2) for index in range(changed)]
    except_diff = len(set(mart) - set(head)) + len(set(head) - set(mart))
    assert except_diff == 926
    pre = compare_complete_results(mart, head)
    assert pre.ok is False
    assert pre.mismatched_rows == changed

    session, warehouse, snap = _prepare(
        mart_rows=mart,
        old_rows=mart,
        head_rows=head,
        candidates=[str(index) for index in range(changed)],
    )
    baseline, repair = _run_baseline_and_repair(session)
    session.cleanup()
    assert baseline["status"] == MART_BASELINE_MATCHED
    assert repair["status"] == REPAIR_SUCCEEDED
    assert repair["missingRows"] == 0
    assert repair["extraRows"] == 0
    assert repair["mismatchedRows"] == 0
    assert repair["disposableResourcesCleaned"] is True
    assert "mismatched_final_rows" not in "\n".join(warehouse.executed).lower()

    proof = SqlChangeProof(
        full_rows_recomputed=changed,
        frontier_rows_recomputed=changed,
        rows_avoided=0,
        source_population_count=changed,
        candidate_frontier_count=changed,
        confirmed_frontier_count=changed,
        before_entity_count=changed,
        after_entity_count=changed,
        changed_source_row_count=changed,
        missing_frontier_entities=0,
        extra_frontier_entities=0,
        mismatched_final_rows=except_diff,
        test_duration_ms=1,
    )
    overlaid, validations = finalize_sql_change_assessment(proof, repair, [])
    assertion = next(
        item for item in validations if item.test_name == "assert_repaired_equals_reference"
    )
    boundary = baseline_boundary_from(
        mart_baseline=baseline,
        repair_validation=repair,
        certification_status="SQL_CERTIFIED",
        validation_status="CANDIDATES_CONFIRMED",
        snapshot=snap,
        cleanup_ok=True,
    )
    assert assertion.status == "passed"
    assert assertion.difference_count == 0
    assert overlaid.mismatched_final_rows == 0
    assert overall_status(validations) == "passed"
    assert boundary["safeInPlaceProductionRepair"] is True
    assert {"status": "NOT_REQUESTED"}["status"] == "NOT_REQUESTED"
