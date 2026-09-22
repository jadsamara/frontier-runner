from __future__ import annotations

import threading

import pytest

from frontier.certification import (
    EXECUTION_FAILED,
    FULL_REFERENCE_VALIDATED,
    SQL_CERTIFIED,
    UNCERTIFIED,
    build_assessment_dimensions,
)
from frontier.execute import IsolatedRun, snapshot_execute
from frontier.snapshot import (
    ASSURANCE_ADAPTER,
    ASSURANCE_EXTERNAL,
    ASSURANCE_NONE,
    MODE_EXTERNAL,
    MODE_TIME_TRAVEL,
    SOURCE_RELATION_UNSUPPORTED,
    SOURCE_SNAPSHOT_NOT_PINNED,
    SnapshotError,
    SourceSnapshot,
    adapter_evidence_token,
    assert_snapshot_payload_is_safe,
    bind_sql_to_snapshot,
    capture_from_catalog,
    collect_source_relations,
    verify_snapshot_binding,
)
from frontier.warehouse import FakeWarehouse


def _verified_snapshot(identifier: str = "fake-snapshot-1") -> SourceSnapshot:
    snapshot = capture_from_catalog(
        ["analytics.dbt_dev.orders"],
        catalog={
            "analytics.dbt_dev.orders": {"kind": "permanent_table", "retention_days": 1},
        },
        identifier=identifier,
        captured_at="2026-09-21T16:00:00Z",
    )
    snapshot.record_phase("discovery")
    snapshot.record_phase("targeted_base")
    snapshot.record_phase("targeted_head")
    snapshot.record_phase("confirmation")
    snapshot.record_phase("full_reference")
    return snapshot


def test_all_phases_share_one_snapshot_identifier() -> None:
    warehouse = FakeWarehouse(
        {"orders": [(1,)]},
        relation_catalog={"analytics.dbt_dev.orders": {"kind": "permanent_table", "retention_days": 7}},
    )
    snapshot = warehouse.capture_snapshot(("analytics.dbt_dev.orders",))
    sql = "select customer_id from analytics.dbt_dev.orders"
    for phase in ("discovery", "targeted_base", "targeted_head", "confirmation", "full_reference"):
        snapshot_execute(warehouse, sql, snapshot, phase=phase)
    assert set(snapshot.phases.values()) == {snapshot.identifier}
    assert snapshot.consistent
    assert snapshot.assurance == ASSURANCE_ADAPTER


def test_one_unbound_relation_prevents_certification() -> None:
    snapshot = capture_from_catalog(
        ["analytics.dbt_dev.orders", "analytics.dbt_dev.missing"],
        catalog={"analytics.dbt_dev.orders": {"kind": "permanent_table", "retention_days": 1}},
        identifier="fake-snapshot-1",
        captured_at="2026-09-21T16:00:00Z",
    )
    assert snapshot.assurance == ASSURANCE_NONE
    assert snapshot.failure_code == SOURCE_SNAPSHOT_NOT_PINNED
    assert snapshot.relations_bound < snapshot.relations_checked
    dims = build_assessment_dimensions(snapshot=snapshot)
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["sourceSnapshot"]["failureCode"] == SOURCE_SNAPSHOT_NOT_PINNED


def test_unsupported_view_fails_closed_until_expanded() -> None:
    unresolved = capture_from_catalog(
        ["analytics.dbt_dev.stg_customers"],
        catalog={"analytics.dbt_dev.stg_customers": {"kind": "view"}},
        identifier="fake-snapshot-1",
        captured_at="2026-09-21T16:00:00Z",
    )
    assert unresolved.failure_code == SOURCE_RELATION_UNSUPPORTED
    assert unresolved.assurance == ASSURANCE_NONE

    resolved = capture_from_catalog(
        ["analytics.dbt_dev.stg_customers"],
        catalog={
            "analytics.dbt_dev.stg_customers": {
                "kind": "view",
                "view_sql": "select * from analytics.raw.raw_customers",
            },
            "analytics.raw.raw_customers": {"kind": "permanent_table", "retention_days": 1},
        },
        identifier="fake-snapshot-2",
        captured_at="2026-09-21T16:00:00Z",
    )
    bound = bind_sql_to_snapshot(
        "select * from analytics.dbt_dev.stg_customers",
        resolved,
    )
    assert "raw_customers" in bound.lower()
    assert "at (timestamp" in bound.lower()
    assert "to_timestamp_tz" in bound.lower()
    assert verify_snapshot_binding(bound, resolved)


def test_materialized_view_fails_closed() -> None:
    snapshot = capture_from_catalog(
        ["analytics.dbt_dev.mv_customers"],
        catalog={"analytics.dbt_dev.mv_customers": {"kind": "materialized view"}},
        identifier="fake-snapshot-1",
        captured_at="2026-09-21T16:00:00Z",
    )
    assert snapshot.failure_code == SOURCE_RELATION_UNSUPPORTED
    assert snapshot.allows_sql_certified() is False
    bound = bind_sql_to_snapshot("select * from analytics.dbt_dev.mv_customers", snapshot)
    assert "at (timestamp" not in bound.lower()


def test_different_snapshot_identifiers_between_phases_fail_closed() -> None:
    snapshot = _verified_snapshot("id-a")
    snapshot.record_phase("discovery", "id-a")
    snapshot.record_phase("targeted_base", "id-b")
    assert snapshot.consistent is False
    assert snapshot.allows_sql_certified() is False
    dims = build_assessment_dimensions(snapshot=snapshot, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED


def test_adapter_verified_requires_adapter_evidence() -> None:
    forged = SourceSnapshot(
        identifier="forged",
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at="2026-09-21T16:00:00Z",
        relation_bindings=_verified_snapshot().relation_bindings,
        adapter_evidence=None,
    )
    forged.record_phase("discovery")
    assert forged.allows_sql_certified() is False
    payload = forged.to_upload_payload()
    assert "adapterEvidence" not in payload
    assert payload["assurance"] == ASSURANCE_ADAPTER
    real = _verified_snapshot()
    assert real.adapter_evidence == adapter_evidence_token(real.identifier)
    assert real.allows_sql_certified()


def test_externally_attested_is_distinct_from_adapter_verified() -> None:
    snapshot = capture_from_catalog(
        ["analytics.dbt_dev.orders"],
        catalog={"analytics.dbt_dev.orders": {"kind": "permanent_table", "retention_days": 1}},
        identifier="attested-1",
        captured_at="2026-09-21T16:00:00Z",
        attestation_source="customer-dba-letter",
    )
    assert snapshot.assurance == ASSURANCE_EXTERNAL
    assert snapshot.mode == MODE_EXTERNAL
    assert snapshot.attestation_source == "customer-dba-letter"
    assert snapshot.allows_sql_certified() is False
    payload = snapshot.to_upload_payload()
    assert payload["attestationSource"] == "customer-dba-letter"
    assert payload["assurance"] != ASSURANCE_ADAPTER


def test_execution_failure_does_not_overwrite_certification() -> None:
    snapshot = _verified_snapshot()
    dims = build_assessment_dimensions(
        snapshot=snapshot,
        static_certified=True,
        execution_failed=True,
        execution_ran=True,
    )
    assert dims["certification"]["status"] == SQL_CERTIFIED
    assert dims["execution"]["status"] == EXECUTION_FAILED


def test_full_reference_validation_does_not_overwrite_certification() -> None:
    snapshot = _verified_snapshot()
    dims = build_assessment_dimensions(
        snapshot=snapshot,
        static_certified=True,
        full_reference_validated=True,
        execution_ran=True,
        candidates_confirmed=True,
    )
    assert dims["certification"]["status"] == SQL_CERTIFIED
    assert dims["validation"]["status"] == FULL_REFERENCE_VALIDATED


def test_legacy_payload_without_snapshot_stays_uncertified() -> None:
    dims = build_assessment_dimensions(snapshot=None, execution_failed=True)
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["sourceSnapshot"]["assurance"] == ASSURANCE_NONE
    assert dims["execution"]["status"] == EXECUTION_FAILED
    assert dims["certification"].get("failureCode") == SOURCE_SNAPSHOT_NOT_PINNED


def test_snapshot_payload_rejects_secrets_and_entity_ids() -> None:
    snapshot = _verified_snapshot()
    payload = snapshot.to_upload_payload()
    assert_snapshot_payload_is_safe(payload)
    assert "password" not in str(payload).lower()
    assert "customer_id" not in str(payload).lower()
    with pytest.raises(Exception):
        assert_snapshot_payload_is_safe({"password": "secret"})


def test_isolated_tables_are_not_time_traveled() -> None:
    snapshot = _verified_snapshot()
    sql = (
        "create or replace table analytics.dbt_ci.FRONTIER_RUN_AFFECTED_KEYS as "
        "select customer_id from analytics.dbt_dev.orders"
    )
    bound = bind_sql_to_snapshot(sql, snapshot)
    assert "FRONTIER_RUN_AFFECTED_KEYS" in bound
    assert bound.lower().count("at (timestamp") == 1
    assert "orders" in bound.lower()


def test_concurrent_assessments_do_not_share_snapshot_bindings() -> None:
    warehouse = FakeWarehouse(
        relation_catalog={"analytics.dbt_dev.orders": {"kind": "permanent_table", "retention_days": 1}}
    )
    first = warehouse.capture_snapshot(("analytics.dbt_dev.orders",))
    second = warehouse.capture_snapshot(("analytics.dbt_dev.orders",))
    assert first.identifier != second.identifier
    sql = "select * from analytics.dbt_dev.orders"
    bound_first = warehouse.bind_query_to_snapshot(sql, first)
    bound_second = warehouse.bind_query_to_snapshot(sql, second)
    assert first.identifier in bound_first
    assert second.identifier in bound_second
    assert first.identifier not in bound_second
    assert second.identifier not in bound_first
    assert warehouse.verify_snapshot_binding(bound_first, first)
    assert not warehouse.verify_snapshot_binding(bound_first, second)

    errors: list[str] = []

    def run(snapshot: SourceSnapshot) -> None:
        try:
            bound = warehouse.bind_query_to_snapshot(sql, snapshot)
            if snapshot.identifier not in bound:
                errors.append("missing identifier")
            if not warehouse.verify_snapshot_binding(bound, snapshot):
                errors.append("verify failed")
        except Exception as error:
            errors.append(str(error))

    threads = [
        threading.Thread(target=run, args=(first,)),
        threading.Thread(target=run, args=(second,)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_isolated_run_records_the_same_snapshot_on_source_phases() -> None:
    warehouse = FakeWarehouse(
        {
            "frontier_origin_counts": [(0, 2, 2)],
            "confirmed_frontier_count": [(2,)],
        },
        relation_catalog={"stg_orders": {"kind": "permanent_table", "retention_days": 1}},
    )
    snapshot = warehouse.capture_snapshot(("stg_orders",))
    session = IsolatedRun(
        warehouse=warehouse,
        relation="ANALYTICS.DBT_CI.FRONTIER_T_AFFECTED_KEYS",
        database="ANALYTICS",
        schema="DBT_CI",
        run_id="t",
        entity_key="customer_id",
        snapshot=snapshot,
    )
    session.materialize(sql_change_queries=("select customer_id from stg_orders",))
    assert snapshot.phases["candidate_materialization"] == snapshot.identifier
    executed = "\n".join(warehouse.executed)
    assert snapshot.identifier in executed
    assert "password" not in executed.lower()


def test_unbound_verify_raises_on_adapter_verified_execute() -> None:
    snapshot = _verified_snapshot()
    warehouse = FakeWarehouse({"select 1": [(1,)]})
    with pytest.raises(SnapshotError):
        snapshot_execute(warehouse, "select 1 from analytics.dbt_dev.other", snapshot, phase="discovery")


def test_collect_source_relations_skips_ctes_and_isolated_tables() -> None:
    sql = """
    with keys as (select customer_id from ANALYTICS.DBT_CI.FRONTIER_T_AFFECTED_KEYS)
    select * from analytics.dbt_dev.orders
    join keys on keys.customer_id = orders.customer_id
    """
    names = collect_source_relations(sql)
    assert any("orders" in name.lower() for name in names)
    assert not any("affected_keys" in name.lower() for name in names)


def test_missing_confirmation_phase_prevents_certification_assurance() -> None:
    snapshot = _verified_snapshot()
    snapshot.phases.pop("confirmation", None)
    assert snapshot.certification_phases_bound() is False
    assert snapshot.allows_sql_certified() is False
    dims = build_assessment_dimensions(snapshot=snapshot, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED


def test_different_confirmation_snapshot_prevents_certification_assurance() -> None:
    snapshot = _verified_snapshot("id-a")
    snapshot.record_phase("confirmation", "id-other")
    assert snapshot.consistent is False
    assert snapshot.allows_sql_certified() is False
    dims = build_assessment_dimensions(snapshot=snapshot, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["sourceSnapshot"]["assurance"] == ASSURANCE_ADAPTER


def test_confirmation_binds_the_21a_snapshot_on_targeted_sql() -> None:
    warehouse = FakeWarehouse(
        {"confirmed_frontier_count": [(2,)]},
        relation_catalog={"stg_orders": {"kind": "permanent_table", "retention_days": 1}},
    )
    snapshot = warehouse.capture_snapshot(("stg_orders",))
    snapshot.record_phase("discovery")
    session = IsolatedRun(
        warehouse=warehouse,
        relation="ANALYTICS.DBT_CI.FRONTIER_T_AFFECTED_KEYS",
        database="ANALYTICS",
        schema="DBT_CI",
        run_id="t",
        entity_key="customer_id",
        snapshot=snapshot,
    )
    confirmed = session.confirm(
        before_sql="select customer_id, 1 as total_orders from stg_orders where order_status = 'F'",
        after_sql="select customer_id, 1 as total_orders from stg_orders where order_status in ('F', 'O')",
    )
    assert confirmed == ()
    assert snapshot.phases["targeted_base"] == snapshot.identifier
    assert snapshot.phases["targeted_head"] == snapshot.identifier
    assert snapshot.phases["confirmation"] == snapshot.identifier
    executed = "\n".join(warehouse.executed).lower()
    assert "at (timestamp" in executed or "at(timestamp" in executed
    assert "frontier_" in executed
    assert snapshot.allows_sql_certified()
    assert snapshot.assurance == ASSURANCE_ADAPTER


def test_confirmation_without_snapshot_does_not_invent_adapter_verified() -> None:
    warehouse = FakeWarehouse({"confirmed_frontier_count": [(2,)]})
    session = IsolatedRun(
        warehouse=warehouse,
        relation="ANALYTICS.DBT_CI.FRONTIER_T_AFFECTED_KEYS",
        database="ANALYTICS",
        schema="DBT_CI",
        run_id="t",
        entity_key="customer_id",
        snapshot=None,
    )
    confirmed = session.confirm(
        before_sql="select customer_id, 1 as total_orders from stg_orders where order_status = 'F'",
        after_sql="select customer_id, 1 as total_orders from stg_orders where order_status in ('F', 'O')",
    )
    assert confirmed == ()
    assert session.snapshot is None
    executed = "\n".join(warehouse.executed).lower()
    assert "at (timestamp" not in executed
    dims = build_assessment_dimensions(snapshot=None, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["sourceSnapshot"]["assurance"] != ASSURANCE_ADAPTER
