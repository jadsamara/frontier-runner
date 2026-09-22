"""Mutation guard: disposable FRONTIER_* DML only, fail closed before SQL submission."""

from __future__ import annotations

import pytest

from frontier.mutation import (
    DELETE,
    INSERT,
    MutationError,
    MutationGuard,
    build_guard,
    canonicalize_relation,
    parse_mutation,
)
from frontier.execute import affected_keys_relation
from frontier.warehouse import FakeWarehouse


def test_parse_mutation_classifies_disposable_operations() -> None:
    assert parse_mutation("select 1") is None
    assert parse_mutation("create table DATA_AGENT_DEV.DBT_CI.FRONTIER_A as select 1").operation == "CREATE_TABLE_AS_SELECT"
    assert parse_mutation("delete from DATA_AGENT_DEV.DBT_CI.FRONTIER_A").operation == DELETE
    assert parse_mutation("insert into DATA_AGENT_DEV.DBT_CI.FRONTIER_A select * from DATA_AGENT_DEV.DBT_CI.FRONTIER_B").operation == INSERT
    assert parse_mutation("drop table DATA_AGENT_DEV.DBT_CI.FRONTIER_A").operation == "DROP_TABLE"
    assert parse_mutation("update DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY set n = 1").operation == "UPDATE"
    assert parse_mutation("merge into DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY using dual").operation == "MERGE"
    assert parse_mutation("truncate table DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY").operation == "TRUNCATE"
    assert parse_mutation("alter table DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY add column x int").operation == "ALTER"


def test_guard_rejects_customer_and_unregistered_frontier_names() -> None:
    keys = affected_keys_relation("guard-1", database="DATA_AGENT_DEV", schema="DBT_CI")
    guard = build_guard(
        run_id="guard-1",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
        keys_relation=keys,
    )
    with pytest.raises(MutationError, match="not permitted"):
        guard.assert_sql_allowed("delete from DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY")
    with pytest.raises(MutationError, match="not permitted"):
        guard.assert_sql_allowed("insert into DATA_AGENT_DEV.DBT_CI.FRONTIER_OTHER select 1")
    owned = canonicalize_relation(keys)
    guard.register(owned)
    guard.mark_created(owned)
    parsed = guard.assert_sql_allowed(f"delete from {keys}")
    assert parsed is not None
    assert parsed.operation == DELETE


def test_guarded_warehouse_does_not_submit_rejected_sql() -> None:
    from frontier.execute import IsolatedRun

    warehouse = FakeWarehouse({"confirmed_frontier_count": [(0,)]})
    session = IsolatedRun(
        warehouse=warehouse,
        relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_GUARD_AFFECTED_KEYS",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
        run_id="guard",
        entity_key="customer_id",
    )
    before = list(warehouse.executed)
    with pytest.raises(MutationError):
        session.warehouse.execute("delete from DATA_AGENT_DEV.DBT_CI.CUSTOMER_SUMMARY")
    assert warehouse.executed == before
