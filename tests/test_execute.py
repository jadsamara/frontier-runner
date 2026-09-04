from __future__ import annotations

import pytest

from frontier.config import ConfigError, load_frontier_config
from frontier.dbt_artifacts import DbtNode, Manifest, load_manifest
from frontier.execute import (
    HANDWRITTEN_FRONTIER_MODELS,
    IsolatedRun,
    affected_keys_relation,
    assert_not_prod,
    confirmed_changed_sql,
    create_affected_keys_sql,
    drop_relation_sql,
    generate_targeted_sql,
    generic_repaired_sql,
    isolated_table_name,
    open_isolated_run,
    profile_shows_reduction,
    restriction_is_pushed,
    run_relation_token,
    sql_change_impact_queries,
)
from frontier.frontier import load_change_events_csv, run_frontier
from frontier.proof import measure_mutation_proof
from frontier.warehouse import FakeWarehouse
from tests.conftest import FIXTURES

CUSTOMER_SUMMARY_SQL = """
with customers as (
    select c.* from stg_customers c
),
orders as (
    select o.* from stg_orders o
)
select
    c.customer_id,
    c.customer_name,
    count(o.order_id) as total_orders
from customers c
left join orders o on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name
"""


def test_relation_names_are_unique_per_run_and_isolated_in_dbt_ci() -> None:
    first = isolated_table_name("jaffle_shop-aaa111")
    second = isolated_table_name("jaffle_shop-bbb222")
    assert first != second
    assert first.startswith("FRONTIER_")
    assert first.endswith("_AFFECTED_KEYS")
    assert "-" not in first
    relation = affected_keys_relation(
        "jaffle_shop-abc1234deadbeef",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
    )
    assert relation == "DATA_AGENT_DEV.DBT_CI.FRONTIER_JAFFLE_SHOP_ABC1234DEADBEEF_AFFECTED_KEYS"
    assert run_relation_token("jaffle_shop-pr-1") != run_relation_token("jaffle_shop-pr-2")


def test_prod_schema_is_rejected(monkeypatch) -> None:
    monkeypatch.delenv("FRONTIER_WAREHOUSE_DATABASE", raising=False)
    monkeypatch.delenv("FRONTIER_WAREHOUSE_SCHEMA", raising=False)
    with pytest.raises(ConfigError, match="DBT_PROD"):
        assert_not_prod(schema="DBT_PROD")
    with pytest.raises(ConfigError, match="DBT_PROD"):
        assert_not_prod(relation="DATA_AGENT_DEV.DBT_PROD.customer_summary")
    warehouse = FakeWarehouse({"full_entity_count": [(150_000,)]})
    with pytest.raises(ConfigError, match="DBT_PROD"):
        open_isolated_run(
            warehouse,
            run_id="jaffle_shop-abc",
            entity_key="customer_id",
            model_database="DATA_AGENT_DEV",
            model_schema="DBT_PROD",
        )


def test_targeted_sql_pushes_join_into_sources() -> None:
    sql = generate_targeted_sql(
        CUSTOMER_SUMMARY_SQL,
        entity_key="customer_id",
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_RUN1_AFFECTED_KEYS",
    )
    lowered = sql.lower()
    assert "inner join" in lowered
    assert "frontier_keys" in lowered
    assert "as frontier_target" not in lowered
    assert "where customer_id in (select" not in lowered
    assert lowered.find("inner join") < lowered.find("group by")
    assert "stg_customers" in lowered
    assert "stg_orders" in lowered
    assert "from stg_customers as c\n  inner join" in lowered or "from stg_customers as c inner join" in lowered
    assert "from stg_orders as o\n  inner join" in lowered or "from stg_orders as o inner join" in lowered
    assert restriction_is_pushed(sql, entity_key="customer_id")
    with pytest.raises(ConfigError, match="unsupported SQL"):
        generate_targeted_sql(
            "insert into customers select 1",
            entity_key="customer_id",
            affected_relation="db.ci.keys",
        )
    with pytest.raises(ConfigError, match="cannot push affected-key restriction"):
        generate_targeted_sql(
            "select customer_id from a union select customer_id from b",
            entity_key="customer_id",
            affected_relation="db.ci.keys",
        )


def test_unsupported_confirmation_is_not_an_empty_set() -> None:
    warehouse = FakeWarehouse({})
    session = IsolatedRun(
        warehouse=warehouse,
        relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_RUN_AFFECTED_KEYS",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
        run_id="run",
        entity_key="customer_id",
    )
    confirmed = session.confirm(
        before_sql="insert into customers select 1",
        after_sql="insert into customers select 2",
    )
    assert confirmed is None


def test_cleanup_runs_on_success_and_failure() -> None:
    warehouse = FakeWarehouse({"full_entity_count": [(150_000,)]})
    session = IsolatedRun(
        warehouse=warehouse,
        relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_RUN_AFFECTED_KEYS",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
        run_id="run",
        entity_key="customer_id",
    )
    session.materialize(["370", "781", "36901"])
    session.cleanup()
    executed = "\n".join(warehouse.executed)
    assert "create schema if not exists data_agent_dev.dbt_ci" in executed.lower()
    assert "create or replace table data_agent_dev.dbt_ci.frontier_run_affected_keys" in executed.lower()
    assert "drop table if exists data_agent_dev.dbt_ci.frontier_run_affected_keys" in executed.lower()

    class Boom(FakeWarehouse):
        def execute(self, sql: str):
            super().execute(sql)
            if sql.lower().startswith("create or replace table"):
                raise ConfigError("create failed after recording")
            return []

    boom = Boom()
    session = IsolatedRun(
        warehouse=boom,
        relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_FAIL_AFFECTED_KEYS",
        database="DATA_AGENT_DEV",
        schema="DBT_CI",
        run_id="run-fail",
        entity_key="customer_id",
    )
    with pytest.raises(ConfigError, match="create failed"):
        session.materialize(["370"])
    session.cleanup()
    assert any(sql.lower().startswith("drop table if exists") for sql in boom.executed)


def test_jaffle_three_customers_without_handwritten_frontier_models(monkeypatch) -> None:
    monkeypatch.delenv("FRONTIER_WAREHOUSE_DATABASE", raising=False)
    monkeypatch.delenv("FRONTIER_WAREHOUSE_SCHEMA", raising=False)
    config = load_frontier_config(FIXTURES / "frontier.yml")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    nodes = {
        unique_id: node
        for unique_id, node in load_manifest(FIXTURES / "manifest.json").nodes.items()
        if node.name not in HANDWRITTEN_FRONTIER_MODELS
    }
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes=nodes,
        sources={},
    )
    warehouse = FakeWarehouse(
        {
            "full_entity_count": [(150_000,)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
        }
    )
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=warehouse,
        run_id="jaffle_shop-pr1",
        persist=True,
    )
    assert {entity.entity_value for entity in result.affected_entities} == {"370", "781", "36901"}
    assert result.frontier_entity_count == 3
    assert result.full_entity_count == 150_000
    assert result.affected_relation is not None
    assert "FRONTIER_" in result.affected_relation
    assert "AFFECTED_KEYS" in result.affected_relation
    assert "DBT_CI" in result.affected_relation
    assert not any(
        model in sql
        for sql in warehouse.executed
        for model in HANDWRITTEN_FRONTIER_MODELS
    )
    assert any(sql.lower().startswith("drop table if exists") for sql in warehouse.executed)


def test_generic_proof_does_not_need_repaired_or_target_models() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")

    def node(name: str) -> DbtNode:
        return DbtNode(
            unique_id=f"model.jaffle_shop.{name}",
            name=name,
            resource_type="model",
            database="DATA_AGENT_DEV",
            schema="DBT_DEV",
            relation_name=f"DATA_AGENT_DEV.DBT_DEV.{name}",
            depends_on=(),
        )

    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={
            node(name).unique_id: node(name)
            for name in (
                "customer_summary",
                "customer_summary_after",
                "mutation_deleted_order",
            )
        },
        sources={},
    )
    warehouse = FakeWarehouse(
        {
            "mutation_deleted_order": [(5, 781)],
            "full_rows_recomputed": [(150_000,)],
            "frontier_rows_recomputed": [(3,)],
            "missing_frontier_entities": [(0,)],
            "extra_frontier_entities": [(0,)],
            "mismatched_final_rows": [(0,)],
        }
    )
    proof = measure_mutation_proof(
        config,
        manifest=manifest,
        warehouse=warehouse,
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_RUN_AFFECTED_KEYS",
    )
    assert proof.frontier_rows_recomputed == 3
    assert proof.mismatched_final_rows == 0
    executed = "\n".join(warehouse.executed).lower()
    assert "frontier_run_affected_keys" in executed
    assert "customer_summary_repaired" not in executed
    assert "frontier_customer_summary_target_after" not in executed


def test_confirmed_changed_sql_is_two_directional() -> None:
    sql = confirmed_changed_sql(
        before_sql="select * from before_keys",
        after_sql="select * from after_keys",
        entity_key="customer_id",
    )
    assert sql.count("except") == 2
    assert "union" in sql
    repaired = generic_repaired_sql(
        before_relation="before_mart",
        targeted_after_sql="select * from after_keys",
        affected_relation="affected",
        entity_key="customer_id",
    )
    assert "not in" in repaired.lower()
    assert "union all" in repaired.lower()
    assert create_affected_keys_sql(
        "db.ci.keys",
        "customer_id",
        ["370", "781"],
    ).lower().startswith("create or replace table")
    sql = create_affected_keys_sql(
        "db.ci.keys",
        "customer_id",
        ["370"],
        sql_change_queries=["select 2 as customer_id union all select 3 as customer_id"],
    ).lower()
    assert "create or replace table" in sql
    assert "'event' as origin" in sql
    assert "'sql_change' as origin" in sql
    assert "frontier_sql_change_keys" in sql
    assert "select 2 as customer_id" in sql
    assert drop_relation_sql("db.ci.keys").lower().startswith("drop table if exists")


FILTER_CHANGE_IMPACT_SQL = (
    "select customer_id from stg_orders where status in ('F', 'O')"
)


def _jaffle_without_handwritten(monkeypatch):
    monkeypatch.delenv("FRONTIER_WAREHOUSE_DATABASE", raising=False)
    monkeypatch.delenv("FRONTIER_WAREHOUSE_SCHEMA", raising=False)
    config = load_frontier_config(FIXTURES / "frontier.yml")
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    nodes = {
        unique_id: node
        for unique_id, node in load_manifest(FIXTURES / "manifest.json").nodes.items()
        if node.name not in HANDWRITTEN_FRONTIER_MODELS
    }
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes=nodes,
        sources={},
    )
    return config, manifest, events


def test_sql_change_candidates_are_found_when_events_are_unrelated(monkeypatch) -> None:
    config, manifest, events = _jaffle_without_handwritten(monkeypatch)
    warehouse = FakeWarehouse(
        {
            "full_entity_count": [(150_000,)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
            "frontier_origin_keys": [
                ("370", "event"),
                ("781", "event"),
                ("36901", "event"),
                ("2", "sql_change"),
                ("3", "sql_change"),
                ("4", "sql_change"),
            ],
            "frontier_origin_counts": [(3, 3, 6)],
        }
    )
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=warehouse,
        run_id="jaffle_shop-filter-pr",
        persist=True,
        sql_change_queries=[FILTER_CHANGE_IMPACT_SQL],
        sql_change_required=True,
    )
    values = {entity.entity_value for entity in result.affected_entities}
    assert {"370", "781", "36901"} <= values
    assert {"2", "3", "4"} <= values
    assert result.event_candidate_count == 3
    assert result.sql_change_candidate_count == 3
    assert result.union_candidate_count == 6
    assert result.full_rebuild_required is False
    create = next(
        sql for sql in warehouse.executed if sql.lower().lstrip().startswith("create or replace table")
    )
    assert "frontier_sql_change_keys" in create.lower()
    assert "status in ('F', 'O')" in create
    assert "'sql_change' as origin" in create.lower()
    reasons = {entity.reason for entity in result.affected_entities}
    assert "SQL change candidate" in reasons


def test_sql_change_without_impact_query_requires_full_rebuild(monkeypatch) -> None:
    config, manifest, events = _jaffle_without_handwritten(monkeypatch)
    warehouse = FakeWarehouse(
        {
            "full_entity_count": [(150_000, 3)],
            "order_id in (1)": [(36901,)],
            "order_id in (-1)": [],
        }
    )
    result = run_frontier(
        config,
        manifest=manifest,
        events=events,
        warehouse=warehouse,
        run_id="jaffle_shop-rebuild",
        persist=True,
        sql_change_required=True,
        sql_change_queries=(),
        before_sql="select customer_id from customer_summary where status = 'F'",
        after_sql="select customer_id from customer_summary where status in ('F', 'O')",
    )
    assert result.full_rebuild_required is True
    assert result.frontier_entity_count == result.full_entity_count
    assert result.percent_rows_avoided == 0
    assert any("unavailable" in reason for reason in result.execution_reasons)
    create_sql = [sql for sql in warehouse.executed if sql.lower().lstrip().startswith("create or replace table")]
    assert create_sql == []


def test_sql_change_impact_queries_fail_closed_without_candidate_sql() -> None:
    queries, required = sql_change_impact_queries(
        {
            "modified": [
                {
                    "name": "stg_orders",
                    "changeKinds": ["FILTER_CHANGED"],
                    "impactStatus": "COMPILED",
                    "candidateSql": FILTER_CHANGE_IMPACT_SQL,
                }
            ]
        }
    )
    assert required is True
    assert queries == (FILTER_CHANGE_IMPACT_SQL,)
    missing, missing_required = sql_change_impact_queries(
        {
            "modified": [
                {
                    "name": "stg_orders",
                    "changeKinds": ["FILTER_CHANGED"],
                    "impactStatus": "COMPILED",
                }
            ]
        }
    )
    assert missing_required is True
    assert missing == ()
    rebuild, rebuild_required = sql_change_impact_queries(
        {
            "modified": [
                {
                    "name": "stg_orders",
                    "impactStatus": "FULL_REBUILD_REQUIRED",
                    "candidateSql": FILTER_CHANGE_IMPACT_SQL,
                }
            ]
        }
    )
    assert rebuild_required is True
    assert rebuild == ()
    none_queries, none_required = sql_change_impact_queries(None)
    assert none_required is False
    assert none_queries == ()


def test_query_profile_shows_reduction_for_pushed_restriction() -> None:
    warehouse = FakeWarehouse({"stg_customers": [(1,)]})
    targeted = generate_targeted_sql(
        CUSTOMER_SUMMARY_SQL,
        entity_key="customer_id",
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_RUN1_AFFECTED_KEYS",
    )
    warehouse.execute(CUSTOMER_SUMMARY_SQL)
    full_id = warehouse.last_query_id
    warehouse.execute(targeted)
    targeted_id = warehouse.last_query_id
    assert full_id and targeted_id and full_id != targeted_id
    assert profile_shows_reduction(
        warehouse.get_query_profile(full_id),
        warehouse.get_query_profile(targeted_id),
    )
