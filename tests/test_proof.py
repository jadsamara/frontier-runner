from __future__ import annotations

from pathlib import Path

from frontier.config import load_frontier_config
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.frontier import ChangeEvent, load_change_events_csv
from frontier.proof import (
    apply_resolved_delete,
    extra_frontier_sql,
    mismatched_rows_sql,
    missing_frontier_sql,
    measure_mutation_proof,
    proof_validation_results,
    recorded_proof,
)
from frontier.warehouse import FakeWarehouse
from frontier.validation import evidence_level
from tests.conftest import FIXTURES, JAFFLE_SHOP


def _node(name: str) -> DbtNode:
    return DbtNode(
        unique_id=f"model.jaffle_shop.{name}",
        name=name,
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name=f"DATA_AGENT_DEV.DBT_DEV.{name}",
        depends_on=(),
    )


def _proof_manifest() -> Manifest:
    names = [
        "customer_summary",
        "customer_summary_after",
        "customer_summary_repaired",
        "frontier_affected_customers",
        "frontier_customer_summary_target_after",
        "mutation_deleted_order",
    ]
    nodes = {node.unique_id: node for node in (_node(name) for name in names)}
    return Manifest(project_name="jaffle_shop", adapter_type="snowflake", nodes=nodes, sources={})


def test_except_sql_is_two_directional() -> None:
    sql = mismatched_rows_sql(
        after_relation="after_mart",
        repaired_relation="repaired_mart",
    )
    assert sql.count("except") == 2
    assert "union all" in sql
    assert "mismatched_final_rows" in sql


def test_coverage_sql_compares_changed_customers_to_frontier() -> None:
    missing = missing_frontier_sql(
        before_relation="before_mart",
        after_relation="after_mart",
        frontier_relation="frontier_customers",
    )
    extra = extra_frontier_sql(
        before_relation="before_mart",
        after_relation="after_mart",
        frontier_relation="frontier_customers",
    )
    assert "missing_frontier_entities" in missing
    assert "extra_frontier_entities" in extra
    assert "except" in missing
    assert "except" in extra


def test_measure_mutation_proof_with_fake_warehouse() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
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
    proof = measure_mutation_proof(config, manifest=_proof_manifest(), warehouse=warehouse)
    assert proof.full_rows_recomputed == 150_000
    assert proof.frontier_rows_recomputed == 3
    assert proof.rows_avoided == 149_997
    assert proof.missing_frontier_entities == 0
    assert proof.extra_frontier_entities == 0
    assert proof.mismatched_final_rows == 0
    assert proof.deleted_order_id == "5"
    assert proof.deleted_order_customer_id == "781"
    validations = proof_validation_results(proof)
    assert all(item.status == "passed" for item in validations)
    assert evidence_level(validations) == "empirically_validated"


def test_apply_resolved_delete_uses_actual_order() -> None:
    events = load_change_events_csv(FIXTURES / "change_events.csv")
    resolved = apply_resolved_delete(events, order_id="5", customer_id="781")
    delete = next(event for event in resolved if event.event_id == "event_003")
    assert delete.entity_value == "5"
    assert delete.prior_entity_value == "781"
    assert delete.operation == "DELETE"
    assert isinstance(resolved[0], ChangeEvent)


def test_recorded_proof_is_the_verified_shape() -> None:
    proof = recorded_proof()
    assert proof.full_rows_recomputed == 150_000
    assert proof.frontier_rows_recomputed == 3
    assert proof.mismatched_final_rows == 0
    assert proof.percent_rows_avoided == 99.998


def test_jaffle_shop_overlays_do_not_write_sample_data() -> None:
    mutations = JAFFLE_SHOP / "models" / "mutations"
    if not mutations.is_dir():
        return
    for path in mutations.glob("*.sql"):
        text = path.read_text()
        assert "insert into" not in text.lower()
        assert "delete from" not in text.lower()
        assert "source(" not in text
        assert "{{ source" not in text
    orders = (mutations / "stg_orders_mutated.sql").read_text()
    customers = (mutations / "stg_customers_mutated.sql").read_text()
    repaired = (mutations / "customer_summary_repaired.sql").read_text()
    except_test = (JAFFLE_SHOP / "tests" / "assert_repaired_equals_reference.sql").read_text()
    assert "* 1.10" in orders or "1.10" in orders
    assert "number(12, 2)" in orders.lower() or "number(12,2)" in orders.lower()
    assert "mutation_deleted_order" in orders
    assert "370" in customers
    assert "MUTATED" in customers
    assert "frontier_affected_customers" in repaired
    assert "frontier_customer_summary_target_after" in repaired
    assert except_test.count("except") == 2
    assert (Path("/Users/jad/Desktop/data_agent_pipeline/jaffle_shop/models/staging/stg_orders.sql").read_text().count("source('tpch'")) == 1
