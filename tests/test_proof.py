from __future__ import annotations

from pathlib import Path

from frontier.config import load_frontier_config
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.frontier import ChangeEvent, load_change_events_csv
from frontier.proof import (
    MutationProof,
    apply_resolved_delete,
    extra_frontier_sql,
    mismatched_rows_sql,
    missing_frontier_sql,
    measure_mutation_proof,
    measure_sql_change_proof,
    proof_validation_results,
    recorded_proof,
    recorded_sql_change_affected,
    recorded_sql_change_proof,
    sql_change_proof_validation_results,
    targeted_mismatch_sql,
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
    assert missing.count("except") >= 3
    assert extra.count("except") >= 3
    assert "union" in missing
    assert "union" in extra
    assert "select * from after_mart" in missing
    assert "select * from before_mart" in missing


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


def test_extra_frontier_candidates_do_not_fail_proof() -> None:
    proof = recorded_proof()
    proof_with_noop = MutationProof(
        full_rows_recomputed=proof.full_rows_recomputed,
        frontier_rows_recomputed=proof.frontier_rows_recomputed,
        rows_avoided=proof.rows_avoided,
        missing_frontier_entities=0,
        extra_frontier_entities=1,
        mismatched_final_rows=0,
        test_duration_ms=proof.test_duration_ms,
        deleted_order_id=proof.deleted_order_id,
        deleted_order_customer_id=proof.deleted_order_customer_id,
    )
    validations = proof_validation_results(proof_with_noop)
    by_name = {item.test_name: item for item in validations}
    assert by_name["assert_changed_customers_in_frontier"].status == "passed"
    assert by_name["assert_no_extra_frontier_entities"].status == "passed"
    assert by_name["assert_no_extra_frontier_entities"].difference_count == 1
    assert "no-ops" in (by_name["assert_no_extra_frontier_entities"].message or "")
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


def test_recorded_sql_change_proof_covers_reference() -> None:
    proof = recorded_sql_change_proof()
    assert proof.full_rows_recomputed == 150_000
    assert proof.candidate_frontier_count == 12
    assert proof.confirmed_frontier_count == 8
    assert proof.source_population_count == 12
    assert proof.changed_source_row_count == 12
    assert proof.extra_frontier_entities == 4
    assert proof.missing_frontier_entities == 0
    assert proof.mismatched_final_rows == 0
    assert proof.targeted_repair_safe is True
    assert proof.full_rebuild_required is False
    values = {entity.entity_value for entity in recorded_sql_change_affected()}
    assert values == {"4", "7", "9", "22", "31", "44", "73", "88"}
    validations = sql_change_proof_validation_results(proof)
    assert {item.test_name for item in validations} == {
        "assert_sql_frontier_covers_reference",
        "assert_repaired_equals_reference",
    }
    assert all(item.status == "passed" for item in validations)
    assert evidence_level(validations) == "empirically_validated"


def test_measure_sql_change_proof_with_fake_warehouse() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    warehouse = FakeWarehouse(
        {
            "before_entity_count": [(150_000,)],
            "after_entity_count": [(150_000,)],
            "changed_source_row_count": [(12,)],
            "mismatched_final_rows": [(0,)],
            "frontier_rows_recomputed": [(12,)],
            "missing_frontier": [(0,)],
            "extra_frontier": [(4,)],
            "confirmed_frontier_count": [(8,)],
        }
    )
    proof = measure_sql_change_proof(
        config,
        warehouse=warehouse,
        before_sql="select customer_id, 1 as total_orders from orders where order_status = 'F'",
        after_sql="select customer_id, 1 as total_orders from orders where order_status in ('F', 'O')",
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_AFFECTED_KEYS",
        impact_sql="select customer_id from orders where order_status in ('O')",
    )
    assert proof.candidate_frontier_count == 12
    assert proof.confirmed_frontier_count == 8
    assert proof.source_population_count == 12
    assert proof.changed_source_row_count == 12
    assert proof.before_entity_count == 150_000
    assert proof.after_entity_count == 150_000
    assert proof.missing_frontier_entities == 0
    assert proof.extra_frontier_entities == 4
    assert proof.mismatched_final_rows == 0
    assert proof.targeted_repair_safe is True


def test_measure_sql_change_proof_uses_targeted_tables_not_full_recompute() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    before_sql = (
        "select customer_id, 1 as total_orders from DATA_AGENT_DEV.DBT_CI.stg_orders "
        "where order_status = 'F'"
    )
    after_sql = (
        "select customer_id, 1 as total_orders from DATA_AGENT_DEV.DBT_CI.stg_orders "
        "where order_status in ('F', 'O')"
    )
    warehouse = FakeWarehouse(
        {
            "frontier_rows_recomputed": [(99_621,)],
            "mismatched_final_rows": [(0,)],
        }
    )
    proof = measure_sql_change_proof(
        config,
        warehouse=warehouse,
        before_sql=before_sql,
        after_sql=after_sql,
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_AFFECTED_KEYS",
        impact_sql="select distinct customer_id from DATA_AGENT_DEV.DBT_CI.stg_orders where order_status = 'O'",
        candidate_count=99_621,
        confirmed_count=99_621,
        targeted_before_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_TARGET_BASE",
        targeted_after_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_TARGET_HEAD",
        reference_relation="DATA_AGENT_DEV.DBT_CI.int_customer_orders",
        full_entity_count=150_000,
        changed_source_row_count=732_044,
    )
    executed = "\n".join(warehouse.executed)
    assert before_sql not in executed
    assert after_sql not in executed
    assert "stg_orders" not in executed.lower()
    assert "int_customer_orders" in executed.lower()
    assert "frontier_test_target_head" in executed.lower()
    assert "frontier_test_affected_keys" in executed.lower()
    assert proof.changed_source_row_count == 732_044
    assert proof.candidate_frontier_count == 99_621
    assert proof.confirmed_frontier_count == 99_621
    assert proof.frontier_rows_recomputed == 99_621
    assert proof.full_rows_recomputed == 150_000
    assert proof.extra_frontier_entities == 0
    assert proof.missing_frontier_entities == 0
    assert proof.mismatched_final_rows == 0
    assert proof.percent_rows_avoided == 33.586


def test_targeted_mismatch_sql_joins_pr_relation() -> None:
    sql = targeted_mismatch_sql(
        reference_relation="DATA_AGENT_DEV.DBT_CI.int_customer_orders",
        targeted_after_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_TARGET_HEAD",
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_AFFECTED_KEYS",
        entity_key="customer_id",
    )
    lowered = sql.lower()
    assert "int_customer_orders" in lowered
    assert "frontier_test_target_head" in lowered
    assert "inner join" in lowered
    assert "stg_orders" not in lowered
    assert sql.count("except") == 2


def test_measure_sql_change_proof_separates_source_rows_from_candidates() -> None:
    config = load_frontier_config(FIXTURES / "frontier.yml")
    warehouse = FakeWarehouse(
        {
            "before_entity_count": [(150_000,)],
            "after_entity_count": [(150_000,)],
            "changed_source_row_count": [(732_044,)],
            "mismatched_final_rows": [(0,)],
            "frontier_rows_recomputed": [(99_621,)],
            "missing_frontier": [(0,)],
            "extra_frontier": [(0,)],
            "confirmed_frontier_count": [(99_621,)],
        }
    )
    proof = measure_sql_change_proof(
        config,
        warehouse=warehouse,
        before_sql="select customer_id, 1 as total_orders from orders where order_status = 'F'",
        after_sql="select customer_id, 1 as total_orders from orders where order_status in ('F', 'O')",
        affected_relation="DATA_AGENT_DEV.DBT_CI.FRONTIER_TEST_AFFECTED_KEYS",
        impact_sql="select distinct customer_id from DATA_AGENT_DEV.DBT_CI.STG_ORDERS where order_status in ('O')",
        candidate_count=99_621,
        confirmed_count=99_621,
        changed_source_row_count=732_044,
    )
    assert proof.changed_source_row_count == 732_044
    assert proof.source_population_count == 732_044
    assert proof.candidate_frontier_count == 99_621
    assert proof.confirmed_frontier_count == 99_621
    assert proof.full_rows_recomputed == 150_000


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
    coverage = (JAFFLE_SHOP / "tests" / "assert_changed_customers_in_frontier.sql").read_text()
    extra = (JAFFLE_SHOP / "tests" / "assert_no_extra_frontier_entities.sql").read_text()
    assert "customer_summary_after" in coverage
    assert "severity='warn'" in extra.replace(" ", "") or "severity='warn'" in extra
    assert "severity='error'" not in extra
    assert (Path("/Users/jad/Desktop/data_agent_pipeline/jaffle_shop/models/staging/stg_orders.sql").read_text().count("source('tpch'")) == 1
