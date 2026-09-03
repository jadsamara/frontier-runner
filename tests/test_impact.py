from __future__ import annotations

from frontier.impact import (
    CANDIDATE_SET_ANALYSIS_FAILED,
    CANDIDATE_SET_EMPTY,
    CANDIDATE_SET_NOT_EVALUATED,
    COMPILED,
    FULL_REBUILD_REQUIRED,
    compile_impact_query,
    evaluate_impact_query,
)
from frontier.warehouse import FakeWarehouse


FILTER_BASE = "select status, customer_id from orders where status = 'F'"
FILTER_PR = "select status, customer_id from orders where status in ('F', 'O')"


def test_filter_change_compiles_is_distinct_from() -> None:
    result = compile_impact_query(
        FILTER_BASE,
        FILTER_PR,
        entity_key="customer_id",
    )
    assert result.status == COMPILED
    assert result.candidate_sql is not None
    sql = result.candidate_sql.lower()
    assert "select distinct" in sql
    assert "customer_id" in sql
    assert "is distinct from" in sql
    assert result.candidate_set_state == CANDIDATE_SET_NOT_EVALUATED
    assert result.candidates is None
    assert result.parameters
    assert result.parameterized_sql is not None
    assert ":p0" in result.parameterized_sql
    again = compile_impact_query(FILTER_BASE, FILTER_PR, entity_key="customer_id")
    assert again.candidate_sql == result.candidate_sql
    assert again.query_fingerprint == result.query_fingerprint


def test_identical_sql_is_empty_not_failure() -> None:
    result = compile_impact_query(FILTER_BASE, FILTER_BASE, entity_key="customer_id")
    assert result.status == COMPILED
    assert result.candidate_sql is None
    assert result.candidate_set_state == CANDIDATE_SET_EMPTY
    assert result.candidates == ()


def test_case_and_simple_projection_and_aggregate_contribution() -> None:
    case = compile_impact_query(
        "select case when status = 'F' then 1 else 0 end, customer_id from orders",
        "select case when status = 'O' then 1 else 0 end, customer_id from orders",
        entity_key="customer_id",
    )
    assert case.status == COMPILED
    assert "is distinct from" in (case.candidate_sql or "").lower()
    assert "case" in (case.candidate_sql or "").lower()

    projection = compile_impact_query(
        "select amount, customer_id from orders",
        "select amount * 1.1, customer_id from orders",
        entity_key="customer_id",
    )
    assert projection.status == COMPILED
    assert "is distinct from" in (projection.candidate_sql or "").lower()

    aggregate = compile_impact_query(
        "select customer_id, sum(amount) from orders group by customer_id",
        "select customer_id, sum(amount * 2) from orders group by customer_id",
        entity_key="customer_id",
    )
    assert aggregate.status == COMPILED
    sql = (aggregate.candidate_sql or "").lower()
    assert "is distinct from" in sql
    assert "amount" in sql


def test_inner_left_equijoin_with_confirmed_keys() -> None:
    result = compile_impact_query(
        "select o.customer_id from orders o join customers c on o.customer_id = c.id where o.status = 'F'",
        "select o.customer_id from orders o join customers c on o.customer_id = c.id where o.status in ('F', 'O')",
        entity_key="customer_id",
        confirmed_keys=("customer_id",),
    )
    assert result.status == COMPILED
    sql = (result.candidate_sql or "").lower()
    assert "join" in sql
    assert "is distinct from" in sql


def test_unsupported_changes_require_full_rebuild() -> None:
    grain = compile_impact_query(
        "select customer_id from orders",
        "select distinct customer_id from orders limit 10",
        entity_key="customer_id",
    )
    assert grain.status == FULL_REBUILD_REQUIRED
    assert grain.candidate_sql is None
    assert grain.candidates is None
    assert grain.candidate_set_state == CANDIDATE_SET_ANALYSIS_FAILED
    assert "grain change" in grain.reasons or "GRAIN_POSSIBLY_CHANGED" in grain.reasons

    udf = compile_impact_query(
        "select status, customer_id from orders",
        "select my_udf(status), customer_id from orders",
        entity_key="customer_id",
    )
    assert udf.status == FULL_REBUILD_REQUIRED
    assert udf.candidates is None
    assert any("UDF" in reason or "UNSUPPORTED" in reason for reason in udf.reasons)

    nondet = compile_impact_query(
        "select customer_id from orders where status = 'F'",
        "select customer_id from orders where status = 'F' and random() > 0",
        entity_key="customer_id",
    )
    assert nondet.status == FULL_REBUILD_REQUIRED
    assert any("nondeterministic" in reason for reason in nondet.reasons)

    window = compile_impact_query(
        "select customer_id, row_number() over (order by ts) as rn from events",
        "select customer_id, row_number() over (order by ts desc) as rn from events",
        entity_key="customer_id",
    )
    assert window.status == FULL_REBUILD_REQUIRED

    correlated = compile_impact_query(
        "select customer_id from orders",
        "select customer_id from orders o where exists (select 1 from events e where e.order_id = o.id)",
        entity_key="customer_id",
    )
    assert correlated.status == FULL_REBUILD_REQUIRED
    assert any("correlated" in reason for reason in correlated.reasons)

    many = compile_impact_query(
        "select o.customer_id from orders o join items i on o.status = i.status",
        "select o.customer_id from orders o join items i on o.status = i.kind",
        entity_key="customer_id",
        confirmed_keys=("customer_id",),
    )
    assert many.status == FULL_REBUILD_REQUIRED
    assert any("many-to-many" in reason for reason in many.reasons)

    dynamic = compile_impact_query(
        "select customer_id from orders",
        "select customer_id from identifier(table_name)",
        entity_key="customer_id",
    )
    assert dynamic.status == FULL_REBUILD_REQUIRED
    assert any("dynamic" in reason for reason in dynamic.reasons)


def test_empty_evaluation_is_not_analysis_failure() -> None:
    compiled = compile_impact_query(FILTER_BASE, FILTER_PR, entity_key="customer_id")
    warehouse = FakeWarehouse({"is distinct from": []})
    evaluated = evaluate_impact_query(compiled, warehouse)
    assert evaluated.status == COMPILED
    assert evaluated.candidate_set_state == CANDIDATE_SET_EMPTY
    assert evaluated.keys == ()
    assert evaluated.candidates == ()

    failed = compile_impact_query(
        FILTER_BASE,
        "select my_udf(status) from orders",
        entity_key="customer_id",
    )
    failed_eval = evaluate_impact_query(failed, warehouse)
    assert failed_eval.status == FULL_REBUILD_REQUIRED
    assert failed_eval.keys is None
    assert failed_eval.candidates is None
    assert failed_eval.candidate_set_state == CANDIDATE_SET_ANALYSIS_FAILED


CUSTOMER_SUMMARY_BASE = """
with customers as (
    select c.* from stg_customers c
),
orders as (
    select o.*
    from stg_orders o
    where o.order_status = 'F'
)
select
    c.customer_id,
    c.customer_name,
    c.customer_nation_key,
    c.customer_market_segment,
    count(o.order_id) as total_orders,
    coalesce(sum(o.total_price), 0) as total_spend,
    coalesce(avg(o.total_price), 0) as average_order_value,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date
from customers c
left join orders o
    on c.customer_id = o.customer_id
group by
    c.customer_id,
    c.customer_name,
    c.customer_nation_key,
    c.customer_market_segment
"""

CUSTOMER_SUMMARY_PR = CUSTOMER_SUMMARY_BASE.replace(
    "where o.order_status = 'F'",
    "where o.order_status in ('F', 'O')",
)


def test_cte_filter_change_compiles_from_inner_source() -> None:
    result = compile_impact_query(
        CUSTOMER_SUMMARY_BASE,
        CUSTOMER_SUMMARY_PR,
        entity_key="customer_id",
        confirmed_keys=("customer_id",),
    )
    assert result.status == COMPILED
    assert result.candidate_sql is not None
    sql = result.candidate_sql.lower()
    assert "select distinct" in sql
    assert "customer_id" in sql
    assert "is distinct from" in sql
    assert "order_status" in sql
    assert "stg_orders" in sql
    assert "group by" not in sql
    assert result.candidate_set_state == CANDIDATE_SET_NOT_EVALUATED
    assert result.candidates is None
    assert "CTE" not in result.reasons


def test_cte_filter_over_cte_source_keeps_dependency() -> None:
    base = """
        with src as (
            select customer_id, order_status from stg_orders
        ),
        orders as (
            select * from src where order_status = 'F'
        )
        select customer_id from orders
    """
    pr = """
        with src as (
            select customer_id, order_status from stg_orders
        ),
        orders as (
            select * from src where order_status in ('F', 'O')
        )
        select customer_id from orders
    """
    result = compile_impact_query(base, pr, entity_key="customer_id")
    assert result.status == COMPILED
    sql = (result.candidate_sql or "").lower()
    assert "is distinct from" in sql
    assert "with src as" in sql
    assert "from src" in sql


def test_recursive_cte_and_nested_subquery_require_full_rebuild() -> None:
    recursive = compile_impact_query(
        "with recursive t as (select 1 as customer_id) select customer_id from t",
        "with recursive t as (select 1 as customer_id) select customer_id from t where customer_id = 1",
        entity_key="customer_id",
    )
    assert recursive.status == FULL_REBUILD_REQUIRED
    assert recursive.candidates is None
    assert any("RECURSIVE_CTE" in reason for reason in recursive.reasons)

    nested = compile_impact_query(
        "select customer_id from (select customer_id from orders where status = 'F') t",
        "select customer_id from (select customer_id from orders where status in ('F', 'O')) t",
        entity_key="customer_id",
    )
    assert nested.status == FULL_REBUILD_REQUIRED
    assert nested.candidates is None
    assert nested.candidate_set_state == CANDIDATE_SET_ANALYSIS_FAILED

