from __future__ import annotations

from frontier.snowflake_sql import (
    AGGREGATE_CHANGED,
    FILTER_CHANGED,
    GRAIN_POSSIBLY_CHANGED,
    GROUPING_CHANGED,
    JOIN_CHANGED,
    UNSUPPORTED,
    WINDOW_CHANGED,
    classify_sql_change,
    parse_snowflake_sql,
)


def test_alias_and_formatting_normalize_away() -> None:
    base = """
        select o.status as order_status, o.customer_id
        from orders as o
        where o.status = 'F'
    """
    pr = """
        SELECT
            status,
            customer_id
        FROM orders
        WHERE status = 'F'
    """
    change = classify_sql_change(base, pr)
    assert change.kinds == ()
    assert change.unsafe is False
    assert parse_snowflake_sql(base).ok
    assert parse_snowflake_sql(pr).ok


def test_filter_equality_to_in_list_is_filter_changed() -> None:
    base = "select status from orders where status = 'F'"
    pr = "select status from orders where status in ('F', 'O')"
    change = classify_sql_change(base, pr)
    assert change.kinds == (FILTER_CHANGED,)
    assert change.unsafe is False


def test_grouping_key_change_is_unsafe() -> None:
    base = "select customer_id, count(*) from orders group by customer_id"
    pr = "select customer_id, status, count(*) from orders group by customer_id, status"
    change = classify_sql_change(base, pr)
    assert GROUPING_CHANGED in change.kinds
    assert change.unsafe is True


def test_unknown_udf_is_unsupported() -> None:
    parsed = parse_snowflake_sql("select my_udf(status) from orders")
    assert parsed.ok is False
    assert any("UDF" in reason for reason in parsed.unsupported)
    change = classify_sql_change(
        "select status from orders",
        "select my_udf(status) from orders",
    )
    assert change.kinds == (UNSUPPORTED,)
    assert change.unsafe is True
    assert change.affected_entities == []
    assert parsed.affected_entities == []


def test_parse_error_never_looks_like_empty_affected_set() -> None:
    parsed = parse_snowflake_sql("not valid snowflake sql [[[")
    assert parsed.ok is False
    assert parsed.ir is None
    assert parsed.unsupported
    assert parsed.affected_entities == []
    change = classify_sql_change("select 1", "not valid snowflake sql [[[")
    assert change.kinds == (UNSUPPORTED,)
    assert change.unsafe is True
    assert "affected" not in change.to_payload()


def test_join_and_window_and_aggregate_and_grain() -> None:
    join = classify_sql_change(
        "select o.id from orders o join customers c on o.customer_id = c.id",
        "select o.id from orders o left join customers c on o.customer_id = c.id",
    )
    assert JOIN_CHANGED in join.kinds

    window = classify_sql_change(
        "select customer_id, row_number() over (partition by customer_id order by ts) as rn from events",
        "select customer_id, row_number() over (partition by customer_id order by ts desc) as rn from events",
    )
    assert WINDOW_CHANGED in window.kinds

    agg = classify_sql_change(
        "select customer_id, count(*) from orders group by customer_id",
        "select customer_id, sum(amount) from orders group by customer_id",
    )
    assert AGGREGATE_CHANGED in agg.kinds
    assert agg.unsafe is False

    grain = classify_sql_change(
        "select customer_id from orders",
        "select distinct customer_id from orders limit 10",
    )
    assert GRAIN_POSSIBLY_CHANGED in grain.kinds
    assert grain.unsafe is True


def test_case_expression_is_expression_changed() -> None:
    change = classify_sql_change(
        "select case when status = 'F' then 1 else 0 end from orders",
        "select case when status = 'O' then 1 else 0 end from orders",
    )
    assert change.kinds == ("EXPRESSION_CHANGED",)
    assert change.unsafe is False
