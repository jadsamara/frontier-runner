from __future__ import annotations

from frontier.sql_fingerprint import normalize_sql, render_executable_sql, sql_fingerprint

import sqlglot

BASE_SQL = """
select
  customer_id,
  count(*) as order_count
from orders
where status = 'complete'
group by customer_id
"""

WHITESPACE_SQL = """
SELECT
    customer_id,
    count(*)    AS   order_count
FROM   orders
WHERE  status = 'complete'
GROUP BY customer_id
"""

COMMENT_SQL = """
select
  customer_id,
  count(*) as order_count -- trailing
from orders
/* block
   comment */
where status = 'complete'
group by customer_id
"""

FILTER_SQL = """
select
  customer_id,
  count(*) as order_count
from orders
where status = 'returned'
group by customer_id
"""


def test_whitespace_only_change_is_not_semantic() -> None:
    assert sql_fingerprint(BASE_SQL, dialect="snowflake") == sql_fingerprint(
        WHITESPACE_SQL,
        dialect="snowflake",
    )
    assert normalize_sql(BASE_SQL, dialect="snowflake") == normalize_sql(
        WHITESPACE_SQL,
        dialect="snowflake",
    )


def test_comment_only_change_is_not_semantic() -> None:
    assert sql_fingerprint(BASE_SQL, dialect="snowflake") == sql_fingerprint(
        COMMENT_SQL,
        dialect="snowflake",
    )


def test_filter_change_is_semantic() -> None:
    assert sql_fingerprint(BASE_SQL, dialect="snowflake") != sql_fingerprint(
        FILTER_SQL,
        dialect="snowflake",
    )


def test_snowflake_executable_sql_does_not_quote_folded_identifiers() -> None:
    tree = sqlglot.parse_one(
        "select customer_id from snowflake_sample_data.tpch_sf1.orders",
        dialect="snowflake",
    )
    sql = render_executable_sql(tree, dialect="snowflake")
    assert "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS" in sql
    assert '"snowflake_sample_data"' not in sql
