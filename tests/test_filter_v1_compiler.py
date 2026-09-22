"""Differential acceptance: true_changed ⊆ candidates. False negatives fail."""

from __future__ import annotations

import sqlite3

import sqlglot

from frontier.certification import (
    ECONOMICS_NOT_EVALUATED,
    EXECUTION_FAILED,
    FULL_REBUILD_RECOMMENDED,
    SQL_CERTIFIED,
    TARGETED_REPAIR_RECOMMENDED,
    UNCERTIFIED,
    VALIDATION_FAILED,
    build_assessment_dimensions,
    economics_decision,
    enrich_certification_record,
    filter_v1_sql_certified,
)
from frontier.filter_v1 import (
    KEY_LINEAGE_BROKEN,
    TWO_TAINTED_JOIN_INPUTS,
    UNSUPPORTED_OPERATION,
    PlanError,
    analyze_static_eligibility,
    assert_candidate_sql_scopes,
)
from frontier.impact import CANDIDATE_SET_ANALYSIS_FAILED, CANDIDATE_SET_EMPTY
from frontier.snapshot import (
    ASSURANCE_ADAPTER,
    MODE_TIME_TRAVEL,
    PERMANENT_TABLE,
    SOURCE_SNAPSHOT_NOT_PINNED,
    RelationBinding,
    SourceSnapshot,
    adapter_evidence_token,
)

MANIFEST_FP = "b" * 64
ENTITY = "customer_id"

CUSTOMERS = """
CREATE TABLE stg_customers (
  customer_id INTEGER,
  customer_name TEXT,
  active TEXT
);
CREATE TABLE stg_orders (
  id INTEGER,
  customer_id INTEGER,
  status TEXT,
  total_price REAL,
  order_date TEXT
);
CREATE TABLE stg_nations (
  nation_id INTEGER,
  customer_id INTEGER,
  nation_name TEXT
);
"""


def _transpile(sql: str) -> str:
    return sqlglot.transpile(sql, read="snowflake", write="sqlite")[0]


def _rows(conn: sqlite3.Connection, sql: str) -> list[tuple]:
    return list(conn.execute(_transpile(sql)))


def _true_changed(conn: sqlite3.Connection, base: str, pr: str) -> set[str]:
    old_map = {str(row[0]): row[1:] for row in _rows(conn, base) if row[0] is not None}
    new_map = {str(row[0]): row[1:] for row in _rows(conn, pr) if row[0] is not None}
    changed: set[str] = set()
    for key in set(old_map) | set(new_map):
        if old_map.get(key) != new_map.get(key):
            changed.add(key)
    return changed


def _analyze(base: str, pr: str, catalog: dict[str, tuple[str, ...]] | None = None):
    return analyze_static_eligibility(
        base,
        pr,
        entity_key=ENTITY,
        dialect="snowflake",
        manifest_version=1,
        manifest_fingerprint=MANIFEST_FP,
        schema_catalog=catalog,
    )


def assert_sound(
    conn: sqlite3.Connection,
    base: str,
    pr: str,
    *,
    catalog: dict[str, tuple[str, ...]] | None = None,
) -> tuple[set[str], set[str]]:
    result = _analyze(base, pr, catalog)
    assert result.eligible is True, result.diagnostic
    assert result.compiled is True, result.diagnostic
    assert result.candidate_sql
    true_changed = _true_changed(conn, base, pr)
    candidates = {
        str(row[0])
        for row in _rows(conn, result.candidate_sql)
        if row and row[0] is not None
    }
    missed = true_changed - candidates
    assert missed == set(), f"false negatives {missed}; true={true_changed} candidates={candidates}"
    return true_changed, candidates


def _seed_flagship(conn: sqlite3.Connection) -> None:
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'Y'), (3, 'C', 'Y'), (4, 'D', 'N');
        INSERT INTO stg_orders VALUES
          (10, 1, 'F', 10, '2020-01-01'),
          (11, 1, 'P', 11, '2020-01-02'),
          (20, 2, 'F', 20, '2020-01-03'),
          (30, 3, 'O', 30, '2020-01-04'),
          (40, 4, 'P', 40, '2020-01-05');
        """
    )


FLAGSHIP_BASE = """
with orders as (
  select id, customer_id, status, total_price, order_date
  from stg_orders
  where status = 'F'
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""
FLAGSHIP_PR = FLAGSHIP_BASE.replace("where status = 'F'", "where status in ('F', 'O')")


def test_1_flagship_filter_inside_orders_cte() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_flagship(conn)
    true_changed, candidates = assert_sound(conn, FLAGSHIP_BASE, FLAGSHIP_PR)
    assert true_changed == {"3"}
    assert "3" in candidates


def test_1b_aliased_physical_columns_compile_to_source_names() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE tpch_customers (c_custkey INTEGER, c_name TEXT);
        CREATE TABLE tpch_orders (
          o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT
        );
        INSERT INTO tpch_customers VALUES (1, 'A'), (2, 'B'), (3, 'C');
        INSERT INTO tpch_orders VALUES
          (10, 1, 'P'),
          (20, 2, 'F'),
          (30, 3, 'O');
        """
    )
    base = """
    with orders as (
      select o_orderkey as id, o_custkey as customer_id, o_orderstatus as order_status
      from tpch_orders
      where o_orderstatus = 'F'
    )
    select c.customer_id, count(o.id) as order_count
    from (
      select c_custkey as customer_id from tpch_customers
    ) as c
    left join orders as o
      on c.customer_id = o.customer_id
    group by c.customer_id
    """
    pr = base.replace("where o_orderstatus = 'F'", "where o_orderstatus in ('F', 'O')")
    result = _analyze(base, pr)
    assert result.eligible is True
    assert result.compiled is True
    sql = result.candidate_sql or ""
    assert "O_ORDERSTATUS" in sql.upper()
    assert "C_CUSTKEY" in sql.upper()
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "3" in true_changed
    assert "3" in candidates
    assert not (true_changed - candidates)


def test_2_right_filter_removes_last_matching_order() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, 'O', 10, '2020-01-01'), (20, 2, 'F', 20, '2020-01-02');
        """
    )
    base = FLAGSHIP_PR
    pr = FLAGSHIP_BASE
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "1" in true_changed
    assert "1" in candidates


def test_3_right_filter_adds_first_matching_order() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_flagship(conn)
    true_changed, _candidates = assert_sound(conn, FLAGSHIP_BASE, FLAGSHIP_PR)
    assert "3" in true_changed


def test_4_one_of_several_matches_removed() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y');
        INSERT INTO stg_orders VALUES
          (10, 1, 'F', 10, '2020-01-01'),
          (11, 1, 'O', 11, '2020-01-02');
        """
    )
    base = FLAGSHIP_PR
    pr = FLAGSHIP_BASE
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "1" in true_changed
    assert "1" in candidates


def test_5_duplicate_right_rows_change_aggregate_multiplicity() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'Y');
        INSERT INTO stg_orders VALUES
          (10, 1, 'F', 10, '2020-01-01'),
          (11, 1, 'F', 10, '2020-01-01'),
          (20, 2, 'F', 20, '2020-01-02');
        """
    )
    base = FLAGSHIP_BASE
    pr = FLAGSHIP_BASE.replace("where status = 'F'", "where status = 'F' and id <> 11")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "1" in true_changed
    assert "1" in candidates


def test_6_predicate_true_null_and_null_true() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, NULL, 10, '2020-01-01'), (20, 2, 'F', 20, '2020-01-02');
        """
    )
    base = """
    with orders as (
      select id, customer_id, status from stg_orders
      where coalesce(status, 'F') = 'F'
    )
    select c.customer_id, count(o.id) as n
    from stg_customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_id
    """
    pr = base.replace("coalesce(status, 'F') = 'F'", "status = 'F'")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "1" in true_changed
    assert "1" in candidates


def test_7_null_join_keys_on_either_side() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (NULL, 'N', 'Y');
        INSERT INTO stg_orders VALUES
          (10, 1, 'F', 10, '2020-01-01'),
          (11, NULL, 'O', 11, '2020-01-02');
        """
    )
    true_changed, candidates = assert_sound(conn, FLAGSHIP_BASE, FLAGSHIP_PR)
    assert true_changed <= candidates


def test_8_filter_on_left_side_of_left_join() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'N'), (3, 'C', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, 'F', 10, '2020-01-01'), (20, 2, 'F', 20, '2020-01-02');
        """
    )
    base = """
    with customers as (
      select customer_id from stg_customers where active = 'Y'
    )
    select c.customer_id, count(o.id) as n
    from customers c
    left join stg_orders o on c.customer_id = o.customer_id
    group by c.customer_id
    """
    pr = base.replace("where active = 'Y'", "where active in ('Y', 'N')")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "2" in true_changed
    assert "2" in candidates


def test_9_inner_join_entity_key_on_fixed_side() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_flagship(conn)
    base = """
    with orders as (
      select id, customer_id, status from stg_orders where status = 'F'
    )
    select c.customer_id, count(o.id) as n
    from stg_customers c
    inner join orders o on c.customer_id = o.customer_id
    group by c.customer_id
    """
    pr = base.replace("where status = 'F'", "where status in ('F', 'O')")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "3" in true_changed
    assert "3" in candidates


def test_10_left_join_then_another_join_before_grouping() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (2, 'B', 'Y'), (3, 'C', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, 'F', 10, '2020-01-01'), (30, 3, 'O', 30, '2020-01-04');
        INSERT INTO stg_nations VALUES (1, 1, 'US'), (2, 2, 'UK'), (3, 3, 'FR');
        """
    )
    base = """
    with orders as (
      select id, customer_id, status from stg_orders where status = 'F'
    )
    select c.customer_id, count(o.id) as n
    from stg_customers c
    left join orders o on c.customer_id = o.customer_id
    inner join stg_nations n on c.customer_id = n.customer_id
    group by c.customer_id
    """
    pr = base.replace("where status = 'F'", "where status in ('F', 'O')")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "3" in true_changed
    assert "3" in candidates


def test_11_multiple_left_rows_map_to_same_entity_key() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y'), (1, 'A2', 'Y'), (2, 'B', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, 'O', 10, '2020-01-01'), (20, 2, 'F', 20, '2020-01-02');
        """
    )
    true_changed, candidates = assert_sound(conn, FLAGSHIP_BASE, FLAGSHIP_PR)
    assert "1" in true_changed
    assert "1" in candidates


def test_12_supported_aggregates_including_distinct() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_flagship(conn)
    base = """
    with orders as (
      select id, customer_id, status, total_price, order_date
      from stg_orders
      where status = 'F'
    )
    select
      c.customer_id,
      count(o.id) as n,
      count(distinct o.id) as d,
      sum(o.total_price) as s,
      avg(o.total_price) as a,
      min(o.order_date) as first_dt,
      max(o.order_date) as last_dt
    from stg_customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_id
    """
    pr = base.replace("where status = 'F'", "where status in ('F', 'O')")
    true_changed, candidates = assert_sound(conn, base, pr)
    assert "3" in true_changed
    assert "3" in candidates


def test_13_verified_star_and_ambiguous_star() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_flagship(conn)
    catalog = {
        "stg_orders": ("id", "customer_id", "status", "total_price", "order_date"),
        "stg_customers": ("customer_id", "customer_name", "active"),
    }
    star_base = """
    with orders as (
      select * from stg_orders where status = 'F'
    )
    select c.customer_id, count(o.id) as n
    from stg_customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_id
    """
    star_pr = star_base.replace("where status = 'F'", "where status in ('F', 'O')")
    true_changed, candidates = assert_sound(conn, star_base, star_pr, catalog=catalog)
    assert "3" in true_changed
    assert "3" in candidates
    ambiguous = _analyze(
        """
        select * from stg_customers c
        left join (select * from stg_orders where status = 'F') o
          on c.customer_id = o.customer_id
        group by customer_id
        """,
        """
        select * from stg_customers c
        left join (select * from stg_orders where status in ('F', 'O')) o
          on c.customer_id = o.customer_id
        group by customer_id
        """,
        catalog,
    )
    assert ambiguous.eligible is False
    assert ambiguous.reason_code == KEY_LINEAGE_BROKEN


def test_14_correlated_and_ambiguous_subqueries_fail_closed() -> None:
    correlated = _analyze(
        FLAGSHIP_BASE,
        FLAGSHIP_PR.replace(
            "where status in ('F', 'O')",
            "where status in ('F', 'O') and exists (select 1 from stg_customers x where x.customer_id = stg_orders.customer_id)",
        ),
    )
    assert correlated.eligible is False
    assert correlated.reason_code == UNSUPPORTED_OPERATION
    scalar = _analyze(
        FLAGSHIP_BASE,
        FLAGSHIP_PR.replace(
            "where status in ('F', 'O')",
            "where status in ('F', 'O') and id > (select max(id) from stg_orders)",
        ),
    )
    assert scalar.eligible is False
    assert scalar.reason_code == UNSUPPORTED_OPERATION


def test_15_reused_changed_cte_is_two_tainted() -> None:
    result = _analyze(
        """
        with changed as (
          select customer_id, status from stg_orders where status = 'F'
        )
        select a.customer_id, count(*) as n
        from changed a
        inner join changed b on a.customer_id = b.customer_id
        group by a.customer_id
        """,
        """
        with changed as (
          select customer_id, status from stg_orders where status in ('F', 'O')
        )
        select a.customer_id, count(*) as n
        from changed a
        inner join changed b on a.customer_id = b.customer_id
        group by a.customer_id
        """,
    )
    assert result.eligible is False
    assert result.reason_code == TWO_TAINTED_JOIN_INPUTS
    assert result.candidate_sql is None
    assert result.compiled is False


def test_16_missing_partial_mismatched_snapshot_never_sql_certified() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible and result.compiled
    dims = build_assessment_dimensions(
        snapshot=None,
        static_certified=filter_v1_sql_certified(
            eligible=True, compiled=True, confirmed=True, execution_failed=False
        ),
        candidates_confirmed=True,
    )
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["certification"]["failureCode"] == SOURCE_SNAPSHOT_NOT_PINNED

    partial = SourceSnapshot(
        identifier="snap-1",
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at="2026-01-01T00:00:00Z",
        relation_bindings=(),
        adapter_evidence=adapter_evidence_token("snap-1"),
    )
    dims = build_assessment_dimensions(snapshot=partial, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED

    verified = SourceSnapshot(
        identifier="snap-1",
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at="2026-01-01T00:00:00Z",
        relation_bindings=(),
        adapter_evidence=adapter_evidence_token("snap-1"),
        phases={"discovery": "snap-1", "targeted_base": "snap-other", "targeted_head": "snap-1", "confirmation": "snap-1"},
    )
    dims = build_assessment_dimensions(snapshot=verified, static_certified=True)
    assert dims["certification"]["status"] == UNCERTIFIED


def test_genuine_empty_set_only_after_compile_and_execute() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(CUSTOMERS)
    conn.executescript(
        """
        INSERT INTO stg_customers VALUES (1, 'A', 'Y');
        INSERT INTO stg_orders VALUES (10, 1, 'P', 10, '2020-01-01');
        """
    )
    base = FLAGSHIP_BASE.replace("where status = 'F'", "where status = 'Z'")
    pr = FLAGSHIP_BASE.replace("where status = 'F'", "where status = 'ZZ'")
    result = _analyze(base, pr)
    assert result.eligible is True
    assert result.compiled is True
    candidates = _rows(conn, result.candidate_sql or "")
    assert candidates == []
    true_changed = _true_changed(conn, base, pr)
    assert true_changed == set()
    assert CANDIDATE_SET_EMPTY == "empty"

    failed = _analyze(FLAGSHIP_BASE, FLAGSHIP_BASE.replace("left join", "full join"))
    assert failed.eligible is False
    assert failed.candidate_sql is None
    assert failed.compiled is False
    assert CANDIDATE_SET_ANALYSIS_FAILED == "analysis_failed"


def test_execution_failed_named_after_compile() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    snapshot = SourceSnapshot(
        identifier="snap-1",
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at="2026-01-01T00:00:00Z",
        relation_bindings=(),
        adapter_evidence=adapter_evidence_token("snap-1"),
        phases={
            "discovery": "snap-1",
            "targeted_base": "snap-1",
            "targeted_head": "snap-1",
            "confirmation": "snap-1",
        },
    )
    snapshot.relation_bindings = ()  # still not fully bound
    dims = build_assessment_dimensions(
        snapshot=None,
        static_certified=False,
        execution_failed=True,
        execution_ran=True,
    )
    enrich_certification_record(
        dims,
        eligibility=result.to_payload(),
        snapshot=None,
        candidate_fingerprint=result.candidate_fingerprint,
        execution_failed=True,
    )
    assert dims["certification"]["status"] != SQL_CERTIFIED
    assert dims["execution"]["status"] == EXECUTION_FAILED
    assert dims["certification"]["status"] == UNCERTIFIED
    assert "candidateQueryFingerprint" in dims["certification"]
    assert dims["baselineBoundary"]["comparesSqlVersionsAtPinnedSnapshot"] is True


def test_passing_targeted_comparison_alone_is_not_sql_certified() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    dims = build_assessment_dimensions(
        snapshot=None,
        static_certified=False,
        candidates_confirmed=True,
        targeted_ran=True,
        execution_ran=True,
    )
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["validation"]["status"] == "CANDIDATES_CONFIRMED"
    assert filter_v1_sql_certified(
        eligible=result.eligible,
        compiled=result.compiled,
        confirmed=True,
        execution_failed=False,
    )


def test_economics_are_independent_of_certification_and_bytes() -> None:
    assert economics_decision(targeted_ran=True) == ECONOMICS_NOT_EVALUATED
    assert (
        economics_decision(frontier_bytes=288_000_000, full_comparison_bytes=238_000_000)
        == FULL_REBUILD_RECOMMENDED
    )
    assert (
        economics_decision(
            frontier_bytes=100,
            full_comparison_bytes=200,
            warehouse_credits=None,
        )
        == ECONOMICS_NOT_EVALUATED
    )
    assert (
        economics_decision(
            frontier_bytes=100,
            full_comparison_bytes=200,
            warehouse_credits=0.01,
        )
        == TARGETED_REPAIR_RECOMMENDED
    )
    dims = build_assessment_dimensions(
        snapshot=None,
        static_certified=True,
        frontier_bytes=288_000_000,
        full_comparison_bytes=238_000_000,
    )
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["economics"]["decision"] == FULL_REBUILD_RECOMMENDED
    assert dims["economics"]["frontierBytesScanned"] == 288_000_000


JAFFLE_REPAIR_BASE = """
WITH customers AS (
    SELECT customer_id, customer_name
    FROM stg_customers
),
filtered_orders AS (
    SELECT order_id, customer_id, ordered_at, subtotal, tax_paid, order_total
    FROM orders
    WHERE order_total >= 20
)
SELECT
    customers.customer_id,
    MAX(customers.customer_name) AS customer_name,
    COUNT(filtered_orders.order_id) AS count_lifetime_orders,
    MIN(filtered_orders.ordered_at) AS first_ordered_at,
    MAX(filtered_orders.ordered_at) AS last_ordered_at,
    COALESCE(SUM(filtered_orders.subtotal), 0) AS lifetime_spend_pretax,
    COALESCE(SUM(filtered_orders.tax_paid), 0) AS lifetime_tax_paid,
    COALESCE(SUM(filtered_orders.order_total), 0) AS lifetime_spend
FROM customers
LEFT JOIN filtered_orders
    ON customers.customer_id = filtered_orders.customer_id
GROUP BY customers.customer_id
"""
JAFFLE_REPAIR_PR = JAFFLE_REPAIR_BASE.replace("order_total >= 20", "order_total >= 30")
JAFFLE_INCOMPLETE_CATALOG = {
    "stg_customers": ("customer_id",),
    "orders": ("order_id", "customer_id", "ordered_at", "subtotal", "tax_paid", "order_total"),
}


def _seed_jaffle_repair(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE stg_customers (
          customer_id INTEGER,
          customer_name TEXT
        );
        CREATE TABLE orders (
          order_id INTEGER,
          customer_id INTEGER,
          ordered_at TEXT,
          subtotal REAL,
          tax_paid REAL,
          order_total REAL
        );
        INSERT INTO stg_customers VALUES
          (1, 'Alice'),
          (2, 'Bob'),
          (3, 'Carol'),
          (4, 'Dan'),
          (5, 'Eve');
        INSERT INTO orders VALUES
          (10, 1, '2020-01-01', 8, 2, 10),
          (11, 1, '2020-01-02', 20, 5, 25),
          (12, 1, '2020-01-03', 20, 5, 25),
          (13, 1, '2020-01-04', 32, 8, 40),
          (20, 2, '2020-01-01', 12, 3, 15),
          (21, 2, '2020-01-02', 14, 4, 18),
          (30, 3, '2020-01-01', 28, 7, 35),
          (31, 3, '2020-01-02', 40, 10, 50),
          (40, 4, '2020-01-01', 18, 4, 22),
          (41, 4, '2020-01-02', 22, 6, 28),
          (50, 5, '2020-01-01', 16, 4, 20),
          (51, 5, '2020-01-02', 24, 6, 30);
        """
    )


def test_jaffle_repair_summary_compiles_and_is_sound() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_jaffle_repair(conn)
    result = _analyze(JAFFLE_REPAIR_BASE, JAFFLE_REPAIR_PR, JAFFLE_INCOMPLETE_CATALOG)
    assert result.eligible is True, result.diagnostic
    assert result.compiled is True, result.diagnostic
    assert result.candidate_sql
    sql = result.candidate_sql.lower()
    assert_candidate_sql_scopes(result.candidate_sql, dialect="snowflake")
    assert "customer_name" not in sql
    true_changed, candidates = assert_sound(
        conn,
        JAFFLE_REPAIR_BASE,
        JAFFLE_REPAIR_PR,
        catalog=JAFFLE_INCOMPLETE_CATALOG,
    )
    assert true_changed <= candidates
    assert true_changed == {"1", "4", "5"}
    assert "3" not in true_changed
    assert "2" not in true_changed
    assert "1" in candidates
    assert "4" in candidates
    assert "5" in candidates


def test_generated_scope_validation_rejects_dropped_column_reference() -> None:
    sql = """
    select P10.CUSTOMER_ID, P10.CUSTOMER_NAME
    from (
        select S9.CUSTOMER_ID
        from stg_customers as S9
    ) as P10
    """
    try:
        assert_candidate_sql_scopes(sql, dialect="snowflake")
    except PlanError as error:
        assert error.code == UNSUPPORTED_OPERATION
        assert "CUSTOMER_NAME" in error.diagnostic.upper() or "customer_name" in error.diagnostic.lower()
    else:
        raise AssertionError("expected unresolved CUSTOMER_NAME to fail compilation")


def test_left_side_key_only_projection_does_not_reference_dropped_descriptive_column() -> None:
    conn = sqlite3.connect(":memory:")
    _seed_jaffle_repair(conn)
    result = _analyze(JAFFLE_REPAIR_BASE, JAFFLE_REPAIR_PR, JAFFLE_INCOMPLETE_CATALOG)
    assert result.eligible is True, result.diagnostic
    assert result.compiled is True, result.diagnostic
    sql = result.candidate_sql or ""
    assert sql
    assert "customer_name" not in sql.lower()
    assert_candidate_sql_scopes(sql, dialect="snowflake")
    parsed = sqlglot.parse_one(sql, read="snowflake")
    assert parsed is not None
    true_changed, candidates = assert_sound(
        conn,
        JAFFLE_REPAIR_BASE,
        JAFFLE_REPAIR_PR,
        catalog=JAFFLE_INCOMPLETE_CATALOG,
    )
    assert true_changed <= candidates
    assert true_changed == {"1", "4", "5"}


def _certified_snapshot(identifier: str = "snap-cert-1") -> SourceSnapshot:
    return SourceSnapshot(
        identifier=identifier,
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at="2026-01-01T00:00:00Z",
        relation_bindings=(
            RelationBinding(name="stg_customers", relation_type=PERMANENT_TABLE, supported=True),
            RelationBinding(name="stg_orders", relation_type=PERMANENT_TABLE, supported=True),
        ),
        adapter_evidence=adapter_evidence_token(identifier),
        phases={
            "discovery": identifier,
            "targeted_base": identifier,
            "targeted_head": identifier,
            "confirmation": identifier,
        },
    )


def test_sql_certified_mart_mismatch_is_not_safe_in_place_repair() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible is True
    assert result.compiled is True
    dims = build_assessment_dimensions(
        snapshot=_certified_snapshot(),
        static_certified=True,
        candidates_confirmed=True,
        execution_ran=True,
        full_reference_failed=True,
    )
    assert dims["certification"]["status"] == SQL_CERTIFIED
    assert dims["validation"]["status"] == VALIDATION_FAILED
    assert dims["baselineBoundary"]["existingMaterializedMartRepresentsSnapshot"] is False
    assert dims["baselineBoundary"]["safeInPlaceProductionRepair"] is False
    assert dims["baselineBoundary"]["comparesSqlVersionsAtPinnedSnapshot"] is True

