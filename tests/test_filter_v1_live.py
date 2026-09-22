"""Live Snowflake Jaffle Shop filter-v1 acceptance. Skips without credentials."""

from __future__ import annotations

import pytest

from frontier.filter_v1 import analyze_static_eligibility
from frontier.warehouse import env_value

MANIFEST_FP = "c" * 64

JAFFLE_BASE = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status = 'F'
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

JAFFLE_PR = JAFFLE_BASE.replace("where status = 'F'", "where status in ('F', 'O')")


def _snowflake_configured() -> bool:
    return bool(env_value("SNOWFLAKE_ACCOUNT") and env_value("SNOWFLAKE_USER"))


@pytest.mark.skipif(not _snowflake_configured(), reason="Snowflake credentials are not configured")
def test_live_jaffle_order_status_filter_change() -> None:
    from frontier.adapters.snowflake import SnowflakeAdapter
    from frontier.snapshot import bind_sql_to_snapshot, capture_from_catalog, collect_source_relations

    result = analyze_static_eligibility(
        JAFFLE_BASE,
        JAFFLE_PR,
        entity_key="customer_id",
        dialect="snowflake",
        manifest_version=1,
        manifest_fingerprint=MANIFEST_FP,
    )
    assert result.eligible is True
    assert result.compiled is True
    assert result.candidate_sql
    warehouse = SnowflakeAdapter()
    try:
        sources = collect_source_relations(JAFFLE_PR, dialect="snowflake")
        snapshot = capture_from_catalog(sources, catalog=getattr(warehouse, "relation_catalog", {}) or {})
        bound = bind_sql_to_snapshot(result.candidate_sql, snapshot, dialect="snowflake")
        rows = warehouse.execute(bound)
        candidates = {str(row[0]) for row in rows if row and row[0] is not None}
        old_rows = warehouse.execute(bind_sql_to_snapshot(JAFFLE_BASE, snapshot, dialect="snowflake"))
        new_rows = warehouse.execute(bind_sql_to_snapshot(JAFFLE_PR, snapshot, dialect="snowflake"))
        old_map = {str(row[0]): row[1:] for row in old_rows if row and row[0] is not None}
        new_map = {str(row[0]): row[1:] for row in new_rows if row and row[0] is not None}
        true_changed = {
            key for key in set(old_map) | set(new_map) if old_map.get(key) != new_map.get(key)
        }
        missed = true_changed - candidates
        print("snapshot_mode", getattr(snapshot, "mode", None))
        print("snapshot_assurance", getattr(snapshot, "assurance", None))
        print("snapshot_identifier", getattr(snapshot, "identifier", None))
        print("old_plan", result.old_plan_fingerprint)
        print("new_plan", result.new_plan_fingerprint)
        print("filter_node", result.changed_filter_node_id)
        print("candidate_fp", result.candidate_fingerprint)
        print("candidates", len(candidates))
        print("true_changed", len(true_changed))
        print("missed", len(missed))
        print("full_old", len(old_map))
        print("full_new", len(new_map))
        assert missed == set()
    finally:
        warehouse.close()


def test_live_jaffle_reports_skip_without_credentials() -> None:
    if _snowflake_configured():
        pytest.skip("live credentials present; the live test above is the acceptance run")
    assert not env_value("SNOWFLAKE_ACCOUNT") or not env_value("SNOWFLAKE_USER")
