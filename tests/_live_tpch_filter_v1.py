"""Live TPCH SF1 filter-v1 evidence: order_status F → IN (F,O)."""

from __future__ import annotations

from pathlib import Path

from frontier.filter_v1 import analyze_static_eligibility
from frontier.snapshot import collect_source_relations
from frontier.adapters.snowflake import load_snowflake_config, open_warehouse
from frontier.warehouse import sql_string

MANIFEST_FP = "f" * 64
CUSTOMER_REL = "FRONTIER_TEST.DBT_DEV.FRONTIER_21C_CUSTOMER"
ORDERS_REL = "FRONTIER_TEST.DBT_DEV.FRONTIER_21C_ORDERS"

BASE_SQL = f"""
with orders as (
  select O_ORDERKEY as id, O_CUSTKEY as customer_id, O_ORDERSTATUS as order_status
  from {ORDERS_REL}
  where O_ORDERSTATUS = 'F'
)
select c.customer_id, count(o.id) as order_count
from (
  select C_CUSTKEY as customer_id
  from {CUSTOMER_REL}
) as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

PR_SQL = BASE_SQL.replace("where O_ORDERSTATUS = 'F'", "where O_ORDERSTATUS in ('F', 'O')")


def _history(warehouse, query_id: str | None) -> dict:
    if not query_id:
        return {}
    try:
        rows = warehouse.execute(
            "select query_id, warehouse_name, warehouse_size, total_elapsed_time, "
            "bytes_scanned, partitions_scanned, rows_produced, compilation_time, execution_time "
            "from table(information_schema.query_history()) "
            f"where query_id = {sql_string(query_id)} "
            "order by start_time desc limit 1"
        )
    except Exception as error:
        return {"query_id": query_id, "history_error": str(error)}
    if not rows:
        return {"query_id": query_id}
    row = rows[0]
    return {
        "query_id": row[0],
        "warehouse_name": row[1],
        "warehouse_size": row[2],
        "total_elapsed_ms": row[3],
        "bytes_scanned": row[4],
        "partitions_scanned": row[5],
        "rows_produced": row[6],
        "compilation_ms": row[7],
        "execution_ms": row[8],
    }


def main() -> int:
    result = analyze_static_eligibility(
        BASE_SQL,
        PR_SQL,
        entity_key="customer_id",
        dialect="snowflake",
        manifest_version=17,
        manifest_fingerprint=MANIFEST_FP,
    )
    print("== compiler ==")
    print("eligible", result.eligible)
    print("compiled", result.compiled)
    print("reason", result.reason_code, result.compile_reason_code)
    print("diagnostic", result.diagnostic)
    print("old_plan_fingerprint", result.old_plan_fingerprint)
    print("new_plan_fingerprint", result.new_plan_fingerprint)
    print("changed_filter_node", result.changed_filter_node_id)
    print("candidate_fingerprint", result.candidate_fingerprint)
    print("candidate_sql")
    print(result.candidate_sql)
    if not (result.eligible and result.compiled and result.candidate_sql):
        return 1

    cfg = load_snowflake_config(Path("/Users/jad/Desktop/zetra_data_plane/jaffle_shop"), target="dev")
    warehouse = open_warehouse(cfg)
    jobs: list[tuple[str, str | None, dict]] = []
    try:
        def _has_table(name: str) -> bool:
            try:
                rows = warehouse.execute(f"select count(*) from {name}")
            except Exception:
                return False
            return bool(rows)

        if not _has_table(CUSTOMER_REL):
            print("== cloning customer ==")
            warehouse.execute(
                f"create table {CUSTOMER_REL} as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER"
            )
            print("clone customer query_id", getattr(warehouse, "last_query_id", None))
        if not _has_table(ORDERS_REL):
            print("== cloning orders ==")
            warehouse.execute(
                f"create table {ORDERS_REL} as select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS"
            )
            print("clone orders query_id", getattr(warehouse, "last_query_id", None))
        customer_n = warehouse.execute(f"select count(*) from {CUSTOMER_REL}")
        orders_n = warehouse.execute(f"select count(*) from {ORDERS_REL}")
        print("cloned_counts", customer_n, orders_n)
        sources = collect_source_relations(BASE_SQL, PR_SQL, result.candidate_sql, dialect="snowflake")
        print("== sources ==")
        print(sources)
        snapshot = warehouse.capture_snapshot(sources)
        print("== snapshot capture ==")
        print("mode", snapshot.mode)
        print("assurance", snapshot.assurance)
        print("identifier", snapshot.identifier)
        print("failure", snapshot.failure_code, snapshot.failure_reason)
        print("checked", snapshot.relations_checked, "bound", snapshot.relations_bound)
        print(
            "bindings",
            [b.to_safe_dict() | {"name": b.name, "type": b.relation_type} for b in snapshot.relation_bindings],
        )

        def run_sql(phase: str, sql: str, *, record: bool = True) -> list:
            warehouse.execute(f"alter session set query_tag = {sql_string('frontier_21c_' + phase)}")
            bound = sql if sql.lstrip().lower().startswith(("create", "alter", "select count")) else warehouse.bind_query_to_snapshot(sql, snapshot)
            if sql.lstrip().lower().startswith("create"):
                verified = True
            elif " at (" in bound.lower() or " at(" in bound.lower():
                verified = warehouse.verify_snapshot_binding(bound, snapshot)
            else:
                verified = False
            print(f"== {phase} bind_verified={verified} ==")
            if phase in {"discovery", "full_reference"}:
                print("sql")
                print(bound[:4000])
            try:
                rows = warehouse.execute(bound)
            except Exception as error:
                qid = getattr(warehouse, "last_query_id", None)
                print(f"{phase} FAILED query_id={qid} error={error}")
                raise
            qid = getattr(warehouse, "last_query_id", None)
            if record:
                snapshot.record_phase(phase)
            jobs.append((phase, qid, _history(warehouse, qid)))
            print(f"{phase} query_id", qid, "rows", len(rows), "history", jobs[-1][2])
            warehouse.execute("alter session unset query_tag")
            return rows

        cand_bound = warehouse.bind_query_to_snapshot(result.candidate_sql, snapshot)
        print("== discovery bound_sql ==")
        print(cand_bound)
        run_sql(
            "discovery",
            "create or replace temporary table frontier_21c_candidates as " + cand_bound,
            record=True,
        )
        cand_count = run_sql("discovery_count", "select count(*) from frontier_21c_candidates", record=False)
        print("candidate_customers", cand_count)

        old_bound = warehouse.bind_query_to_snapshot(BASE_SQL, snapshot)
        new_bound = warehouse.bind_query_to_snapshot(PR_SQL, snapshot)
        run_sql(
            "targeted_base",
            "create or replace temporary table frontier_21c_old_targeted as "
            f"select * from ({old_bound}) as frontier_old "
            "where customer_id in (select customer_id from frontier_21c_candidates)",
        )
        run_sql(
            "targeted_head",
            "create or replace temporary table frontier_21c_new_targeted as "
            f"select * from ({new_bound}) as frontier_new "
            "where customer_id in (select customer_id from frontier_21c_candidates)",
        )
        confirmed = run_sql(
            "confirmation",
            """
            select count(*) from (
              select * from frontier_21c_old_targeted
              minus
              select * from frontier_21c_new_targeted
              union
              select * from frontier_21c_new_targeted
              minus
              select * from frontier_21c_old_targeted
            )
            """,
        )
        print("confirmed_changed_customers", confirmed)

        true_sql = warehouse.bind_query_to_snapshot(
            f"""
            select customer_id from (
              select * from ({BASE_SQL}) as o
              minus
              select * from ({PR_SQL}) as n
              union
              select * from ({PR_SQL}) as n2
              minus
              select * from ({BASE_SQL}) as o2
            )
            """,
            snapshot,
        )
        run_sql(
            "full_reference",
            "create or replace temporary table frontier_21c_true_changed as " + true_sql,
        )
        true_count = run_sql("full_reference_count", "select count(*) from frontier_21c_true_changed", record=False)
        missed = run_sql(
            "set_check",
            """
            select count(*) as missed from (
              select customer_id from frontier_21c_true_changed
              minus
              select customer_id from frontier_21c_candidates
            )
            """,
            record=False,
        )
        extra = run_sql(
            "extra_candidates",
            """
            select count(*) as extra from (
              select customer_id from frontier_21c_candidates
              minus
              select customer_id from frontier_21c_true_changed
            )
            """,
            record=False,
        )
        print("== full reference ==")
        print("true_changed_customers", true_count)
        print("set_check_missed_rows", missed)
        print("extra_candidates", extra)
        print("== snapshot phases ==")
        print(snapshot.phases)
        print("consistent", snapshot.consistent)
        print("allows_sql_certified", snapshot.allows_sql_certified())
        print("== job costs ==")
        for phase, qid, profile in jobs:
            print(phase, qid, profile)
        return 0 if missed and missed[0][0] == 0 else 2
    finally:
        warehouse.close()


if __name__ == "__main__":
    raise SystemExit(main())
