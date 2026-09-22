from __future__ import annotations

import json

from frontier.certification import UNCERTIFIED, build_assessment_dimensions, normalize_certification_status
from frontier.compare import compare_manifests, comparison_for_ingest, format_compare_report
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.filter_v1 import (
    FLIP_RULE,
    KEY_LINEAGE_BROKEN,
    PLAN_NODE_CORRESPONDENCE_FAILED,
    TWO_TAINTED_JOIN_INPUTS,
    UNSUPPORTED_OPERATION,
    analyze_static_eligibility,
)
from frontier.snapshot import ASSURANCE_ADAPTER

MANIFEST_FP = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ENTITY_KEY = "customer_id"

FLAGSHIP_BASE = """
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

FLAGSHIP_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

LEFT_TAINT_BASE = """
with customers as (
  select id as customer_id
  from stg_customers
  where active = 'Y'
)
select c.customer_id, count(o.id) as order_count
from customers as c
inner join stg_orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

LEFT_TAINT_PR = """
with customers as (
  select id as customer_id
  from stg_customers
  where active in ('Y', 'N')
)
select c.customer_id, count(o.id) as order_count
from customers as c
inner join stg_orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

BOTH_TAINT_BASE = """
with changed as (
  select customer_id, status
  from stg_orders
  where status = 'F'
)
select a.customer_id, count(*) as n
from changed as a
inner join changed as b
  on a.customer_id = b.customer_id
group by a.customer_id
"""

BOTH_TAINT_PR = """
with changed as (
  select customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select a.customer_id, count(*) as n
from changed as a
inner join changed as b
  on a.customer_id = b.customer_id
group by a.customer_id
"""

TWO_FILTER_BASE = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status = 'F'
),
customers as (
  select id as customer_id
  from stg_customers
  where active = 'Y'
)
select c.customer_id, count(o.id) as order_count
from customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

TWO_FILTER_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
),
customers as (
  select id as customer_id
  from stg_customers
  where active in ('Y', 'N')
)
select c.customer_id, count(o.id) as order_count
from customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

ALIAS_BASE = FLAGSHIP_BASE
ALIAS_PR = """
with orders as (
  -- formatting and table aliases only
  select id, customer_id, status
  from stg_orders as src
  where status = 'F'
)
select cust.customer_id, count(ord.id) as order_count
from stg_customers as cust
left join orders as ord
  on cust.customer_id = ord.customer_id
group by cust.customer_id
"""

JOIN_AND_FILTER_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.id
group by c.customer_id
"""

AGG_AND_FILTER_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, sum(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

RENAME_BASE = """
with customers as (
  select id as customer_id
  from stg_customers
),
orders as (
  select id, customer_id, status
  from stg_orders
  where status = 'F'
)
select c.customer_id, count(o.id) as order_count
from customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

RENAME_PR = """
with customers as (
  select id as customer_id
  from stg_customers
),
orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) as order_count
from customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

DROPPED_KEY_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.region as region, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.region
"""

OVERWRITE_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select 1 as customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by 1
"""

AMBIGUOUS_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by customer_id
"""

SECOND_JOIN_BASE = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status = 'F'
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
inner join stg_payments as p
  on c.customer_id = p.customer_id
group by c.customer_id
"""

SECOND_JOIN_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
inner join stg_payments as p
  on c.customer_id = p.customer_id
group by c.customer_id
"""

WINDOW_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) over (partition by c.customer_id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
"""

RECURSIVE_PR = """
with recursive orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
  union all
  select id, customer_id, status from orders
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

SET_OP_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
union
select c.customer_id, 0 as order_count
from stg_customers as c
group by c.customer_id
"""


def _analyze(base: str, pr: str, **kwargs):
    return analyze_static_eligibility(
        base,
        pr,
        entity_key=kwargs.get("entity_key", ENTITY_KEY),
        dialect="snowflake",
        manifest_version=kwargs.get("manifest_version", 17),
        manifest_fingerprint=kwargs.get("manifest_fingerprint", MANIFEST_FP),
    )


def _taint_sides(result) -> list[str]:
    return [item["taint"] for item in result.join_taint]


def test_flagship_filter_change_is_right_only_taint() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible is True
    assert result.reason_code is None
    assert result.changed_filter_node_id
    assert result.changed_filter_node_id.startswith("filter:")
    assert _taint_sides(result) == ["right"]
    assert "leftjoin" in result.operators
    assert "group" in result.operators
    payload = result.to_payload()
    assert payload["ruleSetVersion"] == "filter-v1"
    assert payload["manifestDependencies"][0]["manifestVersion"] == 17
    assert payload["manifestDependencies"][0]["fingerprint"] == MANIFEST_FP
    assert "ADAPTER_VERIFIED" not in json.dumps(payload)
    assert "'" not in (payload.get("diagnostic") or "")


def test_left_input_filter_is_left_only_taint() -> None:
    result = _analyze(LEFT_TAINT_BASE, LEFT_TAINT_PR)
    assert result.eligible is True
    assert _taint_sides(result) == ["left"]


def test_reused_cte_on_both_join_sides_is_two_tainted() -> None:
    result = _analyze(BOTH_TAINT_BASE, BOTH_TAINT_PR)
    assert result.eligible is False
    assert result.reason_code == TWO_TAINTED_JOIN_INPUTS
    assert _taint_sides(result) == ["both"]
    assert "one-sided join rule" in (result.diagnostic or "")


def test_two_changed_filters_fail_correspondence() -> None:
    result = _analyze(TWO_FILTER_BASE, TWO_FILTER_PR)
    assert result.eligible is False
    assert result.reason_code == PLAN_NODE_CORRESPONDENCE_FAILED
    dumped = json.dumps(result.to_payload())
    assert "empty" not in dumped.lower() or "must not be inferred" in (result.diagnostic or "")


def test_alias_comment_formatting_is_not_a_semantic_change() -> None:
    result = _analyze(ALIAS_BASE, ALIAS_PR)
    assert result.eligible is False
    assert result.semantic_change is False
    assert result.reason_code is None


def test_join_condition_change_with_filter_is_ineligible() -> None:
    result = _analyze(FLAGSHIP_BASE, JOIN_AND_FILTER_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert result.reason_code != PLAN_NODE_CORRESPONDENCE_FAILED


def test_aggregate_argument_change_with_filter_is_ineligible() -> None:
    result = _analyze(FLAGSHIP_BASE, AGG_AND_FILTER_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION


def test_verified_entity_key_rename_preserves_lineage() -> None:
    result = _analyze(RENAME_BASE, RENAME_PR)
    assert result.eligible is True
    statuses = {item["column"]: item["status"] for item in result.key_lineage}
    assert statuses.get("customer_id") in {"tracked", "renamed"}
    renamed = [item for item in result.key_lineage if item.get("renameFrom")]
    assert renamed


def test_dropped_entity_key_breaks_lineage() -> None:
    result = _analyze(FLAGSHIP_BASE, DROPPED_KEY_PR)
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN


def test_overwritten_entity_key_breaks_lineage() -> None:
    result = _analyze(FLAGSHIP_BASE, OVERWRITE_PR)
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN


def test_ambiguous_entity_key_breaks_lineage() -> None:
    result = _analyze(FLAGSHIP_BASE, AMBIGUOUS_PR)
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN


def test_left_join_then_supported_join_continues_taint() -> None:
    result = _analyze(SECOND_JOIN_BASE, SECOND_JOIN_PR)
    assert result.eligible is True
    assert _taint_sides(result) == ["right", "left"]
    assert len(result.join_taint) == 2


def test_window_fails_closed() -> None:
    result = _analyze(FLAGSHIP_BASE, WINDOW_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert "scope restriction" in (result.diagnostic or "")


def test_recursive_cte_fails_closed() -> None:
    result = _analyze(FLAGSHIP_BASE, RECURSIVE_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert "recursive" in (result.diagnostic or "").lower()


def test_set_operation_fails_closed() -> None:
    result = _analyze(FLAGSHIP_BASE, SET_OP_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert "set operation" in (result.diagnostic or "").lower()


def test_manifest_fingerprint_is_recorded_not_substituted_for_checks() -> None:
    result = _analyze(
        FLAGSHIP_BASE,
        DROPPED_KEY_PR,
        manifest_version=17,
        manifest_fingerprint=MANIFEST_FP,
    )
    payload = result.to_payload()
    assert payload["eligible"] is False
    assert payload["reasonCode"] == KEY_LINEAGE_BROKEN
    assert payload["manifestDependencies"][0]["manifestVersion"] == 17
    assert payload["manifestDependencies"][0]["fingerprint"] == MANIFEST_FP
    assert payload["manifestDependencies"][0]["fields"] == ["targetModel", "entity", "entityKey"]


def test_static_eligibility_never_invents_adapter_verified() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    payload = result.to_payload()
    dumped = json.dumps(payload)
    assert "ADAPTER_VERIFIED" not in dumped
    assert "assurance" not in payload
    dims = build_assessment_dimensions(snapshot=None)
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["sourceSnapshot"]["assurance"] != ASSURANCE_ADAPTER


def test_eligible_plan_does_not_emit_sql_certified() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible is True
    dims = build_assessment_dimensions(snapshot=None, static_certified=False)
    assert dims["certification"]["status"] == UNCERTIFIED


def test_normalize_uncified_misspelling() -> None:
    assert normalize_certification_status("UNCIFIED") == UNCERTIFIED
    assert normalize_certification_status(UNCERTIFIED) == UNCERTIFIED


def _node(name: str, sql: str) -> DbtNode:
    return DbtNode(
        unique_id=f"model.jaffle_shop.{name}",
        name=name,
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name=f"DATA_AGENT_DEV.DBT_DEV.{name}",
        depends_on=(),
        original_file_path=f"models/{name}.sql",
        compiled_code=sql,
        package_name="jaffle_shop",
        tags=(),
    )


def _manifest(*nodes: DbtNode) -> Manifest:
    return Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={node.unique_id: node for node in nodes},
        sources={},
    )


def test_compare_exposes_eligibility_without_uploading_sql() -> None:
    comparison = compare_manifests(
        _manifest(_node("customer_summary", FLAGSHIP_BASE)),
        _manifest(_node("customer_summary", FLAGSHIP_PR)),
        entity_key=ENTITY_KEY,
        target_name="customer_summary",
        semantic_manifest_version=17,
        semantic_manifest_fingerprint=MANIFEST_FP,
    ).to_dict()
    eligibility = comparison["staticEligibility"]
    assert eligibility["eligible"] is True
    assert eligibility["ruleSetVersion"] == "filter-v1"
    assert comparison["modified"][0]["changeKinds"] == ["FILTER_CHANGED"]
    report = format_compare_report(comparison)
    assert "filter-v1 static eligibility: eligible" in report
    assert "candidate SQL compiled" in report
    assert "Narrow frontier safe: yes" not in report
    assert "certification: pending warehouse execution" in report
    assert "narrow frontier decision: not yet established" in report
    ingested = comparison_for_ingest(comparison)
    dumped = json.dumps(ingested)
    assert "where status" not in dumped.lower()
    assert "ADAPTER_VERIFIED" not in dumped
    assert ingested["staticEligibility"]["manifestDependencies"][0]["fingerprint"] == MANIFEST_FP


def test_compare_does_not_call_non_filter_change_a_no_op() -> None:
    comparison = compare_manifests(
        _manifest(_node("customer_summary", FLAGSHIP_BASE)),
        _manifest(_node("customer_summary", JOIN_AND_FILTER_PR)),
        entity_key=ENTITY_KEY,
        target_name="customer_summary",
    ).to_dict()
    assert comparison["modified"]
    eligibility = comparison["staticEligibility"]
    assert eligibility["eligible"] is False
    assert eligibility.get("semanticChange") is not False
    report = format_compare_report(comparison)
    assert "no semantic change" not in report


CATALOG = {
    "stg_orders": ("id", "customer_id", "status", "total_price"),
    "stg_customers": ("customer_id", "customer_name"),
}

STAR_BASE = """
with orders as (
  select * from stg_orders where status = 'F'
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

STAR_PR = STAR_BASE.replace("where status = 'F'", "where status in ('F', 'O')")

AMBIGUOUS_STAR_BASE = """
select * from stg_customers as c
left join (
  select * from stg_orders where status = 'F'
) as o
  on c.customer_id = o.customer_id
group by customer_id
"""

AMBIGUOUS_STAR_PR = AMBIGUOUS_STAR_BASE.replace(
    "where status = 'F'",
    "where status in ('F', 'O')",
)

CORRELATED_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders o
  where status in ('F', 'O')
    and exists (
      select 1 from stg_customers c2 where c2.customer_id = o.customer_id
    )
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""

SCALAR_SUBQUERY_PR = """
with orders as (
  select id, customer_id, status
  from stg_orders
  where status in ('F', 'O')
    and id > (select max(id) from stg_orders)
)
select c.customer_id, count(o.id) as order_count
from stg_customers as c
left join orders as o
  on c.customer_id = o.customer_id
group by c.customer_id
"""


def test_verified_star_expansion_tracks_keys() -> None:
    result = analyze_static_eligibility(
        STAR_BASE,
        STAR_PR,
        entity_key=ENTITY_KEY,
        dialect="snowflake",
        manifest_version=17,
        manifest_fingerprint=MANIFEST_FP,
        schema_catalog=CATALOG,
    )
    assert result.eligible is True
    assert result.compiled is True
    assert result.candidate_sql
    assert "is distinct from" in result.candidate_sql.lower()
    assert "STG_ORDERS" in result.candidate_sql
    assert '"stg_orders"' not in result.candidate_sql


def test_star_without_catalog_fails_closed() -> None:
    result = _analyze(STAR_BASE, STAR_PR)
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN
    assert "star" in (result.diagnostic or "").lower()


def test_ambiguous_star_fails_closed() -> None:
    result = analyze_static_eligibility(
        AMBIGUOUS_STAR_BASE,
        AMBIGUOUS_STAR_PR,
        entity_key=ENTITY_KEY,
        dialect="snowflake",
        manifest_version=17,
        manifest_fingerprint=MANIFEST_FP,
        schema_catalog=CATALOG,
    )
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN


def test_correlated_subquery_fails_closed() -> None:
    result = _analyze(FLAGSHIP_BASE, CORRELATED_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert "subquer" in (result.diagnostic or "").lower() or "exist" in (result.diagnostic or "").lower()


def test_correlation_ambiguous_subquery_fails_closed() -> None:
    result = _analyze(FLAGSHIP_BASE, SCALAR_SUBQUERY_PR)
    assert result.eligible is False
    assert result.reason_code == UNSUPPORTED_OPERATION
    assert "correlation" in (result.diagnostic or "").lower() or "subquer" in (result.diagnostic or "").lower()


def test_eligible_plan_compiles_rowcover_sql() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible is True
    assert result.compiled is True
    sql = (result.candidate_sql or "").lower()
    assert "is distinct from" in sql
    assert "coalesce" in sql
    assert "select distinct" in sql
    assert "union" in sql
    assert result.candidate_fingerprint
    payload = result.to_payload()
    assert payload["compiled"] is True
    assert payload["candidateFingerprint"] == result.candidate_fingerprint
    assert "candidateSql" not in payload
    assert FLIP_RULE in payload["checkedAssumptions"]


def test_two_tainted_join_does_not_compile_an_empty_set() -> None:
    result = _analyze(BOTH_TAINT_BASE, BOTH_TAINT_PR)
    assert result.eligible is False
    assert result.reason_code == TWO_TAINTED_JOIN_INPUTS
    assert result.compiled is False
    assert result.candidate_sql is None
    payload = result.to_payload()
    assert payload.get("candidateSetState") is None
    assert "empty" not in json.dumps(payload).lower() or "must not" in (result.diagnostic or "").lower()


def test_compare_uses_filter_v1_compiler_not_impact_shortcut() -> None:
    comparison = compare_manifests(
        _manifest(_node("customer_summary", FLAGSHIP_BASE)),
        _manifest(_node("customer_summary", FLAGSHIP_PR)),
        entity_key=ENTITY_KEY,
        target_name="customer_summary",
        semantic_manifest_version=17,
        semantic_manifest_fingerprint=MANIFEST_FP,
    ).to_dict()
    sql = (comparison["modified"][0].get("candidateSql") or "").lower()
    assert "union" in sql
    assert "is distinct from" in sql
    assert comparison["modified"][0]["staticEligibility"]["compiled"] is True
    ingested = comparison_for_ingest(comparison)
    assert "candidateSql" not in ingested["modified"][0]
    assert ingested["modified"][0]["queryFingerprint"]


def test_missing_snapshot_never_sql_certified() -> None:
    result = _analyze(FLAGSHIP_BASE, FLAGSHIP_PR)
    assert result.eligible is True
    assert result.compiled is True
    dims = build_assessment_dimensions(
        snapshot=None,
        static_certified=True,
        candidates_confirmed=True,
        execution_ran=True,
    )
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["baselineBoundary"]["existingMaterializedMartRepresentsSnapshot"] is False
    assert dims["baselineBoundary"]["safeInPlaceProductionRepair"] is False


def test_ineligible_plan_keeps_key_lineage_reason() -> None:
    from frontier.certification import enrich_certification_record

    result = _analyze("select * from stg_orders where status = 'F'", "select * from stg_orders where status in ('F', 'O')")
    assert result.eligible is False
    assert result.reason_code == KEY_LINEAGE_BROKEN
    dims = build_assessment_dimensions(snapshot=None, static_certified=False)
    enrich_certification_record(
        dims,
        eligibility=result.to_payload(),
        snapshot=None,
    )
    assert dims["certification"]["status"] == UNCERTIFIED
    assert dims["certification"]["failureCode"] == KEY_LINEAGE_BROKEN
