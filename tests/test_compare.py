from __future__ import annotations

from pathlib import Path

from frontier.compare import (
    compare_manifests,
    compiled_sql_for,
    compiled_sql_pair_for_sql_change,
    stamp_impact_execution,
)
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.sql_fingerprint import sql_fingerprint

STG_CUSTOMERS_SQL = "select id as customer_id from customer"
STG_ORDERS_SQL = "select id as order_id, customer_id from orders where status = 'complete'"
STG_ORDERS_FILTER_SQL = "select id as order_id, customer_id from orders where status = 'returned'"
STG_LEGACY_SQL = "select id as legacy_id from legacy"
INT_SQL = "select * from stg_customers join stg_orders using (customer_id)"
MART_SQL = "select customer_id, count(*) as order_count from int_customer_orders group by 1"


def _node(
    name: str,
    *,
    sql: str,
    depends: tuple[str, ...] = (),
    compiled_code: str | None = None,
    original_file_path: str | None = None,
) -> DbtNode:
    unique_id = f"model.jaffle_shop.{name}"
    return DbtNode(
        unique_id=unique_id,
        name=name,
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name=f"DATA_AGENT_DEV.DBT_DEV.{name}",
        depends_on=depends,
        original_file_path=original_file_path or f"models/{name}.sql",
        compiled_code=sql if compiled_code is None else compiled_code,
        package_name="jaffle_shop",
    )


def _manifest(*nodes: DbtNode) -> Manifest:
    return Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={node.unique_id: node for node in nodes},
        sources={},
    )


def _graph(*, orders_sql: str = STG_ORDERS_SQL, include_legacy: bool = True) -> list[DbtNode]:
    stg_customers = _node("stg_customers", sql=STG_CUSTOMERS_SQL)
    stg_orders = _node("stg_orders", sql=orders_sql)
    nodes = [stg_customers, stg_orders]
    int_deps = [
        "model.jaffle_shop.stg_customers",
        "model.jaffle_shop.stg_orders",
    ]
    if include_legacy:
        nodes.append(_node("stg_legacy", sql=STG_LEGACY_SQL))
        int_deps.append("model.jaffle_shop.stg_legacy")
    nodes.append(
        _node(
            "int_customer_orders",
            sql=INT_SQL,
            depends=tuple(int_deps),
        )
    )
    nodes.append(
        _node(
            "customer_summary",
            sql=MART_SQL,
            depends=("model.jaffle_shop.int_customer_orders",),
        )
    )
    return nodes


def test_whitespace_and_comments_are_unchanged() -> None:
    base = _manifest(*_graph())
    pr_nodes = _graph()
    pr_nodes[1] = _node(
        "stg_orders",
        sql=STG_ORDERS_SQL + "\n-- comment only\n",
    )
    pr = _manifest(*pr_nodes)
    comparison = compare_manifests(base, pr).to_dict()
    assert comparison["modified"] == []
    assert comparison["added"] == []
    assert comparison["removed"] == []
    assert comparison["base"]["fingerprint"] == comparison["pr"]["fingerprint"]
    assert comparison["base"]["modelCount"] == 5
    assert comparison["pr"]["modelCount"] == 5


def test_filter_change_marks_model_modified() -> None:
    base = _manifest(*_graph())
    pr = _manifest(*_graph(orders_sql=STG_ORDERS_FILTER_SQL))
    comparison = compare_manifests(base, pr, entity_key="customer_id").to_dict()
    modified = comparison["modified"]
    assert [item["uniqueId"] for item in modified] == ["model.jaffle_shop.stg_orders"]
    assert modified[0]["baseFingerprint"] != modified[0]["prFingerprint"]
    downstream = {item["name"] for item in modified[0]["downstream"]}
    assert downstream == {"int_customer_orders", "customer_summary"}
    assert comparison["base"]["fingerprint"] != comparison["pr"]["fingerprint"]
    assert modified[0]["changeKinds"] == ["FILTER_CHANGED"]
    assert modified[0]["unsafe"] is False
    assert modified[0]["impactStatus"] == "COMPILED"
    assert "is distinct from" in (modified[0].get("candidateSql") or "").lower()
    assert "FILTER_CHANGED" in (modified[0].get("changeSummary") or "")
    assert comparison["narrowFrontierSafe"] is True


def test_removed_model_reports_downstream_consumers() -> None:
    base = _manifest(*_graph(include_legacy=True))
    pr = _manifest(*_graph(include_legacy=False))
    comparison = compare_manifests(base, pr).to_dict()
    assert [item["uniqueId"] for item in comparison["removed"]] == [
        "model.jaffle_shop.stg_legacy"
    ]
    downstream = {item["name"] for item in comparison["removed"][0]["downstream"]}
    assert downstream == {"int_customer_orders", "customer_summary"}
    assert comparison["added"] == []


def test_added_model_is_classified() -> None:
    base = _manifest(*_graph(include_legacy=False))
    extra = _node("new_mart", sql="select 1 as id", depends=("model.jaffle_shop.customer_summary",))
    pr = _manifest(*_graph(include_legacy=False), extra)
    comparison = compare_manifests(base, pr).to_dict()
    assert [item["name"] for item in comparison["added"]] == ["new_mart"]
    assert comparison["added"][0]["baseFingerprint"] is None
    assert comparison["added"][0]["prFingerprint"] == sql_fingerprint(
        "select 1 as id",
        dialect="snowflake",
    )


def test_comparison_does_not_include_entity_rows() -> None:
    comparison = compare_manifests(_manifest(*_graph()), _manifest(*_graph())).to_dict()
    dumped = str(comparison)
    assert "affectedEntities" not in dumped
    assert "entityValue" not in dumped
    assert "changeEvents" not in dumped


def test_compiled_sql_falls_back_to_compiled_dir(tmp_path: Path) -> None:
    relative = Path("models/stg_orders.sql")
    compiled = tmp_path / "compiled" / "jaffle_shop" / relative
    compiled.parent.mkdir(parents=True)
    compiled.write_text(STG_ORDERS_FILTER_SQL)
    node = _node("stg_orders", sql="", compiled_code="", original_file_path=str(relative))
    assert compiled_sql_for(node, tmp_path / "compiled") == STG_ORDERS_FILTER_SQL

    base = _manifest(_node("stg_orders", sql=STG_ORDERS_SQL))
    pr_node = DbtNode(
        unique_id="model.jaffle_shop.stg_orders",
        name="stg_orders",
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name="DATA_AGENT_DEV.DBT_DEV.stg_orders",
        depends_on=(),
        original_file_path=str(relative),
        compiled_code=None,
        package_name="jaffle_shop",
    )
    pr = _manifest(pr_node)
    comparison = compare_manifests(
        base,
        pr,
        pr_compiled_root=tmp_path / "compiled",
        entity_key="customer_id",
    ).to_dict()
    assert comparison["modified"][0]["uniqueId"] == "model.jaffle_shop.stg_orders"
    assert comparison["modified"][0]["impactStatus"] == "COMPILED"


def test_alias_only_sql_is_not_modified() -> None:
    base = _manifest(
        _node(
            "stg_orders",
            sql="select o.status as order_status from orders as o where o.status = 'F'",
        )
    )
    pr = _manifest(
        _node(
            "stg_orders",
            sql="SELECT status FROM orders WHERE status = 'F'",
        )
    )
    comparison = compare_manifests(base, pr).to_dict()
    assert comparison["modified"] == []
    assert comparison["narrowFrontierSafe"] is True


def test_grouping_change_is_unsafe() -> None:
    base = _manifest(
        _node(
            "customer_summary",
            sql="select customer_id, count(*) from orders group by customer_id",
        )
    )
    pr = _manifest(
        _node(
            "customer_summary",
            sql="select customer_id, status, count(*) from orders group by customer_id, status",
        )
    )
    comparison = compare_manifests(base, pr, entity_key="customer_id").to_dict()
    assert comparison["modified"][0]["changeKinds"] == ["EXPRESSION_CHANGED", "GROUPING_CHANGED"]
    assert comparison["modified"][0]["unsafe"] is True
    assert comparison["modified"][0]["impactStatus"] == "FULL_REBUILD_REQUIRED"
    assert comparison["fullRebuildRequired"] is True
    assert comparison["narrowFrontierSafe"] is False
    dumped = str(comparison)
    assert "affectedEntities" not in dumped


def test_unparseable_sql_is_unsupported_not_empty() -> None:
    base = _manifest(_node("stg_orders", sql=STG_ORDERS_SQL))
    pr = _manifest(_node("stg_orders", sql="not valid snowflake sql [[["))
    comparison = compare_manifests(base, pr).to_dict()
    assert comparison["modified"][0]["changeKinds"] == ["UNSUPPORTED"]
    assert comparison["modified"][0]["unsafe"] is True
    assert comparison["narrowFrontierSafe"] is False
    assert comparison["modified"][0]["unsupportedReasons"]


def test_cte_filter_change_compiles_for_narrow_frontier() -> None:
    base_sql = """
        with customers as (
            select c.* from stg_customers c
        ),
        orders as (
            select o.* from stg_orders o where o.order_status = 'F'
        )
        select c.customer_id, count(o.order_id) as total_orders
        from customers c
        left join orders o on c.customer_id = o.customer_id
        group by c.customer_id
    """
    pr_sql = base_sql.replace(
        "where o.order_status = 'F'",
        "where o.order_status in ('F', 'O')",
    )
    base = _manifest(_node("int_customer_orders", sql=base_sql))
    pr = _manifest(_node("int_customer_orders", sql=pr_sql))
    comparison = compare_manifests(base, pr, entity_key="customer_id").to_dict()
    modified = comparison["modified"]
    assert len(modified) == 1
    assert modified[0]["changeKinds"] == ["FILTER_CHANGED"]
    assert modified[0]["unsafe"] is False
    assert modified[0]["impactStatus"] == "COMPILED"
    assert "is distinct from" in (modified[0].get("candidateSql") or "").lower()
    assert comparison["narrowFrontierSafe"] is True
    assert comparison["fullRebuildRequired"] is False


def test_empty_base_sql_is_rebuild_not_added_filter() -> None:
    base = _manifest(_node("int_customer_orders", sql=""))
    pr = _manifest(
        _node(
            "int_customer_orders",
            sql="select customer_id from stg_orders where order_status in ('F', 'O')",
        )
    )
    comparison = compare_manifests(base, pr, entity_key="customer_id").to_dict()
    modified = comparison["modified"][0]
    assert modified["changeKinds"] == ["UNSUPPORTED"]
    assert "empty SQL" in (modified.get("unsupportedReasons") or [])
    assert modified["impactStatus"] == "FULL_REBUILD_REQUIRED"
    assert comparison["fullRebuildRequired"] is True
    assert comparison["narrowFrontierSafe"] is False


def test_compiled_sql_pair_prefers_changed_production_model() -> None:
    mart_sql = "select customer_id from int_customer_orders"
    before = "select customer_id from stg_orders where order_status = 'F'"
    after = "select customer_id from stg_orders where order_status in ('F', 'O')"
    base = _manifest(
        _node("customer_summary", sql=mart_sql),
        _node("int_customer_orders", sql=before),
        _node("frontier_customer_orders_target", sql=before),
    )
    pr = _manifest(
        _node("customer_summary", sql=mart_sql),
        _node("int_customer_orders", sql=after),
        _node("frontier_customer_orders_target", sql=after),
    )
    pair = compiled_sql_pair_for_sql_change(
        target_name="customer_summary",
        pr_manifest=pr,
        base_manifest=base,
        sql_comparison={
            "modified": [
                {"name": "frontier_customer_orders_target"},
                {"name": "int_customer_orders"},
            ]
        },
    )
    assert pair == (before, after)


def test_stamp_impact_execution_separates_compiler_from_warehouse() -> None:
    comparison = {
        "modified": [
            {"name": "int_customer_orders", "impactStatus": "COMPILED", "changeKinds": ["FILTER_CHANGED"]},
        ]
    }
    live = stamp_impact_execution(
        comparison,
        run_mode="live",
        full_rebuild_required=False,
        sql_change_executed=True,
    )
    assert live is not None
    assert live["modified"][0]["impactStatus"] == "COMPILED"
    assert live["modified"][0]["impactExecution"] == "EXECUTED"

    fixture = stamp_impact_execution(
        comparison,
        run_mode="fixture",
        full_rebuild_required=False,
        sql_change_executed=True,
    )
    assert fixture is not None
    assert fixture["modified"][0]["impactExecution"] == "NOT_EVALUATED"

    rebuild = stamp_impact_execution(
        {
            "modified": [
                {
                    "name": "int_customer_orders",
                    "impactStatus": "FULL_REBUILD_REQUIRED",
                    "changeKinds": ["FILTER_CHANGED"],
                }
            ]
        },
        run_mode="live",
        full_rebuild_required=True,
        sql_change_executed=False,
    )
    assert rebuild is not None
    assert rebuild["modified"][0]["impactExecution"] == "FAILED"

