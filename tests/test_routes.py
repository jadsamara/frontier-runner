from __future__ import annotations

from dataclasses import replace

from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.onboard.routes import (
    RouteHop,
    cdc_unresolved_for_batch,
    compile_route_query,
    derive_source_route,
    derive_target,
    impact_returns_entity_key,
    merge_human_overrides,
    readiness,
    select_change_key,
    sql_change_required_sources,
)


def _model(
    name: str,
    *,
    columns: tuple[str, ...],
    depends: tuple[str, ...] = (),
    compiled: str = "",
    resource_type: str = "model",
) -> DbtNode:
    unique_id = f"{resource_type}.jaffle.{name}"
    return DbtNode(
        unique_id=unique_id,
        name=name,
        resource_type=resource_type,
        database="DB",
        schema="SC",
        relation_name=f"DB.SC.{name}",
        depends_on=depends,
        compiled_code=compiled or f"select {', '.join(columns)} from {name}",
        package_name="jaffle",
        columns=columns,
        original_file_path=f"models/{name}.sql",
    )


def _unique_test(model: str, column: str) -> DbtNode:
    name = f"unique_{model}_{column}"
    return DbtNode(
        unique_id=f"test.jaffle.{name}",
        name=name,
        resource_type="test",
        database="DB",
        schema="SC",
        relation_name=f"DB.SC.{name}",
        depends_on=(f"model.jaffle.{model}",),
        package_name="jaffle",
        columns=(),
    )


def starter_manifest() -> Manifest:
    nodes = {
        node.unique_id: node
        for node in (
            _model(
                "stg_customers",
                columns=("customer_id", "name"),
            ),
            _model(
                "stg_orders",
                columns=("order_id", "customer_id", "order_date"),
                depends=("model.jaffle.stg_customers",),
            ),
            _model(
                "stg_order_items",
                columns=("order_item_id", "order_id", "product_id"),
                depends=("model.jaffle.stg_orders",),
            ),
            _model(
                "stg_products",
                columns=("product_id", "product_name"),
            ),
            _model(
                "stg_supplies",
                columns=("supply_id", "product_id", "cost"),
            ),
            _model(
                "order_items",
                columns=("order_item_id", "order_id", "product_id"),
                depends=(
                    "model.jaffle.stg_order_items",
                    "model.jaffle.stg_products",
                    "model.jaffle.stg_supplies",
                ),
            ),
            _model(
                "orders",
                columns=("order_id", "customer_id", "order_date"),
                depends=("model.jaffle.stg_orders", "model.jaffle.order_items"),
            ),
            _model(
                "customers",
                columns=("customer_id", "customer_name"),
                depends=("model.jaffle.stg_customers", "model.jaffle.orders"),
                compiled="select customer_id, customer_name from orders",
            ),
            _unique_test("stg_customers", "customer_id"),
            _unique_test("stg_orders", "order_id"),
            _unique_test("stg_order_items", "order_item_id"),
            _unique_test("stg_products", "product_id"),
            _unique_test("stg_supplies", "supply_id"),
            _unique_test("customers", "customer_id"),
        )
    }
    return Manifest(project_name="jaffle", adapter_type="snowflake", nodes=nodes, sources={})


def test_direct_route_requires_entity_key_column() -> None:
    manifest = starter_manifest()
    customers = manifest.find_model("customers")
    stg_customers = manifest.find_model("stg_customers")
    route = derive_source_route(manifest, stg_customers, customers, entity_key="customer_id")
    assert route.join_route == "direct"
    assert route.route_status == "VERIFIED"
    assert route.change_key == "customer_id"


def test_supplies_customer_id_direct_is_rejected() -> None:
    manifest = starter_manifest()
    customers = manifest.find_model("customers")
    supplies = manifest.find_model("stg_supplies")
    assert "customer_id" not in supplies.columns
    route = derive_source_route(manifest, supplies, customers, entity_key="customer_id")
    assert route.join_route != "direct"
    assert route.change_key != "customer_id"
    assert route.change_key == "supply_id"


def test_supply_path_is_derived() -> None:
    manifest = starter_manifest()
    route = derive_source_route(
        manifest,
        manifest.find_model("stg_supplies"),
        manifest.find_model("customers"),
        entity_key="customer_id",
    )
    columns = [hop.column for hop in route.route_path]
    assert columns[0] == "supply_id"
    assert "product_id" in columns
    assert "order_id" in columns
    assert columns[-1] == "customer_id"
    assert route.route_status == "VERIFIED"
    sql = compile_route_query(
        route.route_path,
        change_key="supply_id",
        entity_key="customer_id",
    )
    assert "changed_values" in sql
    assert "stg_supplies" in sql
    assert "select distinct" in sql.lower()


def test_order_item_path_is_derived() -> None:
    manifest = starter_manifest()
    route = derive_source_route(
        manifest,
        manifest.find_model("stg_order_items"),
        manifest.find_model("customers"),
        entity_key="customer_id",
    )
    columns = [hop.column for hop in route.route_path]
    assert columns[0] == "order_item_id"
    assert "order_id" in columns
    assert columns[-1] == "customer_id"
    assert route.change_key == "order_item_id"
    assert route.route_status == "VERIFIED"


def test_uniqueness_tests_select_change_key() -> None:
    manifest = starter_manifest()
    key, confidence, evidence = select_change_key(
        manifest,
        manifest.find_model("stg_supplies"),
        entity_key="customer_id",
    )
    assert key == "supply_id"
    assert confidence == "high"
    assert any("uniqueness" in item for item in evidence)


def test_missing_key_is_not_auto_verified() -> None:
    node = _model("stg_mystery", columns=("note", "amount"))
    target = _model("customers", columns=("customer_id",), depends=("model.jaffle.stg_mystery",))
    mystery_test_free = Manifest(
        project_name="jaffle",
        adapter_type="snowflake",
        nodes={node.unique_id: node, target.unique_id: target},
        sources={},
    )
    key, _confidence, _evidence = select_change_key(
        mystery_test_free,
        node,
        entity_key="customer_id",
    )
    assert key != "customer_id"
    route = derive_source_route(mystery_test_free, node, target, entity_key="customer_id")
    assert route.route_status in {"UNRESOLVED", "INVALID"}
    assert route.origin == "derived"


def test_fan_out_query_uses_distinct() -> None:
    sql = compile_route_query(
        (
            RouteHop("stg_supplies", "supply_id"),
            RouteHop("stg_supplies", "product_id"),
            RouteHop("stg_order_items", "product_id"),
            RouteHop("stg_order_items", "order_id"),
            RouteHop("stg_orders", "order_id"),
            RouteHop("stg_orders", "customer_id"),
            RouteHop("customers", "customer_id"),
        ),
        change_key="supply_id",
        entity_key="customer_id",
    )
    assert "select distinct" in sql.lower()


def test_unrelated_route_does_not_block_sql_when_impact_returns_entity_key() -> None:
    sql = "select distinct orders.customer_id from orders where order_date >= '2016-09-02'"
    assert impact_returns_entity_key(sql, "customer_id") is True
    suggestion = derive_target(starter_manifest(), starter_manifest().find_model("customers"))
    supplies = next(item for item in suggestion.sources if item.name == "stg_supplies")
    assert supplies.sql_change_blocker is False


def test_generated_document_is_sql_ready_without_human_confirmation() -> None:
    suggestion = derive_target(starter_manifest(), starter_manifest().find_model("customers"))
    document = suggestion.to_semantic_document()
    assert document["generationKind"] == "generated"
    assert all(source["origin"] == "derived" for source in document["sources"])
    assert any(source["origin"] != "confirmed" for source in document["sources"])


def test_unresolved_irrelevant_route_does_not_block_sql_proof() -> None:
    suggestion = derive_target(starter_manifest(), starter_manifest().find_model("customers"))
    unresolved = [
        source
        for source in suggestion.sources
        if source.route_status != "VERIFIED"
    ]
    required = sql_change_required_sources(
        suggestion.sources,
        impact_sql="select distinct orders.customer_id from orders where (true) is distinct from (order_date >= '2016-09-02')",
        entity_key="customer_id",
        changed_models=("orders",),
    )
    assert required == ()
    if unresolved:
        assert "stg_supplies" not in required


def test_unresolved_relevant_route_fails_closed() -> None:
    sources = derive_target(starter_manifest(), starter_manifest().find_model("customers")).sources
    mystery = replace(
        sources[0],
        name="stg_mystery",
        route_status="UNRESOLVED",
        sql_change_blocker=False,
        cdc_blocker=True,
    )
    required = sql_change_required_sources(
        (*sources, mystery),
        impact_sql="select 1 as flag from stg_mystery",
        entity_key="customer_id",
        changed_models=("stg_mystery",),
    )
    assert "stg_mystery" in required


def test_supplies_route_does_not_block_orders_cdc_batch() -> None:
    suggestion = derive_target(starter_manifest(), starter_manifest().find_model("customers"))
    blocked = cdc_unresolved_for_batch(suggestion.sources, ("stg_orders",))
    assert "stg_supplies" not in blocked


def test_invalid_human_override_is_discarded_not_kept_as_runtime() -> None:
    manifest = starter_manifest()
    generated = derive_target(manifest, manifest.find_model("customers"))
    existing = {
        "sources": [
            {
                "name": "stg_supplies",
                "changeKey": "customer_id",
                "joinRoute": "direct",
                "origin": "confirmed",
                "confidence": "high",
                "mutationPolicy": "targeted_repair",
                "deletesRequireBeforeImage": True,
                "temporalMode": "none",
            },
            {
                "name": "stg_order_items",
                "changeKey": "order_id",
                "joinRoute": "order_id -> customer_id",
                "origin": "confirmed",
                "confidence": "high",
                "mutationPolicy": "targeted_repair",
                "deletesRequireBeforeImage": True,
                "temporalMode": "none",
            },
        ]
    }
    merged = merge_human_overrides(
        generated,
        existing,
        manifest,
        manifest.find_model("customers"),
    )
    supplies = next(item for item in merged.sources if item.name == "stg_supplies")
    assert supplies.origin == "derived"
    assert supplies.change_key == "supply_id"
    assert supplies.join_route != "direct"
    assert supplies.route_status == "VERIFIED"
    items = next(item for item in merged.sources if item.name == "stg_order_items")
    assert items.origin == "confirmed"
    assert items.change_key == "order_id"
    assert items.route_path == ()
    document = merged.to_semantic_document()
    items_doc = next(item for item in document["sources"] if item["name"] == "stg_order_items")
    assert "routePath" not in items_doc
    discarded = next(item for item in merged.discarded_overrides if item.name == "stg_supplies")
    assert discarded.change_key == "customer_id"
    assert discarded.route_status == "INVALID"
    assert any("does not exist" in item for item in discarded.evidence)
    sql_ready, cdc_ready, _verified, _unresolved, invalid = readiness(merged)
    assert sql_ready is True
    assert invalid == 0
    assert discarded.name == "stg_supplies"
    required = sql_change_required_sources(
        merged.sources,
        impact_sql=(
            "select distinct orders.customer_id from orders "
            "where (true) is distinct from (order_date >= '2016-09-02')"
        ),
        entity_key="customer_id",
        changed_models=("orders",),
    )
    assert required == ()
    assert "stg_supplies" not in required
    # CDC from supplies can stay unready if that generated route is unresolved,
    # but this SQL filter must still execute.
    del cdc_ready


def test_catalog_columns_are_unioned_with_compiled_sql() -> None:
    node = _model(
        "stg_supplies",
        columns=("supply_uuid",),
        compiled="select supply_uuid, supply_id, product_id from supplies",
    )
    from frontier.onboard.routes import _columns

    columns = _columns(node)
    assert "supply_uuid" in columns
    assert "product_id" in columns
    assert "supply_id" in columns


def test_sql_aliases_are_used_when_yaml_catalog_is_incomplete() -> None:
    node = _model(
        "stg_supplies",
        columns=("supply_uuid",),
        compiled=(
            "with renamed as ("
            " select md5(id) as supply_uuid, id as supply_id, sku as product_id from supplies"
            ") select * from renamed"
        ),
    )
    from frontier.onboard.routes import _columns

    columns = _columns(node)
    assert "supply_uuid" in columns
    assert "supply_id" in columns
    assert "product_id" in columns


def test_yaml_only_supply_catalog_still_verifies_join_via_aliases() -> None:
    manifest = starter_manifest()
    supplies = manifest.find_model("stg_supplies")
    skinny = replace(
        supplies,
        columns=("supply_uuid",),
        compiled_code=(
            "with renamed as ("
            " select md5(concat(id, sku)) as supply_uuid, id as supply_id, sku as product_id"
            " from supplies"
            ") select * from renamed"
        ),
    )
    items = manifest.find_model("stg_order_items")
    skinny_items = replace(
        items,
        columns=("order_item_id", "order_id"),
        compiled_code="select id as order_item_id, order_id, sku as product_id from items",
    )
    nodes = dict(manifest.nodes)
    nodes[skinny.unique_id] = skinny
    nodes[skinny_items.unique_id] = skinny_items
    nodes.pop("test.jaffle.unique_stg_supplies_supply_id", None)
    nodes["test.jaffle.unique_stg_supplies_supply_uuid"] = _unique_test(
        "stg_supplies", "supply_uuid"
    )
    skinny_manifest = Manifest(
        project_name=manifest.project_name,
        adapter_type=manifest.adapter_type,
        nodes=nodes,
        sources=manifest.sources,
    )
    route = derive_source_route(
        skinny_manifest,
        skinny,
        skinny_manifest.find_model("customers"),
        entity_key="customer_id",
    )
    assert route.change_key == "supply_uuid"
    assert route.route_status == "VERIFIED"
    assert "product_id" in [hop.column for hop in route.route_path]
