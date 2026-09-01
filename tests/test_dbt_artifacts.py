from __future__ import annotations

import pytest

from frontier.dbt_artifacts import inspect_report, load_manifest
from tests.conftest import JAFFLE_SHOP, FIXTURES


def test_fixture_manifest_recognizes_customer_summary() -> None:
    manifest = load_manifest(FIXTURES / "manifest.json")
    report = inspect_report(manifest, "customer_summary")
    names = {node["name"] for node in report["upstreamModels"]}
    assert report["model"]["uniqueId"] == "model.jaffle_shop.customer_summary"
    assert names == {"int_customer_orders", "stg_customers", "stg_orders"}
    source_names = {source["name"] for source in report["sources"]}
    assert "CUSTOMER" in source_names
    assert "ORDERS" in source_names
    assert report["model"]["database"] == "DATA_AGENT_DEV"
    assert report["model"]["schema"] == "DBT_DEV"


@pytest.mark.skipif(
    not (JAFFLE_SHOP / "target" / "manifest.json").is_file(),
    reason="jaffle_shop prototype manifest is not present",
)
def test_real_jaffle_shop_manifest() -> None:
    manifest = load_manifest(JAFFLE_SHOP / "target" / "manifest.json")
    report = inspect_report(manifest, "customer_summary")
    names = {node["name"] for node in report["upstreamModels"]}
    assert "stg_customers" in names
    assert "stg_orders" in names
    assert manifest.find_model("customer_summary").unique_id == "model.jaffle_shop.customer_summary"
    assert report["adapter"] == "snowflake"
