from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from frontier.cdc.config import load_cdc_config, overlay_cdc_with_manifest
from frontier.config import ConfigError
from frontier.semantic import ManifestError, fingerprint_document, pinned_from_payload
from tests.conftest import FIXTURES


def _write(path: Path, payload: dict) -> Path:
    path.write_text(yaml.safe_dump(payload))
    return path


def test_load_cdc_config_from_fixture() -> None:
    config = load_cdc_config(FIXTURES / "frontier-cdc.yml")
    assert config.provider == "snowflake_stream"
    assert config.control_database == "DATA_AGENT_DEV"
    assert config.control_schema == "FRONTIER_CDC"
    assert [source.source_model for source in config.sources] == ["stg_orders", "stg_customers"]
    assert config.sources[0].stream_name == "ORDERS_STREAM"
    assert config.sources[0].require_before_image_for == ("DELETE", "KEY_CHANGE")


def test_reject_unknown_provider(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "frontier-cdc.yml",
        {"version": 1, "provider": "kafka", "sources": []},
    )
    with pytest.raises(ConfigError, match="Unknown CDC provider"):
        load_cdc_config(path)


def test_reject_malformed_identifier(tmp_path: Path) -> None:
    payload = yaml.safe_load((FIXTURES / "frontier-cdc.yml").read_text())
    payload["sources"][0]["stream_relation"] = "FRONTIER_CDC.ORDERS_STREAM"
    path = _write(tmp_path / "frontier-cdc.yml", payload)
    with pytest.raises(ConfigError, match="database.schema.table"):
        load_cdc_config(path)


def test_reject_duplicate_stream_relations(tmp_path: Path) -> None:
    payload = yaml.safe_load((FIXTURES / "frontier-cdc.yml").read_text())
    payload["sources"][1]["stream_relation"] = payload["sources"][0]["stream_relation"]
    path = _write(tmp_path / "frontier-cdc.yml", payload)
    with pytest.raises(ConfigError, match="duplicate stream"):
        load_cdc_config(path)


def test_reject_missing_primary_key(tmp_path: Path) -> None:
    payload = yaml.safe_load((FIXTURES / "frontier-cdc.yml").read_text())
    payload["sources"][0]["primary_key"] = ""
    path = _write(tmp_path / "frontier-cdc.yml", payload)
    with pytest.raises(ConfigError, match="primary_key"):
        load_cdc_config(path)


def test_reject_credentials(tmp_path: Path) -> None:
    payload = yaml.safe_load((FIXTURES / "frontier-cdc.yml").read_text())
    payload["password"] = "super-secret-password"
    path = _write(tmp_path / "frontier-cdc.yml", payload)
    with pytest.raises(ConfigError, match="credentials"):
        load_cdc_config(path)


def _jaffle_pin():
    document = {
        "model": "customer_summary",
        "entity": "customer",
        "entityKey": "customer_id",
        "grain": "one_row_per_customer",
        "sources": [
            {
                "name": "stg_customers",
                "changeKey": "customer_id",
                "joinRoute": "direct",
                "mutationPolicy": "targeted_repair",
                "deletesRequireBeforeImage": False,
                "temporalMode": "none",
                "eventTimeColumn": None,
                "maximumLateness": None,
                "confidence": "high",
                "origin": "confirmed",
            },
            {
                "name": "stg_orders",
                "changeKey": "order_id",
                "joinRoute": "order_id -> customer_id",
                "mutationPolicy": "targeted_repair",
                "deletesRequireBeforeImage": True,
                "temporalMode": "none",
                "eventTimeColumn": None,
                "maximumLateness": None,
                "confidence": "high",
                "origin": "confirmed",
            },
        ],
    }
    return pinned_from_payload(
        {
            "id": "11111111-1111-4111-8111-111111111111",
            "version": 4,
            "fingerprint": fingerprint_document(document),
            "project": "jaffle_shop",
            "status": "active",
            "activatedAt": "2026-09-05T12:00:00.000Z",
            "targetModelUniqueId": "model.jaffle_shop.customer_summary",
            "targetModel": "customer_summary",
            "targetEntityType": "customer",
            "entityKey": "customer_id",
            "grain": "one_row_per_customer",
            "document": document,
        },
        source="saas_active",
    )


def test_overlay_keeps_stream_columns_and_uses_saas_entity() -> None:
    config = overlay_cdc_with_manifest(load_cdc_config(FIXTURES / "frontier-cdc.yml"), _jaffle_pin())
    orders = config.sources[0]
    customers = config.sources[1]
    assert orders.primary_key == "O_ORDERKEY"
    assert orders.target_key == "O_CUSTKEY"
    assert orders.target_entity == "customer"
    assert orders.require_before_image_for == ("DELETE", "KEY_CHANGE")
    assert customers.primary_key == "C_CUSTKEY"
    assert customers.target_key == "C_CUSTKEY"
    assert customers.require_before_image_for == ()


def test_overlay_fails_when_before_image_policy_conflicts(tmp_path: Path) -> None:
    payload = yaml.safe_load((FIXTURES / "frontier-cdc.yml").read_text())
    payload["sources"][1]["require_before_image_for"] = ["DELETE"]
    path = _write(tmp_path / "frontier-cdc.yml", payload)
    with pytest.raises(ManifestError, match="MANIFEST_LOCAL_REMOTE_CONFLICT"):
        overlay_cdc_with_manifest(load_cdc_config(path), _jaffle_pin())
