from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from frontier.cdc.config import load_cdc_config
from frontier.config import ConfigError
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
