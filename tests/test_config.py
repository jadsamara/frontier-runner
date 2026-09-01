from __future__ import annotations

from pathlib import Path

import pytest

from frontier.config import ConfigError, load_frontier_config, redact, write_init_config


def test_load_example_config(dbt_project: Path) -> None:
    config = load_frontier_config(dbt_project / "frontier.yml")
    assert config.project == "jaffle_shop"
    assert config.model.name == "customer_summary"
    assert config.relations["stg_customers"].route.kind == "direct"
    assert config.relations["stg_orders"].route.kind == "query"
    assert "ref('stg_orders')" in (config.relations["stg_orders"].route.query or "")
    assert config.upload.include_entity_ids is False
    assert config.proof.after_mart == "customer_summary_after"
    assert config.proof.repaired_mart == "customer_summary_repaired"


def test_init_writes_example(tmp_path: Path) -> None:
    path = tmp_path / "frontier.yml"
    write_init_config(path)
    config = load_frontier_config(path)
    assert config.model.key == "customer_id"
    with pytest.raises(ConfigError, match="already exists"):
        write_init_config(path)


def test_redact_strips_passwords() -> None:
    redacted = redact({"account": "acme", "password": "super-secret-password", "nested": {"token": "abc"}})
    assert redacted["password"] == "********"
    assert redacted["nested"]["token"] == "********"
    assert redacted["account"] == "acme"
