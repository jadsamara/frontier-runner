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
    assert config.sql_change.rebuild_recommended_pct == 75.0


def test_rebuild_recommended_threshold(monkeypatch) -> None:
    from frontier.config import should_recommend_rebuild, sql_change_rebuild_recommended_pct

    assert should_recommend_rebuild(99_621, 150_000, 50.0) is True
    assert should_recommend_rebuild(99_621, 150_000, 75.0) is False
    assert should_recommend_rebuild(112_500, 150_000, 75.0) is True
    config = load_frontier_config(Path(__file__).parent / "fixtures" / "frontier.yml")
    assert sql_change_rebuild_recommended_pct(config) == 75.0
    monkeypatch.setenv("FRONTIER_SQL_CHANGE_REBUILD_PCT", "40")
    assert sql_change_rebuild_recommended_pct(config) == 40.0


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
