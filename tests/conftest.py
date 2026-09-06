from __future__ import annotations

import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
JAFFLE_SHOP = Path("/Users/jad/Desktop/data_agent_pipeline/jaffle_shop")


@pytest.fixture(autouse=True)
def isolate_frontier_credentials(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Never read or write the developer OS keychain or ~/.config/frontier credentials."""
    monkeypatch.setenv("FRONTIER_ALLOW_LOCAL_MANIFEST", "1")
    monkeypatch.setenv("FRONTIER_HIDE_UPDATE_NOTICE", "1")
    monkeypatch.setenv("FRONTIER_KEYRING_FILE", str(tmp_path / "frontier-keyring"))
    monkeypatch.setenv("FRONTIER_CREDENTIALS_FILE", str(tmp_path / "frontier-credentials"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg-config"))
    monkeypatch.setenv("PYTHON_KEYRING_BACKEND", "keyring.backends.fail.Keyring")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("FRONTIER_API_KEY", raising=False)
    monkeypatch.delenv("FRONTIER_DEMO_API_KEY", raising=False)
    monkeypatch.delenv("FRONTIER_API_URL", raising=False)


def copy_dbt_project(tmp_path: Path) -> Path:
    project = tmp_path / "jaffle_shop"
    (project / "target").mkdir(parents=True)
    (project / "seeds").mkdir()
    shutil.copy(FIXTURES / "frontier.yml", project / "frontier.yml")
    shutil.copy(FIXTURES / "dbt_project.yml", project / "dbt_project.yml")
    shutil.copy(FIXTURES / "manifest.json", project / "target" / "manifest.json")
    shutil.copy(FIXTURES / "run_results.json", project / "target" / "run_results.json")
    shutil.copy(FIXTURES / "change_events.csv", project / "seeds" / "change_events.csv")
    return project


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    return copy_dbt_project(tmp_path)
