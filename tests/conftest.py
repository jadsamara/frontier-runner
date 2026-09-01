from __future__ import annotations

import shutil
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
JAFFLE_SHOP = Path("/Users/jad/Desktop/data_agent_pipeline/jaffle_shop")


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
