from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from frontier.config import ConfigError
from frontier.onboard.constants import CONFIG_DIR_NAME, CONFIG_FILE_NAME, DEFAULT_API_URL


@dataclass(frozen=True)
class LocalFrontierConfig:
    project: str
    api_url: str = DEFAULT_API_URL
    dbt_project_dir: str = "."
    dbt_target: str = "dev"
    git_provider: str = "github"
    default_branch: str = "main"
    version: int = 1
    path: Path | None = None
    target_model: str | None = None
    entity: str | None = None
    entity_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": self.version,
            "project": self.project,
            "api_url": self.api_url,
            "dbt": {
                "project_dir": self.dbt_project_dir,
                "target": self.dbt_target,
            },
            "git": {
                "provider": self.git_provider,
                "default_branch": self.default_branch,
            },
        }
        if self.target_model:
            payload["target"] = {
                "model": self.target_model,
                "entity": self.entity,
                "entity_key": self.entity_key,
            }
        return payload


def config_dir(project_dir: Path) -> Path:
    return project_dir / CONFIG_DIR_NAME


def config_path(project_dir: Path) -> Path:
    return config_dir(project_dir) / CONFIG_FILE_NAME


def load_local_config(project_dir: Path) -> LocalFrontierConfig | None:
    path = config_path(project_dir)
    if not path.is_file():
        return None
    raw = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Invalid {path}")
    dbt = raw.get("dbt") or {}
    git = raw.get("git") or {}
    target = raw.get("target") or {}
    project = str(raw.get("project") or "").strip()
    if not project:
        raise ConfigError(f"{path} is missing project")
    return LocalFrontierConfig(
        project=project,
        api_url=str(raw.get("api_url") or DEFAULT_API_URL).strip() or DEFAULT_API_URL,
        dbt_project_dir=str(dbt.get("project_dir") or ".").strip() or ".",
        dbt_target=str(dbt.get("target") or "dev").strip() or "dev",
        git_provider=str(git.get("provider") or "github").strip() or "github",
        default_branch=str(git.get("default_branch") or "main").strip() or "main",
        version=int(raw.get("version") or 1),
        path=path,
        target_model=str(target.get("model") or "").strip() or None,
        entity=str(target.get("entity") or "").strip() or None,
        entity_key=str(target.get("entity_key") or "").strip() or None,
    )


def write_local_config(project_dir: Path, config: LocalFrontierConfig, *, force: bool = False) -> Path:
    path = config_path(project_dir)
    if path.exists() and not force:
        raise ConfigError(f"{path} already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config.to_dict(), sort_keys=False))
    return path


def persist_target_selection(
    project_dir: Path,
    *,
    model: str,
    entity: str,
    entity_key: str,
) -> Path:
    current = load_local_config(project_dir)
    if current is None:
        return config_path(project_dir)
    updated = LocalFrontierConfig(
        project=current.project,
        api_url=current.api_url,
        dbt_project_dir=current.dbt_project_dir,
        dbt_target=current.dbt_target,
        git_provider=current.git_provider,
        default_branch=current.default_branch,
        version=current.version,
        path=current.path,
        target_model=model,
        entity=entity,
        entity_key=entity_key,
    )
    return write_local_config(project_dir, updated, force=True)
