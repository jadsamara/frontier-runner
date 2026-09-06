from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from frontier.onboard.constants import PRODUCTION_SCHEMA_HINTS


@dataclass(frozen=True)
class ProjectDetection:
    project_dir: Path
    dbt_project_yml: Path | None
    dbt_project_name: str | None
    profile_name: str | None
    profiles_yml: Path | None
    targets: tuple[str, ...]
    adapter_type: str | None
    git_root: Path | None
    github_origin: str | None
    default_branch: str | None
    manifest_path: Path | None
    dbt_installed: bool
    gh_installed: bool
    missing: tuple[str, ...] = field(default_factory=tuple)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    loaded = yaml.safe_load(path.read_text()) or {}
    return loaded if isinstance(loaded, dict) else {}


def _which(name: str) -> bool:
    return shutil.which(name) is not None


def _git(cwd: Path, *args: str) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _github_origin(remote: str | None) -> str | None:
    if not remote:
        return None
    value = remote.strip()
    if "github.com" not in value.lower():
        return None
    if value.endswith(".git"):
        value = value[:-4]
    if value.startswith("git@github.com:"):
        return "https://github.com/" + value.split(":", 1)[1]
    return value


def detect_project(
    project_dir: Path,
    *,
    profiles_path: Path | None = None,
) -> ProjectDetection:
    root = project_dir.resolve()
    dbt_yml = root / "dbt_project.yml"
    dbt_raw = _read_yaml(dbt_yml) if dbt_yml.is_file() else {}
    dbt_name = str(dbt_raw.get("name") or "").strip() or None
    profile_name = str(dbt_raw.get("profile") or dbt_name or "").strip() or None
    profiles_path = profiles_path or (Path.home() / ".dbt" / "profiles.yml")
    profiles = _read_yaml(profiles_path)
    profile = profiles.get(profile_name or "") if profile_name else None
    targets: list[str] = []
    adapter = None
    if isinstance(profile, dict):
        outputs = profile.get("outputs") or {}
        if isinstance(outputs, dict):
            targets = sorted(str(name) for name in outputs)
            first = next(iter(outputs.values()), None)
            if isinstance(first, dict):
                adapter = str(first.get("type") or "").strip() or None
    git_root_text = _git(root, "rev-parse", "--show-toplevel")
    git_root = Path(git_root_text) if git_root_text else None
    origin = _github_origin(_git(root, "remote", "get-url", "origin"))
    branch = _git(root, "symbolic-ref", "--short", "refs/remotes/origin/HEAD")
    if branch and branch.startswith("origin/"):
        branch = branch.split("/", 1)[1]
    if not branch:
        branch = _git(root, "rev-parse", "--abbrev-ref", "HEAD")
    manifest = root / "target" / "manifest.json"
    missing: list[str] = []
    if not dbt_yml.is_file():
        missing.append("dbt_project.yml")
    if not git_root:
        missing.append("git repository")
    if not origin:
        missing.append("GitHub origin")
    return ProjectDetection(
        project_dir=root,
        dbt_project_yml=dbt_yml if dbt_yml.is_file() else None,
        dbt_project_name=dbt_name,
        profile_name=profile_name,
        profiles_yml=profiles_path if profiles_path.is_file() else None,
        targets=tuple(targets),
        adapter_type=adapter,
        git_root=git_root,
        github_origin=origin,
        default_branch=branch,
        manifest_path=manifest if manifest.is_file() else None,
        dbt_installed=_which("dbt"),
        gh_installed=_which("gh"),
        missing=tuple(missing),
    )


def schema_looks_like_production(schema: str | None) -> bool:
    value = (schema or "").strip().lower()
    return any(hint in value for hint in PRODUCTION_SCHEMA_HINTS)
