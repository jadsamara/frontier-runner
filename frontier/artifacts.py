from __future__ import annotations

import os
from pathlib import Path

from frontier.config import ConfigError
from frontier.github import env_flag

ARTIFACT_SHA_FILE = "frontier-artifact-sha"


def artifact_sha_path(target_dir: Path) -> Path:
    return target_dir / ARTIFACT_SHA_FILE


def write_artifact_sha(target_dir: Path, sha: str) -> Path:
    cleaned = sha.strip()
    if not cleaned:
        raise ConfigError("Cannot bind dbt artifacts to an empty commit SHA")
    path = artifact_sha_path(target_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(cleaned + "\n")
    return path


def require_current_artifacts(target_dir: Path) -> str | None:
    """Refuse inspect/run/prove when CI artifacts are not bound to this commit.

    Local runs (no GITHUB_ACTIONS) skip the check so developers can use an
    existing target/ directory. GitHub Actions always requires a stamp written
    after a successful dbt build for this GITHUB_SHA.
    """
    enforce = env_flag("GITHUB_ACTIONS") or env_flag("FRONTIER_REQUIRE_ARTIFACT_SHA")
    if not enforce:
        return None
    expected = (os.environ.get("GITHUB_SHA") or "").strip()
    if not expected:
        raise ConfigError("GITHUB_SHA is required to bind dbt artifacts in CI")
    path = artifact_sha_path(target_dir)
    if not path.is_file():
        raise ConfigError(
            f"Missing {path.name}; refusing dbt artifacts that are not bound to {expected}",
        )
    recorded = path.read_text().strip()
    if recorded != expected:
        raise ConfigError(
            f"dbt artifacts were generated for {recorded}, not {expected}; refusing stale evidence",
        )
    return recorded
