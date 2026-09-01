from __future__ import annotations

import os
import re
from typing import Any

_PR_REF = re.compile(r"refs/pull/(\d+)/(?:merge|head)")
_TRUTHY = {"1", "true", "yes", "on"}


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def pull_request_number() -> int | None:
    raw = os.environ.get("FRONTIER_PULL_REQUEST") or os.environ.get("GITHUB_PR_NUMBER") or ""
    stripped = raw.strip()
    if stripped.isdigit():
        return int(stripped)
    match = _PR_REF.fullmatch((os.environ.get("GITHUB_REF") or "").strip())
    if match:
        return int(match.group(1))
    return None


def github_source() -> dict[str, Any] | None:
    """GitHub Actions context for the ingest payload. Never includes tokens or secrets."""
    sha = (os.environ.get("GITHUB_SHA") or "").strip()
    repository = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    if not sha or not repository:
        return None
    branch = (os.environ.get("GITHUB_HEAD_REF") or os.environ.get("GITHUB_REF_NAME") or "").strip()
    if not branch:
        return None
    source: dict[str, Any] = {
        "repository": repository,
        "branch": branch,
        "commitSha": sha,
    }
    number = pull_request_number()
    if number is not None:
        source["pullRequestNumber"] = number
    return source


def default_external_run_id(project: str) -> str:
    """Prefer `{project}-{GITHUB_SHA}` so a new commit is a new run and a re-run is idempotent."""
    sha = (os.environ.get("GITHUB_SHA") or "").strip()
    if sha:
        return f"{project}-{sha}"
    from datetime import datetime, timezone

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{project}-{stamp}"
