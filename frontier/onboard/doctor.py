from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from frontier import __version__
from frontier.config import redact
from frontier.credentials import load_credentials
from frontier.errors import InstallError
from frontier.local_config import load_local_config
from frontier.onboard.constants import DEFAULT_API_URL, SUPPORTED_PYTHON
from frontier.onboard.detect import ProjectDetection, detect_project, schema_looks_like_production
from frontier.onboard.saas import (
    fetch_active_manifest_summary,
    fetch_runner_versions,
    saas_reachable,
    whoami,
)
from frontier.onboard.versions import version_at_least
from frontier.warehouse import connect_warehouse, load_dbt_profile_output

ConnectFn = Callable[..., Any]


@dataclass
class DoctorCheck:
    id: str
    label: str
    ok: bool
    required: bool
    detail: str = ""
    next_action: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def python_supported(version: tuple[int, int] | None = None) -> bool:
    current = version or sys.version_info[:2]
    return any(current[:2] == supported[:2] for supported in SUPPORTED_PYTHON)


def _workflow_path(detection: ProjectDetection) -> Path | None:
    root = detection.git_root or detection.project_dir
    path = root / ".github" / "workflows" / "frontier.yml"
    return path if path.is_file() else None


def _workflow_runner_version(text: str) -> str | None:
    import re

    match = re.search(r"frontier-runner(?:\[snowflake\])?==([0-9]+\.[0-9]+\.[0-9]+)", text)
    if match:
        return match.group(1)
    match = re.search(r"frontier_runner-([0-9]+\.[0-9]+\.[0-9]+)-py3-none-any", text)
    if match:
        return match.group(1)
    return None


def run_doctor(
    project_dir: Path,
    *,
    connect: ConnectFn = connect_warehouse,
    skip_warehouse: bool = False,
) -> list[DoctorCheck]:
    checks: list[DoctorCheck] = []
    detection = detect_project(project_dir)
    local = load_local_config(project_dir)
    creds = load_credentials()
    api_url = (local.api_url if local else None) or (creds.api_url if creds else DEFAULT_API_URL)

    checks.append(
        DoctorCheck(
            id="runner_version",
            label="Runner version",
            ok=True,
            required=True,
            detail=__version__,
        )
    )
    py_ok = python_supported()
    checks.append(
        DoctorCheck(
            id="python",
            label="Supported Python",
            ok=py_ok,
            required=True,
            detail=f"{sys.version_info.major}.{sys.version_info.minor}",
            next_action=None if py_ok else "Install Python 3.11 or newer.",
        )
    )
    checks.append(
        DoctorCheck(
            id="dbt_installed",
            label="dbt installed",
            ok=detection.dbt_installed,
            required=False,
            detail="dbt" if detection.dbt_installed else "not found on PATH",
            next_action=None if detection.dbt_installed else "Install dbt Core or dbt Fusion.",
        )
    )
    dbt_ok = detection.dbt_project_yml is not None
    checks.append(
        DoctorCheck(
            id="dbt_project",
            label="dbt project",
            ok=dbt_ok,
            required=True,
            detail=detection.dbt_project_name or "dbt_project.yml missing",
            next_action=None if dbt_ok else "Run `frontier init` from the dbt project root.",
        )
    )
    manifest_ok = detection.manifest_path is not None
    checks.append(
        DoctorCheck(
            id="manifest",
            label="dbt compile artifacts",
            ok=manifest_ok,
            required=False,
            detail=str(detection.manifest_path) if manifest_ok else "target/manifest.json missing",
            next_action=None if manifest_ok else "Run `dbt compile`, then retry.",
        )
    )
    git_ok = detection.git_root is not None
    checks.append(
        DoctorCheck(
            id="git",
            label="Git repository",
            ok=git_ok,
            required=True,
            next_action=None if git_ok else "Initialize a Git repository in this project.",
        )
    )
    github_ok = detection.github_origin is not None
    checks.append(
        DoctorCheck(
            id="github_remote",
            label="GitHub remote",
            ok=github_ok,
            required=True,
            detail=detection.github_origin or "origin is not GitHub",
            next_action=None if github_ok else "Set `origin` to a GitHub repository.",
        )
    )

    reachable = saas_reachable(api_url)
    checks.append(
        DoctorCheck(
            id="saas",
            label="Frontier SaaS reachable",
            ok=reachable,
            required=True,
            detail=api_url,
            next_action=None if reachable else "Confirm api_url in `.frontier/config.yml`.",
        )
    )

    auth_ok = False
    identity = None
    if creds:
        try:
            identity = whoami(creds.api_url, creds.api_key)
            auth_ok = True
        except InstallError:
            auth_ok = False
    checks.append(
        DoctorCheck(
            id="auth",
            label="Frontier authentication",
            ok=auth_ok,
            required=True,
            detail=identity.api_key_prefix if identity else "not authenticated",
            next_action=None if auth_ok else "Run `frontier login --api-key`.",
        )
    )

    active = None
    if creds and auth_ok:
        try:
            active = fetch_active_manifest_summary(creds)
        except InstallError:
            active = None
    active_ok = active is not None
    checks.append(
        DoctorCheck(
            id="active_manifest",
            label="Active semantic manifest",
            ok=active_ok,
            required=True,
            detail=(
                f"version {active.get('version')} · {active.get('targetModel')}"
                if active
                else "none"
            ),
            next_action=None
            if active_ok
            else "Review and activate the draft in Frontier.",
        )
    )
    project_name = detection.dbt_project_name or (local.project if local else "")
    matches = True
    if active and project_name:
        matches = str(active.get("project") or "") == project_name
    checks.append(
        DoctorCheck(
            id="manifest_project",
            label="Active manifest matches dbt project",
            ok=bool(not active or matches),
            required=True,
            next_action=None if matches else "Activate a manifest for this dbt project.",
        )
    )

    snowflake_profile = detection.adapter_type == "snowflake" if detection.adapter_type else False
    if detection.adapter_type is None and detection.profiles_yml is None:
        snowflake_profile = False
    elif detection.adapter_type is None and detection.profile_name:
        snowflake_profile = False
    else:
        snowflake_profile = detection.adapter_type in {None, "snowflake"} and bool(detection.targets)
    checks.append(
        DoctorCheck(
            id="snowflake_profile",
            label="Snowflake profile",
            ok=bool(detection.adapter_type == "snowflake"),
            required=True,
            detail=detection.adapter_type or "no profile",
            next_action=None
            if detection.adapter_type == "snowflake"
            else "Add a Snowflake output to profiles.yml.",
        )
    )

    connected = False
    schema = None
    if not skip_warehouse and detection.adapter_type == "snowflake":
        try:
            output = load_dbt_profile_output(project_dir, target=local.dbt_target if local else None)
            schema = str(output.get("schema") or "")
            warehouse = connect(project_dir, target=local.dbt_target if local else None)
            try:
                if hasattr(warehouse, "scalar"):
                    warehouse.scalar("select 1")
                connected = True
            finally:
                close = getattr(warehouse, "close", None)
                if callable(close):
                    close()
        except Exception as error:
            connected = False
            schema = schema or str(error)
    elif skip_warehouse:
        connected = detection.adapter_type == "snowflake"
    checks.append(
        DoctorCheck(
            id="snowflake_connection",
            label="Snowflake connection",
            ok=connected,
            required=True,
            next_action=None if connected else "Confirm Snowflake credentials in profiles.yml.",
        )
    )
    prod = schema_looks_like_production(schema)
    checks.append(
        DoctorCheck(
            id="non_prod_schema",
            label="Target schema is not production",
            ok=not prod,
            required=True,
            detail=schema or "",
            next_action=None
            if not prod
            else "Point the PR-assessment role at a non-production schema.",
        )
    )

    workflow = _workflow_path(detection)
    checks.append(
        DoctorCheck(
            id="github_workflow",
            label="GitHub workflow",
            ok=workflow is not None,
            required=True,
            detail=str(workflow) if workflow else "not installed",
            next_action=None if workflow else "Run `frontier setup github`.",
        )
    )
    workflow_supported = True
    if workflow is not None:
        text = workflow.read_text()
        pinned = _workflow_runner_version(text)
        try:
            versions = fetch_runner_versions(api_url)
            if pinned:
                workflow_supported = version_at_least(pinned, versions.minimum_supported)
        except InstallError:
            workflow_supported = pinned is not None
        if pinned is None:
            workflow_supported = False
    checks.append(
        DoctorCheck(
            id="workflow_runner_version",
            label="Runner version in workflow is supported",
            ok=workflow is None or workflow_supported,
            required=True,
            next_action=None
            if workflow is None or workflow_supported
            else "Regenerate `.github/workflows/frontier.yml` with `frontier setup github`.",
        )
    )
    return checks


def format_doctor(checks: list[DoctorCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        mark = "✓" if check.ok else "✗"
        extra = f" ({check.detail})" if check.detail else ""
        lines.append(f"{mark} {check.label}{extra}")
    failed = [check for check in checks if not check.ok]
    if failed:
        action = next((check.next_action for check in failed if check.next_action), None)
        if action:
            lines.extend(["", "Next action:", action])
    return "\n".join(lines)


def doctor_failed(checks: list[DoctorCheck]) -> bool:
    return any(not check.ok and check.required for check in checks)


def doctor_json(checks: list[DoctorCheck]) -> dict[str, Any]:
    return redact(
        {
            "runnerVersion": __version__,
            "ok": not doctor_failed(checks),
            "checks": [check.to_dict() for check in checks],
        }
    )
