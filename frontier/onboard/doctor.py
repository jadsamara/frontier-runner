from __future__ import annotations

import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from frontier import __version__
from frontier.config import redact
from frontier.credentials import try_resolve_api_credential
from frontier.errors import InstallError
from frontier.local_config import load_local_config
from frontier.semantic import default_pin_path
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
    skipped: bool = False

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
    creds = try_resolve_api_credential()
    api_url = (
        (local.api_url if local else None)
        or (creds.api_url if creds else None)
        or DEFAULT_API_URL
    )

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
    initialized = local is not None or (project_dir / "frontier.yml").is_file()
    checks.append(
        DoctorCheck(
            id="local_config",
            label="Frontier local config",
            ok=initialized,
            required=True,
            detail=str(local.path) if local and local.path else (
                "frontier.yml" if (project_dir / "frontier.yml").is_file() else "missing"
            ),
            next_action=None if initialized else "Run `frontier init`.",
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
            else "Run `frontier discover`, then review and activate the draft in Frontier.",
        )
    )
    pin_path = default_pin_path(project_dir)
    pinned_ok = pin_path.is_file()
    checks.append(
        DoctorCheck(
            id="pinned_manifest",
            label="Pinned semantic manifest",
            ok=pinned_ok,
            required=active_ok,
            skipped=not active_ok,
            detail=str(pin_path) if pinned_ok else "target/frontier-manifest.json missing",
            next_action=None if pinned_ok or not active_ok else "Run `frontier manifest fetch`.",
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
            ok=bool(active_ok and matches),
            required=active_ok,
            skipped=not active_ok,
            detail="no active manifest" if not active_ok else (
                "" if matches else f"{active.get('project')} != {project_name}"
            ),
            next_action=None if not active_ok or matches else "Activate a manifest for this dbt project.",
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


NEXT_ACTION_PRIORITY = (
    "auth",
    "local_config",
    "active_manifest",
    "pinned_manifest",
    "git",
    "github_workflow",
)


def doctor_next_action(checks: list[DoctorCheck]) -> str | None:
    by_id = {check.id: check for check in checks}
    for check_id in NEXT_ACTION_PRIORITY:
        check = by_id.get(check_id)
        if (
            check
            and not check.skipped
            and not check.ok
            and check.next_action
        ):
            return check.next_action
    for check in checks:
        if not check.skipped and not check.ok and check.next_action:
            return check.next_action
    return None


def format_doctor(checks: list[DoctorCheck]) -> str:
    lines: list[str] = []
    for check in checks:
        if check.skipped:
            mark = "–"
        elif check.ok:
            mark = "✓"
        else:
            mark = "✗"
        extra = f" ({check.detail})" if check.detail else ""
        lines.append(f"{mark} {check.label}{extra}")
    action = doctor_next_action(checks)
    if action:
        lines.extend(["", "Next action:", action])
    return "\n".join(lines)


def doctor_failed(checks: list[DoctorCheck]) -> bool:
    return any(not check.ok and check.required and not check.skipped for check in checks)


def doctor_json(checks: list[DoctorCheck]) -> dict[str, Any]:
    return redact(
        {
            "runnerVersion": __version__,
            "ok": not doctor_failed(checks),
            "checks": [check.to_dict() for check in checks],
        }
    )
