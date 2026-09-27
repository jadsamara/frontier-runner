from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from frontier.config import ConfigError
from frontier.credentials import (
    AUTH_REQUIRED_MESSAGE,
    SOURCE_ENV,
    StoredCredentials,
    load_stored_credentials,
    try_resolve_api_credential,
)
from frontier.errors import InstallError
from frontier.identity import (
    ARTIFACT_PROFILE_MISMATCH_CODE,
    canonical_id,
    format_organization_identity,
    format_project_identity,
    has_canonical_ids,
    identities_equal,
    identity_incomplete_error,
    identity_mismatch_error,
    identity_mismatches,
    normalize_api_origin,
    tenant_identity,
)
from frontier.local_config import LocalFrontierConfig, load_local_config
from frontier.onboard.constants import DEFAULT_API_URL
from frontier.profiles import (
    ProfileRecord,
    get_profile,
    load_selected_profile_name,
    profile_error,
    refresh_profile_display_names,
    replace_profile,
    validate_profile_name,
)
from frontier.warehouse import default_profiles_path

CI_ENV_KEYS = (
    "GITHUB_ACTIONS",
    "GITLAB_CI",
    "CIRCLECI",
    "TF_BUILD",
    "BITBUCKET_BUILD_NUMBER",
    "BUILDKITE",
    "TRAVIS",
    "JENKINS_URL",
)

SOURCE_FLAG = "flag"
SOURCE_ENV_PROFILE = "env"
SOURCE_LOCAL = "local"
SOURCE_NONE = "none"


@dataclass(frozen=True)
class ExecutionContext:
    profile_name: str | None
    profile_source: str
    api_url: str
    api_url_source: str
    creds: StoredCredentials | None
    organization_id: str
    organization_name: str
    project_id: str
    project_name: str
    dbt_target: str | None
    profiles_path: Path | None
    local: LocalFrontierConfig | None
    record: ProfileRecord | None
    dbt_project_name: str | None = None
    dbt_profile_name: str | None = None
    issues: tuple[str, ...] = ()

    @property
    def selected(self) -> bool:
        return bool(self.profile_name)


def in_recognized_ci() -> bool:
    for key in CI_ENV_KEYS:
        if (os.environ.get(key) or "").strip():
            return True
    flag = (os.environ.get("CI") or "").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _arg_profile_name(args: Any) -> str | None:
    raw = getattr(args, "frontier_profile", None)
    value = str(raw).strip() if raw else ""
    return value or None


def _env_profile_name() -> str | None:
    value = (os.environ.get("FRONTIER_PROFILE") or "").strip()
    return value or None


def select_frontier_profile_name(
    args: Any,
    project_dir: Path,
    *,
    consult_local_state: bool | None = None,
) -> tuple[str | None, str]:
    explicit = _arg_profile_name(args)
    if explicit:
        return validate_profile_name(explicit), SOURCE_FLAG
    env_name = _env_profile_name()
    if env_name:
        return validate_profile_name(env_name), SOURCE_ENV_PROFILE
    use_local = consult_local_state
    if use_local is None:
        use_local = not in_recognized_ci()
    if use_local:
        selected = load_selected_profile_name(project_dir)
        if selected:
            return selected, SOURCE_LOCAL
    return None, SOURCE_NONE


def _normalize_origin(url: str | None) -> str:
    return normalize_api_origin(url)


def resolve_api_origin(
    args: Any,
    *,
    record: ProfileRecord | None,
    creds: StoredCredentials | None,
    local: LocalFrontierConfig | None,
    config_url: str | None = None,
) -> tuple[str, str]:
    explicit = getattr(args, "api_url", None)
    if explicit:
        return _normalize_origin(str(explicit)) or DEFAULT_API_URL, "flag"
    env_url = (os.environ.get("FRONTIER_API_URL") or "").strip()
    if env_url:
        return _normalize_origin(env_url) or DEFAULT_API_URL, "env"
    if record and record.api_url:
        return _normalize_origin(record.api_url) or DEFAULT_API_URL, "profile"
    if creds and creds.api_url:
        return _normalize_origin(creds.api_url) or DEFAULT_API_URL, "credential"
    if local and local.api_url:
        return _normalize_origin(local.api_url) or DEFAULT_API_URL, "local_config"
    if config_url:
        return _normalize_origin(config_url) or DEFAULT_API_URL, "config"
    return DEFAULT_API_URL.rstrip("/"), "default"


def resolve_profiles_yml_path(
    args: Any,
    *,
    record: ProfileRecord | None,
) -> Path | None:
    explicit = getattr(args, "profiles", None)
    if explicit:
        return Path(str(explicit)).expanduser()
    env_dir = (os.environ.get("DBT_PROFILES_DIR") or "").strip()
    if env_dir:
        return Path(env_dir).expanduser() / "profiles.yml"
    if record and record.profiles_path:
        return Path(record.profiles_path).expanduser()
    return None


def resolve_dbt_target(
    args: Any,
    *,
    record: ProfileRecord | None,
    honor_local_config_target: bool = False,
    local: LocalFrontierConfig | None = None,
) -> str | None:
    explicit = getattr(args, "target", None)
    if explicit:
        return str(explicit).strip() or None
    env_target = (os.environ.get("FRONTIER_DBT_TARGET") or "").strip()
    if env_target:
        return env_target
    if record and record.dbt_target:
        return record.dbt_target
    if honor_local_config_target and local and local.dbt_target:
        return local.dbt_target
    return None


def identities_compatible(
    *,
    left_origin: str,
    left_org_id: str,
    left_org_name: str,
    left_project_id: str,
    left_project_name: str,
    right_origin: str,
    right_org_id: str,
    right_org_name: str,
    right_project_id: str,
    right_project_name: str,
    include_origin: bool = True,
) -> list[str]:
    return identity_mismatches(
        tenant_identity(
            api_origin=left_origin,
            organization_id=left_org_id,
            project_id=left_project_id,
            organization_name=left_org_name,
            project_name=left_project_name,
        ),
        tenant_identity(
            api_origin=right_origin,
            organization_id=right_org_id,
            project_id=right_project_id,
            organization_name=right_org_name,
            project_name=right_project_name,
        ),
        include_origin=include_origin,
    )


def _whoami_identity(api_url: str, api_key: str):
    from frontier.onboard.saas import whoami

    return whoami(api_url, api_key)


def ensure_named_profile_ids(
    record: ProfileRecord,
    creds: StoredCredentials,
    *,
    revalidate: bool = False,
) -> ProfileRecord:
    """Require canonical org/project IDs for a named profile; backfill from stored-key whoami."""
    if has_canonical_ids(record.organization_id, record.project_id) and not revalidate:
        return record
    try:
        identity = _whoami_identity(record.api_url or creds.api_url, creds.api_key)
    except InstallError as error:
        if has_canonical_ids(record.organization_id, record.project_id):
            raise
        if getattr(error, "code", "") == "SAAS_UNREACHABLE":
            raise identity_incomplete_error(
                next_action="Retry when Frontier SaaS is reachable, then re-run the command.",
            ) from error
        raise
    if not has_canonical_ids(identity.organization_id, identity.project_id):
        raise identity_incomplete_error()
    if has_canonical_ids(record.organization_id, record.project_id):
        stored = tenant_identity(
            api_origin=record.api_url,
            organization_id=record.organization_id,
            project_id=record.project_id,
            organization_name=record.organization_name,
            project_name=record.project_name,
        )
        live = tenant_identity(
            api_origin=identity.api_url or record.api_url,
            organization_id=identity.organization_id,
            project_id=identity.project_id,
            organization_name=identity.organization,
            project_name=identity.project,
        )
        if not identities_equal(stored, live, include_origin=False):
            raise identity_mismatch_error(
                explanation="Stored profile identity does not match the authenticated project.",
                cause=(
                    f"Profile '{record.name}' is bound to "
                    f"{format_project_identity(record.project_name, record.project_id)}, "
                    f"but the key authenticates "
                    f"{format_project_identity(identity.project, identity.project_id)}."
                ),
                next_action=f"Run `frontier profile create {record.name} --api-key --force`.",
            )
        return refresh_profile_display_names(
            record,
            organization_name=identity.organization,
            project_name=identity.project,
        )
    updated = replace_profile(
        ProfileRecord(
            name=record.name,
            api_url=record.api_url,
            organization_id=canonical_id(identity.organization_id),
            organization_name=identity.organization,
            project_id=canonical_id(identity.project_id),
            project_name=identity.project,
            dbt_target=record.dbt_target,
            profiles_path=record.profiles_path,
            credential_ref=record.credential_ref,
        )
    )
    return updated


def resolve_profile_credential(
    *,
    args: Any,
    record: ProfileRecord,
    api_url: str,
) -> StoredCredentials:
    env_key = (os.environ.get("FRONTIER_API_KEY") or "").strip()
    stored = load_stored_credentials(profile_name=record.name)
    if env_key:
        env_url = (os.environ.get("FRONTIER_API_URL") or "").strip() or api_url
        identity = _whoami_identity(_normalize_origin(env_url) or api_url, env_key)
        live_org_id = canonical_id(getattr(identity, "organization_id", "") or "")
        live_project_id = canonical_id(getattr(identity, "project_id", "") or "")
        if not has_canonical_ids(record.organization_id, record.project_id):
            raise identity_incomplete_error(
                next_action=(
                    "Re-run after the named profile has immutable IDs, or unset FRONTIER_API_KEY "
                    "and let the stored profile credential revalidate."
                ),
            )
        if not has_canonical_ids(live_org_id, live_project_id):
            raise identity_incomplete_error()
        mismatches = identities_compatible(
            left_origin=record.api_url,
            left_org_id=record.organization_id,
            left_org_name=record.organization_name,
            left_project_id=record.project_id,
            left_project_name=record.project_name,
            right_origin=identity.api_url,
            right_org_id=live_org_id,
            right_org_name=identity.organization,
            right_project_id=live_project_id,
            right_project_name=identity.project,
            include_origin=False,
        )
        if mismatches:
            raise identity_mismatch_error(
                explanation="FRONTIER_API_KEY does not match the selected Frontier profile.",
                cause="Differing identities: " + ", ".join(mismatches) + ".",
                next_action=(
                    "Unset FRONTIER_API_KEY, or use the key for "
                    f"{format_project_identity(record.project_name, record.project_id)}."
                ),
            )
        refresh_profile_display_names(
            record,
            organization_name=identity.organization,
            project_name=identity.project,
        )
        return StoredCredentials(
            api_url=_normalize_origin(env_url) or api_url,
            api_key=env_key,
            project=identity.project,
            organization=identity.organization,
            source=SOURCE_ENV,
            organization_id=live_org_id,
            project_id=live_project_id,
        )
    if stored is None:
        raise profile_error(
            "FRONTIER_PROFILE_CREDENTIAL_MISSING",
            f"Frontier profile '{record.name}' has no stored credential.",
            cause="The profile metadata exists but its keychain/file credential is missing.",
            next_action=f"Run `frontier login --api-key --profile {record.name}`.",
        )
    return StoredCredentials(
        api_url=_normalize_origin(stored.api_url) or api_url,
        api_key=stored.api_key,
        project=stored.project or record.project_name,
        organization=stored.organization or record.organization_name,
        source=stored.source,
        organization_id=stored.organization_id or record.organization_id,
        project_id=stored.project_id or record.project_id,
    )


def resolve_execution_context(
    args: Any,
    project_dir: Path | None = None,
    *,
    honor_local_config_target: bool = False,
    require_credential: bool = True,
) -> ExecutionContext:
    """Single execution-context resolver used by all Frontier commands."""
    root = Path(project_dir or getattr(args, "project_dir", None) or ".").expanduser().resolve()
    local = load_local_config(root)
    name, source = select_frontier_profile_name(args, root)
    record = get_profile(name) if name else None
    dbt_project_name, dbt_profile_name = _dbt_names(root)
    if record is None:
        creds = try_resolve_api_credential()
        api_url, api_url_source = resolve_api_origin(args, record=None, creds=creds, local=local)
        if creds and not creds.api_url:
            creds = StoredCredentials(
                api_url=api_url,
                api_key=creds.api_key,
                project=creds.project,
                organization=creds.organization,
                source=creds.source,
                organization_id=creds.organization_id,
                project_id=creds.project_id,
            )
        return ExecutionContext(
            profile_name=None,
            profile_source=source,
            api_url=api_url,
            api_url_source=api_url_source,
            creds=creds,
            organization_id=creds.organization_id if creds else "",
            organization_name=creds.organization if creds else "",
            project_id=creds.project_id if creds else "",
            project_name=creds.project if creds else (local.project if local else ""),
            dbt_target=resolve_dbt_target(
                args,
                record=None,
                honor_local_config_target=honor_local_config_target,
                local=local,
            ),
            profiles_path=resolve_profiles_yml_path(args, record=None),
            local=local,
            record=None,
            dbt_project_name=dbt_project_name,
            dbt_profile_name=dbt_profile_name,
        )

    placeholder_url, _source = resolve_api_origin(args, record=record, creds=None, local=local)
    issues: list[str] = []
    creds = None
    try:
        creds = resolve_profile_credential(args=args, record=record, api_url=placeholder_url)
    except InstallError as error:
        issues.append(getattr(error, "code", "") or "AUTH_REQUIRED")
        if require_credential or getattr(error, "code", "") not in {
            "FRONTIER_PROFILE_CREDENTIAL_MISSING",
            "FRONTIER_PROFILE_IDENTITY_MISMATCH",
            "FRONTIER_PROFILE_IDENTITY_INCOMPLETE",
        }:
            raise
    if creds and require_credential:
        record = ensure_named_profile_ids(record, creds)
        record = get_profile(record.name)
    elif creds and not has_canonical_ids(record.organization_id, record.project_id):
        issues.append("FRONTIER_PROFILE_IDENTITY_INCOMPLETE")
    api_url, api_url_source = resolve_api_origin(args, record=record, creds=creds, local=local)
    return ExecutionContext(
        profile_name=record.name,
        profile_source=source,
        api_url=api_url,
        api_url_source=api_url_source,
        creds=creds,
        organization_id=record.organization_id or (creds.organization_id if creds else ""),
        organization_name=record.organization_name or (creds.organization if creds else ""),
        project_id=record.project_id or (creds.project_id if creds else ""),
        project_name=record.project_name or (creds.project if creds else ""),
        dbt_target=resolve_dbt_target(
            args,
            record=record,
            honor_local_config_target=False,
            local=local,
        ),
        profiles_path=resolve_profiles_yml_path(args, record=record),
        local=local,
        record=record,
        dbt_project_name=dbt_project_name,
        dbt_profile_name=dbt_profile_name,
        issues=tuple(code for code in issues if code),
    )


def require_context_credentials(ctx: ExecutionContext) -> StoredCredentials:
    if ctx.creds is None:
        raise ConfigError(AUTH_REQUIRED_MESSAGE)
    return ctx.creds


def warehouse_connect_kwargs(args: Any, ctx: ExecutionContext) -> dict[str, Any]:
    profiles_path = ctx.profiles_path
    target = ctx.dbt_target
    explicit_profiles = getattr(args, "profiles", None)
    if explicit_profiles:
        profiles_path = Path(str(explicit_profiles)).expanduser()
    explicit_target = getattr(args, "target", None)
    if explicit_target:
        target = str(explicit_target).strip() or target
    return {"profiles_path": profiles_path, "target": target}


def assessment_identity_fields(
    ctx: ExecutionContext,
    *,
    dbt_target: str | None,
    dbt_project: str | None,
    pinned_version: int | None = None,
    pinned_fingerprint: str | None = None,
) -> dict[str, Any]:
    if ctx.selected and not has_canonical_ids(ctx.organization_id, ctx.project_id):
        raise identity_incomplete_error(
            next_action="Re-run after the named Frontier profile has immutable organization and project IDs.",
        )
    fields: dict[str, Any] = {
        "apiOrigin": ctx.api_url,
    }
    if ctx.organization_id:
        fields["organizationId"] = ctx.organization_id
    if ctx.organization_name:
        fields["organizationName"] = ctx.organization_name
    if ctx.project_id:
        fields["projectId"] = ctx.project_id
    if ctx.project_name:
        fields["projectName"] = ctx.project_name
    if dbt_project:
        fields["dbtProject"] = dbt_project
    if dbt_target:
        fields["dbtTarget"] = dbt_target
    if pinned_version is not None:
        fields["pinnedManifestVersion"] = pinned_version
    if pinned_fingerprint:
        fields["pinnedManifestFingerprint"] = pinned_fingerprint
    if ctx.profile_name:
        fields["frontierProfile"] = ctx.profile_name
    return fields


def assert_artifact_matches_context(
    payload: dict[str, Any],
    ctx: ExecutionContext,
    *,
    dbt_project: str | None,
    dbt_target: str | None,
    pinned_version: int | None = None,
    pinned_fingerprint: str | None = None,
) -> None:
    identity = payload.get("assessmentIdentity") or {}
    if not isinstance(identity, dict):
        return
    recorded_org_id = canonical_id(str(identity.get("organizationId") or ""))
    recorded_project_id = canonical_id(str(identity.get("projectId") or ""))
    if not has_canonical_ids(recorded_org_id, recorded_project_id):
        return
    recorded_origin = str(identity.get("apiOrigin") or "").strip()
    recorded_org = str(identity.get("organizationName") or "").strip()
    recorded_project = str(identity.get("projectName") or payload.get("project") or "").strip()
    recorded_dbt_project = str(identity.get("dbtProject") or "").strip()
    recorded_target = str(identity.get("dbtTarget") or payload.get("dbtTarget") or "").strip()
    recorded_version = identity.get("pinnedManifestVersion")
    recorded_fingerprint = str(identity.get("pinnedManifestFingerprint") or "").strip()
    if ctx.selected and not has_canonical_ids(ctx.organization_id, ctx.project_id):
        raise identity_incomplete_error(
            next_action="Switch to a Frontier profile with immutable IDs, then retry upload.",
        )
    mismatches: list[str] = []
    mismatches.extend(
        identities_compatible(
            left_origin=recorded_origin,
            left_org_id=recorded_org_id,
            left_org_name=recorded_org,
            left_project_id=recorded_project_id,
            left_project_name=recorded_project,
            right_origin=ctx.api_url,
            right_org_id=ctx.organization_id,
            right_org_name=ctx.organization_name,
            right_project_id=ctx.project_id,
            right_project_name=ctx.project_name,
        )
    )
    if recorded_dbt_project and dbt_project and recorded_dbt_project != dbt_project:
        mismatches.append(f"dbt project ({recorded_dbt_project} vs {dbt_project})")
    current_target = dbt_target or ctx.dbt_target
    if recorded_target and current_target and recorded_target != current_target:
        mismatches.append(f"dbt target ({recorded_target} vs {current_target})")
    if recorded_version is not None and pinned_version is not None and recorded_version != pinned_version:
        mismatches.append("semantic manifest version")
    if recorded_fingerprint and pinned_fingerprint and recorded_fingerprint != pinned_fingerprint:
        mismatches.append("semantic manifest fingerprint")
    if mismatches:
        artifact_project = format_project_identity(recorded_project, recorded_project_id)
        current_project = format_project_identity(ctx.project_name, ctx.project_id)
        raise profile_error(
            ARTIFACT_PROFILE_MISMATCH_CODE,
            "This assessment was generated for a different Frontier execution context.\n"
            f"Artifact project: {artifact_project}\n"
            f"Current project: {current_project}",
            cause="Differing identities: " + ", ".join(mismatches) + ".",
            next_action="Switch back to the Frontier profile that generated the artifact, then retry upload.",
            docs_path="/docs/troubleshooting#artifact-profile-mismatch",
        )


def format_execution_banner(
    ctx: ExecutionContext,
    *,
    dbt_profile: str | None,
    dbt_target: str | None,
    warehouse_type: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> str:
    lines: list[str] = []
    if ctx.profile_name:
        lines.append(f"Frontier profile: {ctx.profile_name}")
        lines.append(
            f"Organization: {format_organization_identity(ctx.organization_name, ctx.organization_id)}"
        )
        lines.append(f"Project: {format_project_identity(ctx.project_name, ctx.project_id)}")
    if dbt_profile:
        lines.append(f"dbt profile: {dbt_profile}")
    if dbt_target:
        lines.append(f"dbt target: {dbt_target}")
    if warehouse_type:
        lines.append(f"Warehouse adapter: {warehouse_type}")
    if database or schema:
        location = ".".join(part for part in (database, schema) if part)
        if location:
            lines.append(f"Database/schema: {location}")
    return "\n".join(lines)


def print_execution_banner(
    ctx: ExecutionContext,
    *,
    dbt_profile: str | None = None,
    dbt_target: str | None = None,
    warehouse_type: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> None:
    text = format_execution_banner(
        ctx,
        dbt_profile=dbt_profile or ctx.dbt_profile_name,
        dbt_target=dbt_target or ctx.dbt_target,
        warehouse_type=warehouse_type,
        database=database,
        schema=schema,
    )
    if text:
        print(text, flush=True)


def default_profiles_yml() -> Path:
    return default_profiles_path()


def _dbt_names(project_dir: Path) -> tuple[str | None, str | None]:
    path = project_dir / "dbt_project.yml"
    if not path.is_file():
        return None, None
    try:
        import yaml

        loaded = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return None, None
    if not isinstance(loaded, dict):
        return None, None
    project = str(loaded.get("name") or "").strip() or None
    profile = str(loaded.get("profile") or "").strip() or None
    return project, profile
