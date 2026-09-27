from __future__ import annotations

import getpass
import os
from pathlib import Path
from typing import Any

from frontier.context import (
    SOURCE_FLAG,
    SOURCE_ENV_PROFILE,
    SOURCE_LOCAL,
    SOURCE_NONE,
    ensure_named_profile_ids,
    resolve_execution_context,
)
from frontier.credentials import (
    StoredCredentials,
    default_fallback_path,
    delete_credentials,
    key_prefix,
    load_stored_credentials,
    save_credentials,
)
from frontier.errors import InstallError
from frontier.identity import (
    format_organization_identity,
    format_project_identity,
    has_canonical_ids,
    identities_equal,
    identity_incomplete_error,
    tenant_identity,
)
from frontier.onboard.constants import DEFAULT_API_URL
from frontier.onboard.gitignore import ensure_gitignore
from frontier.onboard.prompt import prompt_text, prompt_yes_no
from frontier.onboard.saas import whoami
from frontier.profiles import (
    FRONTIER_PROFILE_HELP,
    ProfileRecord,
    clear_selected_profile_if_matches,
    get_profile,
    load_registry,
    profile_credential_status,
    profile_error,
    remove_profile_metadata,
    upsert_profile,
    validate_profile_name,
    write_selected_profile,
)
from frontier.warehouse import load_dbt_profile_output

__all__ = ["FRONTIER_PROFILE_HELP"]


def _project_dir(args: Any) -> Path:
    flag = getattr(args, "project_dir_opt", None)
    value = flag or getattr(args, "project_dir", ".") or "."
    return Path(value).expanduser().resolve()


def _prompt_api_key(args: Any) -> str:
    reader = getattr(args, "_getpass", None) or getpass.getpass
    try:
        api_key = str(reader("Project API key: ")).strip()
    except (EOFError, KeyboardInterrupt) as error:
        raise InstallError(
            "AUTH_CANCELLED",
            "Login cancelled.",
            cause="No API key was entered.",
            next_action="Re-run with `--api-key` and paste the project key.",
            docs_path="/docs/quick-start",
        ) from error
    if not api_key:
        raise InstallError(
            "AUTH_INVALID",
            "No API key was entered.",
            cause="Hidden input was empty.",
            next_action="Paste the project API key shown once at project creation.",
            docs_path="/docs/quick-start",
        )
    return api_key


def _resolve_create_api_url(args: Any) -> str:
    explicit = getattr(args, "api_url", None)
    if explicit:
        return str(explicit).rstrip("/")
    env_url = (os.environ.get("FRONTIER_API_URL") or "").strip()
    if env_url:
        return env_url.rstrip("/")
    assume = bool(getattr(args, "yes", False) or getattr(args, "force", False))
    return prompt_text(
        "Frontier API URL",
        default=DEFAULT_API_URL,
        assume=DEFAULT_API_URL if assume else None,
    ).rstrip("/") or DEFAULT_API_URL


def authenticate_project_key(api_url: str, api_key: str) -> StoredCredentials:
    identity = whoami(api_url, api_key)
    return StoredCredentials(
        api_url=identity.api_url or api_url.rstrip("/"),
        api_key=api_key,
        project=identity.project,
        organization=identity.organization,
        organization_id=identity.organization_id,
        project_id=identity.project_id,
    )


def require_named_profile_identity(creds: StoredCredentials) -> None:
    if not has_canonical_ids(creds.organization_id, creds.project_id):
        raise identity_incomplete_error()


def store_profile_credential(
    *,
    name: str,
    creds: StoredCredentials,
    dbt_target: str | None,
    profiles_path: str | None,
    force: bool = False,
) -> tuple[ProfileRecord, str]:
    require_named_profile_identity(creds)
    record = ProfileRecord(
        name=name,
        api_url=creds.api_url,
        organization_id=creds.organization_id,
        organization_name=creds.organization,
        project_id=creds.project_id,
        project_name=creds.project,
        dbt_target=dbt_target or None,
        profiles_path=profiles_path or None,
        credential_ref=name,
    )
    upsert_profile(record, force=force)
    backend = save_credentials(creds, profile_name=name)
    return record, backend


def _maybe_validate_target(project_dir: Path, target: str | None, profiles: str | None) -> None:
    if not target:
        return
    dbt_project = project_dir / "dbt_project.yml"
    if not dbt_project.is_file():
        return
    explicit = Path(profiles).expanduser() if profiles else None
    env_dir = (os.environ.get("DBT_PROFILES_DIR") or "").strip()
    if explicit is None and not env_dir:
        from frontier.warehouse import default_profiles_path

        if not default_profiles_path().is_file():
            return
    try:
        load_dbt_profile_output(
            project_dir,
            profiles_path=Path(profiles).expanduser() if profiles else None,
            target=target,
        )
    except Exception as error:
        message = str(error)
        if "Missing dbt profiles.yml" in message or "Missing dbt_project.yml" in message:
            return
        raise profile_error(
            "FRONTIER_PROFILE_TARGET_MISSING",
            f"dbt target '{target}' was not found.",
            cause=message,
            next_action=f"Add target `{target}` to the dbt profile declared by dbt_project.yml.",
            docs_path="/docs/authenticate",
        ) from error


def cmd_profile_create(args: Any) -> int:
    name = validate_profile_name(str(getattr(args, "name", "") or ""))
    api_url = _resolve_create_api_url(args)
    want_prompt = bool(getattr(args, "api_key", False))
    env_key = (os.environ.get("FRONTIER_API_KEY") or "").strip()
    legacy = load_stored_credentials()
    if want_prompt:
        api_key = _prompt_api_key(args)
    elif env_key:
        api_key = env_key
    elif legacy and legacy.api_key:
        api_key = legacy.api_key
        api_url = (legacy.api_url or api_url).rstrip("/")
    else:
        api_key = _prompt_api_key(args)
    creds = authenticate_project_key(api_url, api_key)
    require_named_profile_identity(creds)
    target = (getattr(args, "target", None) or "").strip() or None
    profiles = (getattr(args, "profiles", None) or "").strip() or None
    project_dir = _project_dir(args)
    _maybe_validate_target(project_dir, target, profiles)
    record, backend = store_profile_credential(
        name=name,
        creds=creds,
        dbt_target=target,
        profiles_path=profiles,
        force=bool(getattr(args, "force", False)),
    )
    print("Authenticated")
    print(f"Frontier profile: {record.name}")
    print(f"Organization: {format_organization_identity(record.organization_name, record.organization_id)}")
    print(f"Project: {format_project_identity(record.project_name, record.project_id)}")
    print(f"API key: {key_prefix(creds.api_key)}")
    if record.dbt_target:
        print(f"dbt target: {record.dbt_target}")
    if backend == "file":
        print(
            "Warning: secure keychain storage is unavailable. "
            f"Credentials were stored in {default_fallback_path()} with permission 0600.",
        )
    return 0


def cmd_profile_list(args: Any) -> int:
    registry = load_registry()
    ctx = resolve_execution_context(args, _project_dir(args), require_credential=False)
    if not registry.profiles:
        print("No Frontier profiles.")
        print("Create one with `frontier profile create NAME --api-key`.")
        return 0
    width = max(len(name) for name in registry.profiles)
    for name, record in registry.profiles.items():
        marker = "*" if ctx.profile_name == name else " "
        org_project = " / ".join(
            part for part in (record.organization_name, record.project_name) if part
        ) or "(unknown)"
        target = f"target={record.dbt_target}" if record.dbt_target else "target=(none)"
        credential = f"credential={profile_credential_status(name)}"
        print(
            f"{marker} {name:<{width}}  {org_project:<32}  {target:<18}  {credential}"
        )
    return 0


def cmd_profile_use(args: Any) -> int:
    name = validate_profile_name(str(getattr(args, "name", "") or ""))
    record = get_profile(name)
    stored = load_stored_credentials(profile_name=name)
    if stored is None:
        raise profile_error(
            "FRONTIER_PROFILE_CREDENTIAL_MISSING",
            f"Frontier profile '{name}' has no stored credential.",
            cause="The profile exists but its keychain/file credential is missing.",
            next_action=f"Run `frontier login --api-key --profile {name}`.",
        )
    if not bool(getattr(args, "offline", False)):
        try:
            record = ensure_named_profile_ids(record, stored, revalidate=True)
        except InstallError as error:
            if getattr(error, "code", "") == "SAAS_UNREACHABLE":
                pass
            else:
                raise
    project_dir = _project_dir(args)
    write_selected_profile(project_dir, name)
    ensure_gitignore(project_dir)
    print(f"Using Frontier profile: {name}")
    print(f"Organization: {format_organization_identity(record.organization_name, record.organization_id)}")
    print(f"Project: {format_project_identity(record.project_name, record.project_id)}")
    if record.dbt_target:
        print(f"dbt target: {record.dbt_target}")
    print("Selection is local to this project and is not committed.")
    return 0


def _identity_status(ctx) -> str:
    if not ctx.profile_name:
        return "none"
    if ctx.creds is None or profile_credential_status(ctx.profile_name) == "missing":
        return "credential missing"
    try:
        identity = whoami(ctx.api_url, ctx.creds.api_key)
    except InstallError as error:
        if getattr(error, "code", "") == "SAAS_UNREACHABLE":
            return "SaaS unavailable; identity not revalidated"
        if getattr(error, "code", "") == "AUTH_INVALID":
            return "identity mismatch"
        return "SaaS unavailable; identity not revalidated"
    if not has_canonical_ids(identity.organization_id, identity.project_id):
        return "identity incomplete"
    if not has_canonical_ids(ctx.organization_id, ctx.project_id):
        return "identity incomplete"
    if not identities_equal(
        tenant_identity(
            api_origin=ctx.api_url,
            organization_id=ctx.organization_id,
            project_id=ctx.project_id,
            organization_name=ctx.organization_name,
            project_name=ctx.project_name,
        ),
        tenant_identity(
            api_origin=identity.api_url or ctx.api_url,
            organization_id=identity.organization_id,
            project_id=identity.project_id,
            organization_name=identity.organization,
            project_name=identity.project,
        ),
        include_origin=False,
    ):
        return "identity mismatch"
    return "verified"


def cmd_profile_status(args: Any) -> int:
    project_dir = _project_dir(args)
    ctx = resolve_execution_context(args, project_dir, honor_local_config_target=False, require_credential=False)
    source_label = {
        SOURCE_FLAG: "--profile",
        SOURCE_ENV_PROFILE: "FRONTIER_PROFILE",
        SOURCE_LOCAL: "local selection",
        SOURCE_NONE: "none (legacy)",
    }.get(ctx.profile_source, ctx.profile_source)
    print(f"Frontier profile: {ctx.profile_name or '(none)'}")
    print(f"Selection source: {source_label}")
    print(
        f"Organization: {format_organization_identity(ctx.organization_name, ctx.organization_id)}"
    )
    print(f"Project: {format_project_identity(ctx.project_name, ctx.project_id)}")
    print(f"API origin: {ctx.api_url}")
    if ctx.profile_name:
        print(f"Credential source: {profile_credential_status(ctx.profile_name)}")
        print(f"Identity: {_identity_status(ctx)}")
    elif ctx.creds:
        print(f"Credential source: {ctx.creds.source}")
    else:
        print("Credential source: missing")
    print(f"dbt project: {ctx.dbt_project_name or '(none)'}")
    print(f"dbt profile: {ctx.dbt_profile_name or '(none)'}")
    profiles_path = ctx.profiles_path
    if profiles_path is None:
        from frontier.warehouse import default_profiles_path

        env_dir = (os.environ.get("DBT_PROFILES_DIR") or "").strip()
        profiles_path = Path(env_dir).expanduser() / "profiles.yml" if env_dir else default_profiles_path()
    print(f"profiles.yml: {profiles_path}")
    target = ctx.dbt_target
    adapter = database = schema = warehouse = ""
    warning = None
    try:
        output = load_dbt_profile_output(
            project_dir,
            profiles_path=ctx.profiles_path,
            target=target,
        )
        from frontier.config import redact

        redacted = redact(output)
        adapter = str(redacted.get("type") or "")
        database = str(redacted.get("database") or redacted.get("dbname") or redacted.get("project") or "")
        schema = str(redacted.get("schema") or redacted.get("dataset") or "")
        warehouse = str(redacted.get("warehouse") or "")
        if not target:
            target = str(output.get("_target") or "") or target
    except Exception as error:
        warning = str(error)
    print(f"dbt target: {target or '(profiles.yml default, then dev)'}")
    if adapter:
        print(f"Warehouse adapter: {adapter}")
    if database or schema:
        print(f"Database/schema: {'.'.join(part for part in (database, schema) if part)}")
    if warehouse:
        print(f"Warehouse: {warehouse}")
    if ctx.profile_name and profile_credential_status(ctx.profile_name) == "missing":
        print("Warning: Frontier profile credential is missing.")
    if warning:
        print(f"Warning: {warning}")
    return 0 if (not ctx.profile_name or profile_credential_status(ctx.profile_name) != "missing") else 1


def cmd_profile_remove(args: Any) -> int:
    name = validate_profile_name(str(getattr(args, "name", "") or ""))
    project_dir = _project_dir(args)
    ctx = resolve_execution_context(args, project_dir, require_credential=False)
    force = bool(getattr(args, "force", False))
    assume = force
    if ctx.profile_name == name and not force:
        confirmed = prompt_yes_no(
            f"Remove the active Frontier profile '{name}'?",
            default=False,
            assume_yes=False,
        )
        if not confirmed:
            print(f"Kept active Frontier profile '{name}'. Pass --force to remove it.")
            return 1
    registry = load_registry()
    existed = name in registry.profiles
    if existed:
        remove_profile_metadata(name)
    clear_selected_profile_if_matches(project_dir, name)
    stored = load_stored_credentials(profile_name=name)
    if stored is not None:
        delete_cred = assume or prompt_yes_no(
            f"Delete the stored credential for Frontier profile '{name}'?",
            default=False,
            assume_yes=False,
        )
        if delete_cred:
            delete_credentials(profile_name=name)
            print(f"Removed credential for Frontier profile '{name}'.")
        else:
            print(f"Kept credential for Frontier profile '{name}'.")
    if existed:
        print(f"Removed Frontier profile '{name}'.")
    else:
        print(f"Frontier profile '{name}' was already absent.")
    return 0
