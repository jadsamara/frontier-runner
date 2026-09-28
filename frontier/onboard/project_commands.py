from __future__ import annotations

import secrets
from pathlib import Path
from typing import Any

from frontier.context import in_recognized_ci
from frontier.credentials import (
    StoredCredentials,
    default_fallback_path,
    key_prefix,
)
from frontier.errors import InstallError
from frontier.identity import (
    format_organization_identity,
    format_project_identity,
    has_canonical_ids,
)
from frontier.onboard.constants import DEFAULT_API_URL
from frontier.onboard.profile_commands import store_profile_credential
from frontier.onboard.prompt import is_interactive, prompt_choice
from frontier.onboard.saas import (
    create_cli_project,
    get_cli_project,
    issue_cli_project_key,
    list_cli_organizations,
    list_cli_projects,
)
from frontier.onboard.user_auth import require_user_authorization
from frontier.profiles import (
    load_registry,
    validate_profile_name,
    write_selected_profile,
)


def _api_url(args: Any) -> str:
    return (getattr(args, "api_url", None) or DEFAULT_API_URL).rstrip("/")


def _project_dir(args: Any) -> Path:
    flag = getattr(args, "project_dir_opt", None)
    value = flag or getattr(args, "project_dir", ".") or "."
    return Path(value).expanduser().resolve()


def _refuse_implicit_ci(args: Any) -> None:
    if not in_recognized_ci():
        return
    if bool(getattr(args, "non_interactive", False)):
        return
    raise InstallError(
        "USER_AUTH_REQUIRED",
        "Project creation is not allowed implicitly in CI.",
        cause="Recognized CI without --non-interactive user authorization.",
        next_action="Create projects locally, then set FRONTIER_API_KEY in GitHub Actions.",
        docs_path="/docs/github",
    )


def _select_organization(args: Any, organizations: list[dict[str, str]]) -> str | None:
    explicit = (getattr(args, "organization", None) or "").strip() or None
    if explicit:
        return explicit
    if len(organizations) == 1:
        org = organizations[0]
        print(f"Organization: {org['name']} ({org['id']})")
        return org["id"]
    if len(organizations) == 0:
        raise InstallError(
            "ORGANIZATION_NOT_FOUND",
            "No Frontier organization is available for this user.",
            cause="The authenticated user has no organization membership.",
            next_action="Create an organization in Frontier, then retry.",
        )
    non_interactive = bool(getattr(args, "non_interactive", False)) or not is_interactive()
    if non_interactive:
        raise InstallError(
            "ORGANIZATION_REQUIRED",
            "Select a Frontier organization.",
            cause="This user belongs to multiple organizations.",
            next_action="Re-run with `--organization ORG_ID_OR_SLUG`.",
        )
    labels = [f"{item['name']} ({item['slug']})" for item in organizations]
    chosen = prompt_choice("Select an organization", labels)
    return organizations[labels.index(chosen)]["id"]


def create_project_with_optional_profile(args: Any) -> int:
    """Shared implementation for `project create` and `profile create --create-project`."""
    _refuse_implicit_ci(args)
    api_url = _api_url(args)
    user = require_user_authorization(api_url)
    organizations = list(list_cli_organizations(api_url, user.access_token))
    organization = _select_organization(args, organizations)
    name = str(getattr(args, "project_name", None) or getattr(args, "create_project", None) or "").strip()
    if not name:
        raise InstallError(
            "PROJECT_CREATE_FAILED",
            "A project name is required.",
            cause="No project name was provided.",
            next_action="Pass the SaaS project name to create.",
        )
    profile_name = (getattr(args, "profile_name", None) or getattr(args, "name", None) or "").strip()
    if getattr(args, "no_profile", False):
        profile_name = ""
    if profile_name:
        profile_name = validate_profile_name(profile_name)
    created = create_cli_project(
        api_url,
        user.access_token,
        name=name,
        organization=organization,
        warehouse_type=(getattr(args, "warehouse", None) or "").strip() or None,
        idempotency_key=secrets.token_urlsafe(18),
    )
    if not has_canonical_ids(created.organization_id, created.id):
        raise InstallError(
            "FRONTIER_PROFILE_IDENTITY_INCOMPLETE",
            "The created project did not return canonical organization and project IDs.",
            cause="POST /api/v1/projects omitted organizationId or id.",
            next_action="Retry after Frontier SaaS is upgraded.",
        )
    issued = issue_cli_project_key(
        api_url,
        user.access_token,
        created.id,
    )
    creds = StoredCredentials(
        api_url=api_url,
        api_key=issued.api_key,
        project=created.name,
        organization=created.organization_name,
        organization_id=created.organization_id,
        project_id=created.id,
    )
    try:
        if profile_name:
            _record, backend = store_profile_credential(
                name=profile_name,
                creds=creds,
                dbt_target=(getattr(args, "target", None) or "").strip() or None,
                profiles_path=(getattr(args, "profiles", None) or "").strip() or None,
                force=bool(getattr(args, "force", False)),
            )
        else:
            from frontier.credentials import save_credentials

            backend = save_credentials(creds)
    except InstallError:
        raise
    except Exception as error:
        print("Project created" if created.created else "Project already created by this request")
        print(
            "Organization: "
            + format_organization_identity(created.organization_name, created.organization_id)
        )
        print("Project: " + format_project_identity(created.name, created.id))
        print("Credential: not stored")
        recovery = (
            f"frontier project key create {created.id}"
            + (f" --profile {profile_name}" if profile_name else "")
        )
        print(f"Next: {recovery}")
        raise InstallError(
            "PROJECT_KEY_STORAGE_FAILED",
            "The project was created, but the project API key could not be stored locally.",
            cause=type(error).__name__,
            next_action=recovery,
            docs_path="/docs/authenticate",
        ) from error
    creds = None
    issued = None

    print("Project created" if created.created else "Project already created by this request")
    print(
        "Organization: "
        + format_organization_identity(created.organization_name, created.organization_id)
    )
    print("Project: " + format_project_identity(created.name, created.id))
    if profile_name:
        print(f"Frontier profile: {profile_name}")
    target = (getattr(args, "target", None) or "").strip()
    if target:
        print(f"dbt target: {target}")
    print(
        "Credential: stored in keychain"
        if backend == "keyring"
        else f"Credential: stored in {default_fallback_path()} (0600)"
    )
    if profile_name and bool(getattr(args, "use", False)):
        write_selected_profile(_project_dir(args), profile_name)
        print(f"Selected Frontier profile `{profile_name}` for this repository.")
    elif profile_name:
        print(f"Next: frontier profile use {profile_name}")
    else:
        print("Next: frontier profile create NAME --api-key")
    return 0


def cmd_organization_list(args: Any) -> int:
    api_url = _api_url(args)
    user = require_user_authorization(api_url)
    organizations = list_cli_organizations(api_url, user.access_token)
    if not organizations:
        print("No organizations.")
        return 0
    print(f"{'ORGANIZATION':<28} {'ORGANIZATION ID':<38} ROLE")
    for org in organizations:
        print(f"{org['name']:<28} {org['id']:<38} {org['role']}")
    return 0


def cmd_project_list(args: Any) -> int:
    api_url = _api_url(args)
    user = require_user_authorization(api_url)
    projects = list_cli_projects(api_url, user.access_token)
    registry = load_registry()
    print(
        f"{'PROJECT':<22} {'PROJECT ID':<38} {'ORGANIZATION':<18} {'ROLE':<8} LOCAL PROFILE"
    )
    for project in projects:
        local = "-"
        for name, record in registry.profiles.items():
            if record.organization_id == project["organizationId"] and record.project_id == project["id"]:
                local = name
                break
        print(
            f"{project['name']:<22} {project['id']:<38} {project['organizationName']:<18} {project['role']:<8} {local}"
        )
    return 0


def cmd_project_status(args: Any) -> int:
    api_url = _api_url(args)
    user = require_user_authorization(api_url)
    project = get_cli_project(api_url, user.access_token, str(getattr(args, "project", "") or ""))
    print("Project: " + format_project_identity(project["name"], project["id"]))
    print(
        "Organization: "
        + format_organization_identity(project["organizationName"], project["organizationId"])
    )
    print(f"Role: {project['role']}")
    registry = load_registry()
    locals_ = [
        name
        for name, record in registry.profiles.items()
        if record.organization_id == project["organizationId"] and record.project_id == project["id"]
    ]
    print(f"Local profiles: {', '.join(locals_) or '(none)'}")
    return 0


def cmd_project_create(args: Any) -> int:
    args.project_name = str(getattr(args, "name", "") or "")
    args.profile_name = (getattr(args, "profile", None) or getattr(args, "frontier_profile", None) or "")
    return create_project_with_optional_profile(args)


def cmd_project_key_create(args: Any) -> int:
    _refuse_implicit_ci(args)
    api_url = _api_url(args)
    user = require_user_authorization(api_url)
    project_ref = str(getattr(args, "project", "") or "").strip()
    issued = issue_cli_project_key(api_url, user.access_token, project_ref)
    if not has_canonical_ids(issued.organization_id, issued.project_id):
        raise InstallError(
            "FRONTIER_PROFILE_IDENTITY_INCOMPLETE",
            "Key issuance did not return canonical organization and project IDs.",
            cause="The API key response omitted identity IDs.",
            next_action="Retry after Frontier SaaS is upgraded.",
        )
    profile_name = (getattr(args, "profile", None) or getattr(args, "frontier_profile", None) or "").strip()
    creds = StoredCredentials(
        api_url=api_url,
        api_key=issued.api_key,
        project=issued.project_name,
        organization=issued.organization_name,
        organization_id=issued.organization_id,
        project_id=issued.project_id,
    )
    if profile_name:
        _record, backend = store_profile_credential(
            name=validate_profile_name(profile_name),
            creds=creds,
            dbt_target=(getattr(args, "target", None) or "").strip() or None,
            profiles_path=(getattr(args, "profiles", None) or "").strip() or None,
            force=True,
        )
        print(f"Stored a replacement project key for Frontier profile `{profile_name}`.")
    else:
        from frontier.credentials import save_credentials

        backend = save_credentials(creds)
        print("Stored a replacement project API key.")
    print(
        "Credential: stored in keychain"
        if backend == "keyring"
        else f"Credential: stored in {default_fallback_path()} (0600)"
    )
    print(f"API key: {key_prefix(issued.api_key)}")
    issued = None
    creds = None
    return 0


def cmd_profile_create_project(args: Any) -> int:
    args.project_name = str(getattr(args, "create_project", "") or "")
    args.profile_name = str(getattr(args, "name", "") or "")
    return create_project_with_optional_profile(args)
