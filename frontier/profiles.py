from __future__ import annotations

import os
import re
import tempfile
import unicodedata
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from frontier.credentials import credential_backend
from frontier.errors import InstallError
from frontier.onboard.constants import CONFIG_DIR_NAME

PROFILE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
REGISTRY_VERSION = 1
STATE_VERSION = 1
STATE_FILE_NAME = "state.yml"
REGISTRY_FILE_NAME = "profiles.yml"
INTERNAL_GITIGNORE = "state.yml\n"

FRONTIER_PROFILE_HELP = (
    "Frontier profile name. A Frontier profile is a local named execution context. "
    "It selects a Frontier SaaS project credential and may provide a default dbt "
    "target and profiles.yml path. It does not replace the dbt `profile:` declared "
    "by dbt_project.yml."
)


def profile_error(
    code: str,
    explanation: str,
    *,
    cause: str,
    next_action: str,
    docs_path: str = "/docs/authenticate",
) -> InstallError:
    return InstallError(
        code,
        explanation,
        cause=cause,
        next_action=next_action,
        docs_path=docs_path,
    )


def validate_profile_name(name: str) -> str:
    value = (name or "").strip()
    if not value:
        raise profile_error(
            "FRONTIER_PROFILE_INVALID_NAME",
            "Frontier profile name is empty.",
            cause="A Frontier profile name is required.",
            next_action="Use a name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}.",
        )
    if any(ord(char) < 32 for char in value) or "/" in value or "\\" in value:
        raise profile_error(
            "FRONTIER_PROFILE_INVALID_NAME",
            "Frontier profile name contains unsafe characters.",
            cause="Names must not include slashes, path separators, or control characters.",
            next_action="Use a name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}.",
        )
    if ".." in value or value in {".", ".."}:
        raise profile_error(
            "FRONTIER_PROFILE_INVALID_NAME",
            "Frontier profile name is not allowed.",
            cause="Path traversal tokens are rejected.",
            next_action="Use a name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}.",
        )
    normalized = unicodedata.normalize("NFKC", value)
    if normalized != value or not PROFILE_NAME_PATTERN.fullmatch(value):
        raise profile_error(
            "FRONTIER_PROFILE_INVALID_NAME",
            "Frontier profile name is invalid.",
            cause="The name failed conservative validation or unsafe normalization.",
            next_action="Use a name matching [A-Za-z0-9][A-Za-z0-9._-]{0,63}.",
        )
    return value


@dataclass(frozen=True)
class ProfileRecord:
    name: str
    api_url: str
    organization_id: str = ""
    organization_name: str = ""
    project_id: str = ""
    project_name: str = ""
    dbt_target: str | None = None
    profiles_path: str | None = None
    credential_ref: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "api_url": self.api_url,
            "organization_id": self.organization_id or None,
            "organization_name": self.organization_name or None,
            "project_id": self.project_id or None,
            "project_name": self.project_name or None,
            "dbt_target": self.dbt_target,
            "profiles_path": self.profiles_path,
            "credential_ref": self.credential_ref or self.name,
        }


@dataclass
class ProfileRegistry:
    version: int = REGISTRY_VERSION
    profiles: dict[str, ProfileRecord] = field(default_factory=dict)
    path: Path | None = None


def default_registry_path() -> Path:
    override = (os.environ.get("FRONTIER_PROFILES_FILE") or "").strip()
    if override:
        return Path(override).expanduser()
    xdg = (os.environ.get("XDG_CONFIG_HOME") or "").strip()
    root = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return root / "frontier" / REGISTRY_FILE_NAME


def state_path(project_dir: Path) -> Path:
    return project_dir / CONFIG_DIR_NAME / STATE_FILE_NAME


def internal_gitignore_path(project_dir: Path) -> Path:
    return project_dir / CONFIG_DIR_NAME / ".gitignore"


def _record_from_mapping(name: str, raw: Any) -> ProfileRecord | None:
    if not isinstance(raw, dict):
        return None
    api_url = str(raw.get("api_url") or "").strip()
    if not api_url:
        return None
    target = str(raw.get("dbt_target") or "").strip() or None
    profiles = raw.get("profiles_path")
    profiles_path = str(profiles).strip() if profiles else None
    return ProfileRecord(
        name=name,
        api_url=api_url.rstrip("/"),
        organization_id=str(raw.get("organization_id") or "").strip(),
        organization_name=str(raw.get("organization_name") or "").strip(),
        project_id=str(raw.get("project_id") or "").strip(),
        project_name=str(raw.get("project_name") or "").strip(),
        dbt_target=target,
        profiles_path=profiles_path or None,
        credential_ref=str(raw.get("credential_ref") or name).strip() or name,
    )


def load_registry(*, path: Path | None = None) -> ProfileRegistry:
    registry_path = path or default_registry_path()
    if not registry_path.is_file():
        return ProfileRegistry(path=registry_path)
    loaded = yaml.safe_load(registry_path.read_text()) or {}
    if not isinstance(loaded, dict):
        return ProfileRegistry(path=registry_path)
    profiles: dict[str, ProfileRecord] = {}
    raw_profiles = loaded.get("profiles") or {}
    if isinstance(raw_profiles, dict):
        for name, spec in raw_profiles.items():
            try:
                validated = validate_profile_name(str(name))
            except InstallError:
                continue
            record = _record_from_mapping(validated, spec)
            if record:
                profiles[validated] = record
    return ProfileRegistry(
        version=int(loaded.get("version") or REGISTRY_VERSION),
        profiles=profiles,
        path=registry_path,
    )


def save_registry(registry: ProfileRegistry, *, path: Path | None = None) -> Path:
    registry_path = path or registry.path or default_registry_path()
    payload = {
        "version": REGISTRY_VERSION,
        "profiles": {name: record.to_dict() for name, record in registry.profiles.items()},
    }
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = yaml.safe_dump(payload, sort_keys=False)
    handle, tmp_name = tempfile.mkstemp(
        dir=str(registry_path.parent),
        prefix=".profiles.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp_name, registry_path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return registry_path


def get_profile(name: str, *, registry: ProfileRegistry | None = None) -> ProfileRecord:
    validated = validate_profile_name(name)
    current = registry or load_registry()
    record = current.profiles.get(validated)
    if record is None:
        raise profile_error(
            "FRONTIER_PROFILE_NOT_FOUND",
            f"Frontier profile '{validated}' was not found.",
            cause="No named Frontier profile with that name exists in the local registry.",
            next_action=f"Run `frontier profile create {validated} --api-key`.",
        )
    return record


def upsert_profile(record: ProfileRecord, *, force: bool = False) -> ProfileRecord:
    validate_profile_name(record.name)
    registry = load_registry()
    if record.name in registry.profiles and not force:
        raise profile_error(
            "FRONTIER_PROFILE_EXISTS",
            f"Frontier profile '{record.name}' already exists.",
            cause="Creating a profile does not overwrite an existing name by default.",
            next_action=f"Pass `--force` to replace '{record.name}', or choose another name.",
        )
    registry.profiles[record.name] = record
    save_registry(registry)
    return record


def replace_profile(record: ProfileRecord) -> ProfileRecord:
    """Overwrite one profile record without deleting sibling registry entries."""
    return upsert_profile(record, force=True)


def refresh_profile_display_names(
    record: ProfileRecord,
    *,
    organization_name: str,
    project_name: str,
) -> ProfileRecord:
    org_name = (organization_name or "").strip()
    project = (project_name or "").strip()
    if org_name == record.organization_name and project == record.project_name:
        return record
    return replace_profile(
        replace(
            record,
            organization_name=org_name or record.organization_name,
            project_name=project or record.project_name,
        )
    )


def remove_profile_metadata(name: str) -> bool:
    validated = validate_profile_name(name)
    registry = load_registry()
    if validated not in registry.profiles:
        return False
    del registry.profiles[validated]
    save_registry(registry)
    return True


def load_selected_profile_name(project_dir: Path) -> str | None:
    path = state_path(project_dir)
    if not path.is_file():
        return None
    try:
        loaded = yaml.safe_load(path.read_text()) or {}
    except Exception:
        return None
    if not isinstance(loaded, dict):
        return None
    name = str(loaded.get("selected_profile") or "").strip()
    if not name:
        return None
    try:
        return validate_profile_name(name)
    except InstallError:
        return None


def write_selected_profile(project_dir: Path, name: str | None) -> Path:
    path = state_path(project_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    gitignore = internal_gitignore_path(project_dir)
    if not gitignore.is_file() or INTERNAL_GITIGNORE.strip() not in gitignore.read_text():
        existing = gitignore.read_text() if gitignore.is_file() else ""
        if INTERNAL_GITIGNORE.strip() not in existing.splitlines():
            prefix = "" if not existing or existing.endswith("\n") else "\n"
            gitignore.write_text(existing + prefix + INTERNAL_GITIGNORE)
    if not name:
        if path.is_file():
            path.unlink()
        return path
    payload = {"version": STATE_VERSION, "selected_profile": validate_profile_name(name)}
    path.write_text(yaml.safe_dump(payload, sort_keys=False))
    return path


def clear_selected_profile_if_matches(project_dir: Path, name: str) -> None:
    selected = load_selected_profile_name(project_dir)
    if selected == name:
        write_selected_profile(project_dir, None)


def profile_credential_status(name: str) -> str:
    backend = credential_backend(profile_name=name)
    if backend == "keyring":
        return "keychain"
    if backend == "file":
        return "file"
    return "missing"
