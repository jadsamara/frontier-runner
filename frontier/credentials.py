from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from frontier.config import ConfigError, redact
from frontier.onboard.constants import KEYRING_SERVICE

KEYRING_USERNAME = "default"
AUTH_REQUIRED_MESSAGE = "AUTH_REQUIRED: Run `frontier login --api-key`."
SOURCE_ENV = "FRONTIER_API_KEY"
SOURCE_KEYRING = "keyring"
SOURCE_FILE = "file"
SOURCE_DEMO = "FRONTIER_DEMO_API_KEY"
PROFILE_KEYRING_PREFIX = "profile:"


@dataclass(frozen=True)
class StoredCredentials:
    api_url: str
    api_key: str
    project: str
    organization: str = ""
    source: str = SOURCE_KEYRING
    organization_id: str = ""
    project_id: str = ""

    def prefix(self) -> str:
        return key_prefix(self.api_key)

    def to_payload(self) -> dict[str, str]:
        payload = {
            "apiUrl": self.api_url,
            "apiKey": self.api_key,
            "project": self.project,
            "organization": self.organization,
        }
        if self.organization_id:
            payload["organizationId"] = self.organization_id
        if self.project_id:
            payload["projectId"] = self.project_id
        return payload


def default_fallback_path() -> Path:
    override = (os.environ.get("FRONTIER_CREDENTIALS_FILE") or "").strip()
    if override:
        return Path(override).expanduser()
    xdg = (os.environ.get("XDG_CONFIG_HOME") or "").strip()
    root = Path(xdg).expanduser() if xdg else Path.home() / ".config"
    return root / "frontier" / "credentials"


def isolated_keyring_path() -> Path | None:
    override = (os.environ.get("FRONTIER_KEYRING_FILE") or "").strip()
    if not override:
        return None
    return Path(override).expanduser()


def keyring_username(profile_name: str | None = None) -> str:
    name = (profile_name or "").strip()
    if not name:
        return KEYRING_USERNAME
    return f"{PROFILE_KEYRING_PREFIX}{name}"


def key_prefix(api_key: str) -> str:
    key = api_key.strip()
    if len(key) <= 12:
        return "frn_…"
    return f"{key[:12]}…"


def is_demo_local_mode() -> bool:
    if (os.environ.get("GITHUB_ACTIONS") or "").strip():
        return False
    flag = (os.environ.get("FRONTIER_ALLOW_LOCAL_MANIFEST") or "").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _load_keyring() -> Any | None:
    if isolated_keyring_path() is not None:
        return None
    try:
        import keyring
    except Exception:
        return None
    return keyring


def _isolated_entries() -> dict[str, str]:
    path = isolated_keyring_path()
    if path is None or not path.is_file():
        return {}
    raw = path.read_text()
    if not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {KEYRING_USERNAME: raw}
    if not isinstance(payload, dict):
        return {KEYRING_USERNAME: raw}
    if payload.get("apiKey"):
        return {KEYRING_USERNAME: raw}
    if int(payload.get("version") or 0) == 1 and isinstance(payload.get("entries"), dict):
        entries: dict[str, str] = {}
        for key, value in payload["entries"].items():
            if isinstance(value, dict):
                entries[str(key)] = json.dumps(value)
            elif value:
                entries[str(key)] = str(value)
        return entries
    return {KEYRING_USERNAME: raw}


def _write_isolated_entries(entries: dict[str, str]) -> None:
    path = isolated_keyring_path()
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if not entries:
        if path.is_file():
            path.unlink()
        return
    path.write_text(json.dumps({"version": 1, "entries": entries}))
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def _set_keyring(payload: str) -> bool:
    return _set_keyring_entry(KEYRING_USERNAME, payload)


def _set_keyring_entry(username: str, payload: str) -> bool:
    isolated = isolated_keyring_path()
    if isolated is not None:
        entries = _isolated_entries()
        entries[username] = payload
        _write_isolated_entries(entries)
        return True
    module = _load_keyring()
    if module is None:
        return False
    try:
        module.set_password(KEYRING_SERVICE, username, payload)
        return True
    except Exception:
        return False


def _get_keyring() -> str | None:
    return _get_keyring_entry(KEYRING_USERNAME)


def _get_keyring_entry(username: str) -> str | None:
    isolated = isolated_keyring_path()
    if isolated is not None:
        return _isolated_entries().get(username)
    module = _load_keyring()
    if module is None:
        return None
    try:
        return module.get_password(KEYRING_SERVICE, username)
    except Exception:
        return None


def _delete_keyring() -> None:
    _delete_keyring_entry(KEYRING_USERNAME)


def _delete_keyring_entry(username: str) -> None:
    isolated = isolated_keyring_path()
    if isolated is not None:
        entries = _isolated_entries()
        entries.pop(username, None)
        _write_isolated_entries(entries)
        return
    module = _load_keyring()
    if module is None:
        return
    try:
        module.delete_password(KEYRING_SERVICE, username)
    except Exception:
        return


def _write_fallback(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(payload)
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def _parse_payload(raw: str, *, source: str) -> StoredCredentials | None:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return _credentials_from_mapping(payload, source=source)


def _credentials_from_mapping(payload: dict[str, Any], *, source: str) -> StoredCredentials | None:
    key = str(payload.get("apiKey") or "").strip()
    url = str(payload.get("apiUrl") or "").strip()
    if not key or not url:
        return None
    return StoredCredentials(
        api_url=url,
        api_key=key,
        project=str(payload.get("project") or "").strip(),
        organization=str(payload.get("organization") or "").strip(),
        source=source,
        organization_id=str(payload.get("organizationId") or "").strip(),
        project_id=str(payload.get("projectId") or "").strip(),
    )


def _read_file_store(path: Path) -> tuple[StoredCredentials | None, dict[str, StoredCredentials]]:
    if not path.is_file():
        return None, {}
    raw = path.read_text()
    parsed = _parse_payload(raw, source=SOURCE_FILE)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return parsed, {}
    if not isinstance(payload, dict):
        return parsed, {}
    if int(payload.get("version") or 0) == 1 and isinstance(payload.get("profiles"), dict):
        named: dict[str, StoredCredentials] = {}
        for name, item in payload["profiles"].items():
            if not isinstance(item, dict):
                continue
            creds = _credentials_from_mapping(item, source=SOURCE_FILE)
            if creds:
                named[str(name)] = creds
        legacy = None
        if isinstance(payload.get("legacy"), dict):
            legacy = _credentials_from_mapping(payload["legacy"], source=SOURCE_FILE)
        return legacy, named
    return parsed, {}


def _write_file_store(
    path: Path,
    *,
    legacy: StoredCredentials | None,
    named: dict[str, StoredCredentials],
) -> None:
    if not named:
        if legacy is None:
            if path.is_file():
                path.unlink()
            return
        _write_fallback(path, json.dumps(legacy.to_payload()))
        return
    payload: dict[str, Any] = {
        "version": 1,
        "profiles": {name: creds.to_payload() for name, creds in named.items()},
    }
    if legacy is not None:
        payload["legacy"] = legacy.to_payload()
    _write_fallback(path, json.dumps(payload))


def save_credentials(
    creds: StoredCredentials,
    *,
    fallback_path: Path | None = None,
    profile_name: str | None = None,
) -> str:
    """Store the project API key. Returns 'keyring' or 'file'."""
    encoded = json.dumps(creds.to_payload())
    username = keyring_username(profile_name)
    stored = (
        _set_keyring_entry(username, encoded)
        if profile_name
        else _set_keyring(encoded)
    )
    if stored:
        return SOURCE_KEYRING
    path = fallback_path or default_fallback_path()
    legacy, named = _read_file_store(path)
    if profile_name:
        named = dict(named)
        named[profile_name] = StoredCredentials(
            api_url=creds.api_url,
            api_key=creds.api_key,
            project=creds.project,
            organization=creds.organization,
            source=SOURCE_FILE,
            organization_id=creds.organization_id,
            project_id=creds.project_id,
        )
        _write_file_store(path, legacy=legacy, named=named)
    else:
        _write_file_store(path, legacy=creds, named=named)
    return SOURCE_FILE


def load_stored_credentials(
    *,
    fallback_path: Path | None = None,
    profile_name: str | None = None,
) -> StoredCredentials | None:
    """Load credentials from the OS keychain, then the 0600 fallback file."""
    username = keyring_username(profile_name)
    raw = _get_keyring_entry(username) if profile_name else _get_keyring()
    if raw:
        parsed = _parse_payload(raw, source=SOURCE_KEYRING)
        if parsed:
            return parsed
    path = fallback_path or default_fallback_path()
    legacy, named = _read_file_store(path)
    if profile_name:
        return named.get(profile_name)
    return legacy


def credential_backend(profile_name: str | None = None) -> str | None:
    """Return keychain/file/None without exposing key material."""
    username = keyring_username(profile_name)
    raw = _get_keyring_entry(username) if profile_name else _get_keyring()
    if raw and _parse_payload(raw, source=SOURCE_KEYRING):
        return SOURCE_KEYRING
    path = default_fallback_path()
    legacy, named = _read_file_store(path)
    if profile_name:
        return SOURCE_FILE if profile_name in named else None
    return SOURCE_FILE if legacy is not None else None


def try_resolve_api_credential(
    *,
    fallback_path: Path | None = None,
) -> StoredCredentials | None:
    """Resolve API credentials without raising.

    Order: FRONTIER_API_KEY, OS keychain, 0600 file, then FRONTIER_DEMO_API_KEY
    only in explicit demo/local mode.
    """
    stored = load_stored_credentials(fallback_path=fallback_path)
    env_key = (os.environ.get("FRONTIER_API_KEY") or "").strip()
    env_url = (os.environ.get("FRONTIER_API_URL") or "").strip()
    env_project = (os.environ.get("FRONTIER_PROJECT") or "").strip()
    if env_key:
        return StoredCredentials(
            api_url=env_url or (stored.api_url if stored else ""),
            api_key=env_key,
            project=env_project or (stored.project if stored else ""),
            organization=stored.organization if stored else "",
            source=SOURCE_ENV,
            organization_id=stored.organization_id if stored else "",
            project_id=stored.project_id if stored else "",
        )
    if stored:
        return stored
    demo = (os.environ.get("FRONTIER_DEMO_API_KEY") or "").strip()
    if demo and is_demo_local_mode():
        return StoredCredentials(
            api_url=env_url,
            api_key=demo,
            project=env_project,
            source=SOURCE_DEMO,
        )
    return None


def resolve_api_credential(
    *,
    fallback_path: Path | None = None,
) -> StoredCredentials:
    creds = try_resolve_api_credential(fallback_path=fallback_path)
    if creds is None:
        raise ConfigError(AUTH_REQUIRED_MESSAGE)
    return creds


def load_credentials(*, fallback_path: Path | None = None) -> StoredCredentials | None:
    """Compatibility wrapper around the shared resolver."""
    return try_resolve_api_credential(fallback_path=fallback_path)


def delete_credentials(
    *,
    fallback_path: Path | None = None,
    profile_name: str | None = None,
) -> None:
    if profile_name:
        _delete_keyring_entry(keyring_username(profile_name))
    else:
        _delete_keyring()
    path = fallback_path or default_fallback_path()
    legacy, named = _read_file_store(path)
    if profile_name:
        named = dict(named)
        named.pop(profile_name, None)
        _write_file_store(path, legacy=legacy, named=named)
        return
    _write_file_store(path, legacy=None, named=named)


def redact_credentials_dict(value: dict[str, Any]) -> dict[str, Any]:
    return redact(value)
