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


@dataclass(frozen=True)
class StoredCredentials:
    api_url: str
    api_key: str
    project: str
    organization: str = ""
    source: str = SOURCE_KEYRING

    def prefix(self) -> str:
        return key_prefix(self.api_key)

    def to_payload(self) -> dict[str, str]:
        return {
            "apiUrl": self.api_url,
            "apiKey": self.api_key,
            "project": self.project,
            "organization": self.organization,
        }


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


def _set_keyring(payload: str) -> bool:
    isolated = isolated_keyring_path()
    if isolated is not None:
        isolated.parent.mkdir(parents=True, exist_ok=True)
        isolated.write_text(payload)
        isolated.chmod(stat.S_IRUSR | stat.S_IWUSR)
        return True
    module = _load_keyring()
    if module is None:
        return False
    try:
        module.set_password(KEYRING_SERVICE, KEYRING_USERNAME, payload)
        return True
    except Exception:
        return False


def _get_keyring() -> str | None:
    isolated = isolated_keyring_path()
    if isolated is not None:
        if isolated.is_file():
            return isolated.read_text()
        return None
    module = _load_keyring()
    if module is None:
        return None
    try:
        return module.get_password(KEYRING_SERVICE, KEYRING_USERNAME)
    except Exception:
        return None


def _delete_keyring() -> None:
    isolated = isolated_keyring_path()
    if isolated is not None:
        if isolated.is_file():
            isolated.unlink()
        return
    module = _load_keyring()
    if module is None:
        return
    try:
        module.delete_password(KEYRING_SERVICE, KEYRING_USERNAME)
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
    )


def save_credentials(
    creds: StoredCredentials,
    *,
    fallback_path: Path | None = None,
) -> str:
    """Store the project API key. Returns 'keyring' or 'file'."""
    encoded = json.dumps(creds.to_payload())
    if _set_keyring(encoded):
        return SOURCE_KEYRING
    path = fallback_path or default_fallback_path()
    _write_fallback(path, encoded)
    return SOURCE_FILE


def load_stored_credentials(
    *,
    fallback_path: Path | None = None,
) -> StoredCredentials | None:
    """Load credentials from the OS keychain, then the 0600 fallback file."""
    raw = _get_keyring()
    if raw:
        parsed = _parse_payload(raw, source=SOURCE_KEYRING)
        if parsed:
            return parsed
    path = fallback_path or default_fallback_path()
    if path.is_file():
        return _parse_payload(path.read_text(), source=SOURCE_FILE)
    return None


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


def delete_credentials(*, fallback_path: Path | None = None) -> None:
    _delete_keyring()
    path = fallback_path or default_fallback_path()
    if path.is_file():
        path.unlink()


def redact_credentials_dict(value: dict[str, Any]) -> dict[str, Any]:
    return redact(value)
