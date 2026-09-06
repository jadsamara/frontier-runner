from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from frontier.config import redact
from frontier.onboard.constants import KEYRING_SERVICE

KEYRING_USERNAME = "default"


@dataclass(frozen=True)
class StoredCredentials:
    api_url: str
    api_key: str
    project: str
    organization: str = ""

    def prefix(self) -> str:
        key = self.api_key.strip()
        if len(key) <= 12:
            return "frn_…"
        return f"{key[:12]}…"

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


def key_prefix(api_key: str) -> str:
    key = api_key.strip()
    if len(key) <= 12:
        return "frn_…"
    return f"{key[:12]}…"


def _load_keyring() -> Any | None:
    try:
        import keyring
    except Exception:
        return None
    return keyring


def _set_keyring(payload: str) -> bool:
    module = _load_keyring()
    if module is None:
        return False
    try:
        module.set_password(KEYRING_SERVICE, KEYRING_USERNAME, payload)
        return True
    except Exception:
        return False


def _get_keyring() -> str | None:
    module = _load_keyring()
    if module is None:
        return None
    try:
        return module.get_password(KEYRING_SERVICE, KEYRING_USERNAME)
    except Exception:
        return None


def _delete_keyring() -> None:
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


def save_credentials(
    creds: StoredCredentials,
    *,
    fallback_path: Path | None = None,
) -> str:
    """Store the project API key. Returns 'keyring' or 'file'."""
    encoded = json.dumps(creds.to_payload())
    if _set_keyring(encoded):
        return "keyring"
    path = fallback_path or default_fallback_path()
    _write_fallback(path, encoded)
    return "file"


def load_credentials(*, fallback_path: Path | None = None) -> StoredCredentials | None:
    raw = _get_keyring()
    if not raw:
        path = fallback_path or default_fallback_path()
        if path.is_file():
            raw = path.read_text()
    if not raw:
        env_key = (os.environ.get("FRONTIER_API_KEY") or "").strip()
        env_url = (os.environ.get("FRONTIER_API_URL") or "").strip()
        env_project = (os.environ.get("FRONTIER_PROJECT") or "").strip()
        if env_key and env_url:
            return StoredCredentials(
                api_url=env_url,
                api_key=env_key,
                project=env_project,
            )
        return None
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
    )


def delete_credentials(*, fallback_path: Path | None = None) -> None:
    _delete_keyring()
    path = fallback_path or default_fallback_path()
    if path.is_file():
        path.unlink()


def redact_credentials_dict(value: dict[str, Any]) -> dict[str, Any]:
    return redact(value)
