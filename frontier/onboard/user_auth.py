from __future__ import annotations

import base64
import hashlib
import secrets
import time
import webbrowser
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from frontier.credentials import (
    StoredUserAuthorization,
    delete_user_authorization,
    load_user_authorization,
    save_user_authorization,
    user_authorization_backend,
)
from frontier.errors import InstallError
from frontier.onboard.constants import DEFAULT_API_URL
from frontier.onboard.saas import (
    fetch_cli_user,
    poll_cli_device_token,
    revoke_cli_user_token,
    start_cli_device,
)


def pkce_s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def require_user_authorization(api_url: str) -> StoredUserAuthorization:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    stored = load_user_authorization(origin)
    if stored is None:
        raise InstallError(
            "USER_AUTH_REQUIRED",
            "Frontier user login is required for this command.",
            cause="No short-lived CLI user authorization is stored.",
            next_action="Run `frontier login`, then retry.",
            docs_path="/docs/authenticate",
        )
    try:
        identity = fetch_cli_user(origin, stored.access_token)
    except InstallError as error:
        if error.code in {"USER_AUTH_EXPIRED", "USER_AUTH_REQUIRED", "AUTH_INVALID"}:
            raise InstallError(
                "USER_AUTH_EXPIRED",
                "Frontier user authorization expired.",
                cause=error.cause,
                next_action="Run `frontier login`.",
                docs_path="/docs/authenticate",
            ) from error
        raise
    return StoredUserAuthorization(
        api_url=origin,
        access_token=stored.access_token,
        email=identity.email or stored.email,
        user_id=identity.user_id or stored.user_id,
        expires_at=identity.expires_at or stored.expires_at,
        source=stored.source,
        issuer=identity.issuer or stored.issuer,
        audience=stored.audience,
    )


def complete_user_login(
    args: Any,
    *,
    sleeper: Callable[[float], None] | None = None,
    opener: Callable[[str], bool] | None = None,
    clock: Callable[[], float] | None = None,
) -> int:
    api_url = (getattr(args, "api_url", None) or DEFAULT_API_URL).rstrip("/")
    verifier = secrets.token_urlsafe(32)
    state = secrets.token_urlsafe(32)
    started = start_cli_device(
        api_url,
        code_challenge=pkce_s256(verifier),
        state=state,
    )
    print("Authorize Frontier CLI in the browser.")
    print(f"Open {started.verification_uri_complete or started.verification_uri}")
    print(f"Code: {started.user_code}")
    open_url = started.verification_uri_complete or started.verification_uri
    try:
        (opener or webbrowser.open)(open_url)
    except Exception:
        pass
    deadline = (clock or time.time)() + max(1, started.expires_in)
    interval = max(1, started.interval)
    sleep = sleeper or time.sleep
    while (clock or time.time)() < deadline:
        token_body = poll_cli_device_token(
            api_url,
            device_code=started.device_code,
            code_verifier=verifier,
            state=state,
        )
        if token_body is None:
            sleep(interval)
            continue
        access_token = str(token_body.get("accessToken") or "")
        if not access_token:
            raise InstallError(
                "USER_AUTH_EXPIRED",
                "Frontier did not return a user authorization.",
                cause="The token response omitted accessToken.",
                next_action="Retry `frontier login`.",
            )
        expires_in = int(token_body.get("expiresIn") or 0)
        expires_at = (
            datetime.now(tz=timezone.utc) + timedelta(seconds=max(1, expires_in))
        ).isoformat()
        user = token_body.get("user") if isinstance(token_body.get("user"), dict) else {}
        auth = StoredUserAuthorization(
            api_url=api_url,
            access_token=access_token,
            email=str(user.get("email") or ""),
            user_id=str(user.get("id") or ""),
            expires_at=expires_at,
            issuer=str(token_body.get("issuer") or api_url),
            audience=str(token_body.get("audience") or "frontier-cli"),
        )
        backend = save_user_authorization(auth)
        print("Signed in as a Frontier user")
        if auth.email:
            print(f"User: {auth.email}")
        print("This authorization can create projects. It cannot run assessments.")
        print("Project API keys remain the credential for prove/upload/CI.")
        if backend == "file":
            from frontier.credentials import default_fallback_path

            print(
                "Warning: secure keychain storage is unavailable. "
                f"User authorization was stored in {default_fallback_path()} with permission 0600.",
            )
        return 0
    raise InstallError(
        "USER_AUTH_DEVICE_CODE_EXPIRED",
        "The CLI login code expired before it was authorized.",
        cause="The browser authorization was not completed in time.",
        next_action="Retry `frontier login` and authorize the request promptly.",
        docs_path="/docs/authenticate",
    )


def cmd_auth_user_status(args: Any) -> int:
    api_url = (getattr(args, "api_url", None) or DEFAULT_API_URL).rstrip("/")
    try:
        auth = require_user_authorization(api_url)
    except InstallError as error:
        print("User: not authenticated")
        print(f"Next: frontier login")
        if error.code != "USER_AUTH_REQUIRED":
            print(error.format())
        return 1
    backend = user_authorization_backend(api_url) or auth.source
    print(f"User: {auth.email or auth.user_id or 'authenticated'}")
    print(f"Expires: {auth.expires_at}")
    print(f"Credential: stored in {'keychain' if backend == 'keyring' else backend}")
    identity = fetch_cli_user(api_url, auth.access_token)
    if identity.organizations:
        print("Organizations:")
        for org in identity.organizations:
            print(f"  {org['name']} ({org['id']})  {org['role']}")
    return 0


def cmd_auth_user_logout(args: Any) -> int:
    api_url = (getattr(args, "api_url", None) or DEFAULT_API_URL).rstrip("/")
    stored = load_user_authorization(api_url)
    if stored:
        try:
            revoke_cli_user_token(api_url, stored.access_token)
        except Exception:
            pass
    delete_user_authorization(api_url)
    print("Signed out the Frontier user authorization.")
    print("Project API keys were not removed.")
    return 0
