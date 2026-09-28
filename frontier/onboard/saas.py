from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urljoin, urlparse, urlunparse

from frontier.credentials import StoredCredentials, key_prefix
from frontier.errors import InstallError
from frontier.onboard.constants import DEFAULT_API_URL


@dataclass(frozen=True)
class WhoAmI:
    organization: str
    project: str
    api_key_prefix: str
    api_url: str
    organization_id: str = ""
    project_id: str = ""


@dataclass(frozen=True)
class RunnerVersions:
    minimum_supported: str
    latest_stable: str


@dataclass(frozen=True)
class DraftManifestResult:
    version: int
    status: str
    review_url: str
    generated: bool = False
    sql_change_ready: bool = False
    cdc_ready: bool = False
    created: bool = True
    fingerprint: str = ""


BIND_HOSTS = {"0.0.0.0", "::", "[::]"}
LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1"}


def public_origin(configured: str) -> str:
    return (configured or DEFAULT_API_URL).rstrip("/")


def rewrite_user_facing_url(url: str, configured_origin: str) -> str:
    """Replace Cloud Run bind hosts with the configured public SaaS origin."""
    configured = public_origin(configured_origin)
    if not url:
        return configured
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    cfg = urlparse(configured)
    cfg_host = (cfg.hostname or "").lower()
    if host in BIND_HOSTS or (host in LOOPBACK_HOSTS and cfg_host not in LOOPBACK_HOSTS):
        return urlunparse(
            (cfg.scheme, cfg.netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
        )
    return url


def _request(
    *,
    method: str,
    url: str,
    api_key: str | None = None,
    payload: dict[str, Any] | None = None,
    extra_headers: dict[str, str] | None = None,
    timeout_seconds: int = 30,
) -> tuple[int, dict[str, Any]]:
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    if extra_headers:
        headers.update(extra_headers)
    request = urllib.request.Request(url, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            body = json.loads(raw) if raw else {}
            return int(response.status), body if isinstance(body, dict) else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(detail) if detail else {}
        except json.JSONDecodeError:
            body = {"error": detail}
        if not isinstance(body, dict):
            body = {"error": detail}
        return int(error.code), body
    except urllib.error.URLError as error:
        raise InstallError(
            "SAAS_UNREACHABLE",
            "Frontier SaaS is not reachable.",
            cause=str(error.reason) if getattr(error, "reason", None) else "network error",
            next_action="Check FRONTIER_API_URL and your network, then retry.",
            docs_path="/docs/troubleshooting#saas-unreachable",
        ) from error


def whoami(api_url: str, api_key: str) -> WhoAmI:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="GET",
        url=urljoin(origin + "/", "api/v1/auth/whoami"),
        api_key=api_key,
    )
    if status in {401, 403}:
        raise InstallError(
            "AUTH_INVALID",
            "The project API key is invalid or revoked.",
            cause="Frontier rejected the stored credentials.",
            next_action="Run `frontier login --api-key` with a current project key.",
            docs_path="/docs/troubleshooting#auth-invalid",
        )
    if status != 200:
        raise InstallError(
            "AUTH_INVALID",
            "Could not validate the project API key.",
            cause=str(body.get("error") or f"HTTP {status}"),
            next_action="Confirm the API URL and key, then retry `frontier login --api-key`.",
            docs_path="/docs/troubleshooting#auth-invalid",
        )
    project = str(body.get("projectName") or body.get("project") or "").strip()
    organization = str(body.get("organizationName") or body.get("organization") or "").strip()
    if not project:
        raise InstallError(
            "AUTH_INVALID",
            "The API key is valid but is not bound to a project.",
            cause="whoami did not return a project name.",
            next_action="Create a project in Frontier, then generate a new API key.",
            docs_path="/docs/quick-start",
        )
    prefix = str(body.get("apiKeyPrefix") or key_prefix(api_key))
    return WhoAmI(
        organization=organization,
        project=project,
        api_key_prefix=prefix,
        api_url=origin,
        organization_id=str(body.get("organizationId") or "").strip(),
        project_id=str(body.get("projectId") or "").strip(),
    )


def fetch_runner_versions(api_url: str) -> RunnerVersions:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="GET",
        url=urljoin(origin + "/", "api/v1/runner/versions"),
    )
    if status != 200:
        raise InstallError(
            "SAAS_UNREACHABLE",
            "Could not read supported runner versions.",
            cause=str(body.get("error") or f"HTTP {status}"),
            next_action="Retry `frontier update-check` after confirming the API URL.",
            docs_path="/docs/troubleshooting#saas-unreachable",
        )
    return RunnerVersions(
        minimum_supported=str(body.get("minimumSupported") or "0.1.1"),
        latest_stable=str(body.get("latestStable") or "0.2.4"),
    )


def saas_reachable(api_url: str) -> bool:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    try:
        status, _body = _request(
            method="GET",
            url=urljoin(origin + "/", "api/health"),
            timeout_seconds=3,
        )
    except InstallError:
        return False
    return status < 500


def upload_draft_manifest(
    creds: StoredCredentials,
    document: dict[str, Any],
) -> DraftManifestResult:
    origin = creds.api_url.rstrip("/")
    payload_document = json.loads(json.dumps(document, ensure_ascii=False))
    if isinstance(payload_document, dict):
        payload_document.pop("generationKind", None)
    from frontier.semantic import semantic_fingerprint

    fingerprint = semantic_fingerprint(payload_document)
    status, body = _request(
        method="POST",
        url=urljoin(
            origin + "/",
            f"api/v1/projects/{quote(creds.project, safe='')}/manifests",
        ),
        api_key=creds.api_key,
        payload={"document": payload_document, "changedBy": "cli", "generated": True},
        extra_headers={"Idempotency-Key": fingerprint},
    )
    if status in {401, 403}:
        raise InstallError(
            "AUTH_INVALID",
            "Could not upload a draft semantic manifest.",
            cause="Frontier rejected the project API key.",
            next_action="Run `frontier login --api-key`.",
            docs_path="/docs/troubleshooting#auth-invalid",
        )
    if status == 404:
        raise InstallError(
            "PROJECT_NOT_FOUND",
            "No matching Frontier project was found.",
            cause=str(body.get("error") or "Not found"),
            next_action="Confirm `.frontier/config.yml` project matches the SaaS project name.",
            docs_path="/docs/troubleshooting",
        )
    if status not in {200, 201}:
        raise InstallError(
            "MANIFEST_DRAFT_FAILED",
            "Frontier rejected the inferred semantic manifest.",
            cause=str(body.get("error") or f"HTTP {status}"),
            next_action="Review the suggested mapping and retry `frontier discover`.",
            docs_path="/docs/semantic-manifest",
        )
    version = int(body.get("version") or 1)
    origin = public_origin(creds.api_url)
    review = str(body.get("reviewUrl") or f"{origin}/manifests?version={version}")
    created = bool(body.get("created")) if "created" in body else status == 201
    return DraftManifestResult(
        version=version,
        status=str(body.get("status") or "draft"),
        review_url=rewrite_user_facing_url(review, origin),
        generated=str(body.get("status") or "") == "active" or bool(body.get("generated")),
        sql_change_ready=bool(body.get("sqlChangeReady", True)),
        cdc_ready=bool(body.get("cdcReady", False)),
        created=created,
        fingerprint=str(body.get("fingerprint") or fingerprint),
    )


def fetch_active_manifest_summary(creds: StoredCredentials) -> dict[str, Any] | None:
    origin = creds.api_url.rstrip("/")
    status, body = _request(
        method="GET",
        url=urljoin(
            origin + "/",
            f"api/v1/projects/{quote(creds.project, safe='')}/manifests/active",
        ),
        api_key=creds.api_key,
    )
    if status == 404:
        return None
    if status in {401, 403}:
        raise InstallError(
            "AUTH_INVALID",
            "Could not read the active semantic manifest.",
            cause="Frontier rejected the project API key.",
            next_action="Run `frontier login --api-key`.",
            docs_path="/docs/troubleshooting#auth-invalid",
        )
    if status != 200:
        raise InstallError(
            "SAAS_UNREACHABLE",
            "Could not read the active semantic manifest.",
            cause=str(body.get("error") or f"HTTP {status}"),
            next_action="Retry after confirming Frontier is reachable.",
            docs_path="/docs/troubleshooting#saas-unreachable",
        )
    return body


@dataclass(frozen=True)
class DeviceStart:
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int
    interval: int
    issuer: str
    audience: str


@dataclass(frozen=True)
class UserWhoAmI:
    user_id: str
    email: str
    expires_at: str
    issuer: str
    organizations: tuple[dict[str, str], ...]


@dataclass(frozen=True)
class CreatedProject:
    id: str
    name: str
    organization_id: str
    organization_name: str
    organization_slug: str
    warehouse_type: str
    created: bool


@dataclass(frozen=True)
class IssuedProjectKey:
    id: str
    project_id: str
    project_name: str
    organization_id: str
    organization_name: str
    key_prefix: str
    api_key: str


def _cli_error(
    status: int,
    body: dict[str, Any],
    *,
    fallback: str,
    explanation: str,
    next_action: str,
) -> InstallError:
    code = str(body.get("code") or fallback)
    return InstallError(
        code,
        explanation,
        cause=str(body.get("error") or f"HTTP {status}"),
        next_action=next_action,
        docs_path="/docs/troubleshooting",
    )


def start_cli_device(api_url: str, *, code_challenge: str, state: str) -> DeviceStart:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="POST",
        url=urljoin(origin + "/", "api/v1/cli/device/start"),
        payload={
            "codeChallenge": code_challenge,
            "codeChallengeMethod": "S256",
            "state": state,
        },
    )
    if status != 200:
        raise _cli_error(
            status,
            body,
            fallback="USER_AUTH_EXPIRED",
            explanation="Could not start Frontier CLI login.",
            next_action="Retry `frontier login` after confirming the API URL.",
        )
    return DeviceStart(
        device_code=str(body.get("deviceCode") or ""),
        user_code=str(body.get("userCode") or ""),
        verification_uri=str(body.get("verificationUri") or ""),
        verification_uri_complete=str(body.get("verificationUriComplete") or ""),
        expires_in=int(body.get("expiresIn") or 0),
        interval=int(body.get("interval") or 5),
        issuer=str(body.get("issuer") or origin),
        audience=str(body.get("audience") or "frontier-cli"),
    )


def poll_cli_device_token(
    api_url: str,
    *,
    device_code: str,
    code_verifier: str,
    state: str,
) -> dict[str, Any] | None:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="POST",
        url=urljoin(origin + "/", "api/v1/cli/device/token"),
        payload={
            "deviceCode": device_code,
            "codeVerifier": code_verifier,
            "state": state,
        },
    )
    if status == 400 and body.get("error") == "authorization_pending":
        return None
    if status != 200:
        raise _cli_error(
            status,
            body,
            fallback="USER_AUTH_EXPIRED",
            explanation="Frontier CLI login did not complete.",
            next_action="Retry `frontier login` and authorize the request in the browser.",
        )
    return body


def fetch_cli_user(api_url: str, access_token: str) -> UserWhoAmI:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="GET",
        url=urljoin(origin + "/", "api/v1/auth/user"),
        api_key=access_token,
    )
    if status in {401, 403}:
        raise _cli_error(
            status,
            body,
            fallback="USER_AUTH_EXPIRED",
            explanation="Frontier user authorization is missing or expired.",
            next_action="Run `frontier login`.",
        )
    if status != 200:
        raise _cli_error(
            status,
            body,
            fallback="USER_AUTH_EXPIRED",
            explanation="Could not read Frontier user authorization.",
            next_action="Run `frontier login`.",
        )
    orgs = body.get("organizations") if isinstance(body.get("organizations"), list) else []
    return UserWhoAmI(
        user_id=str(body.get("userId") or ""),
        email=str(body.get("email") or ""),
        expires_at=str(body.get("expiresAt") or ""),
        issuer=str(body.get("issuer") or origin),
        organizations=tuple(
            {
                "id": str(item.get("id") or ""),
                "name": str(item.get("name") or ""),
                "slug": str(item.get("slug") or ""),
                "role": str(item.get("role") or ""),
            }
            for item in orgs
            if isinstance(item, dict)
        ),
    )


def list_cli_organizations(api_url: str, access_token: str) -> list[dict[str, str]]:
    return list(fetch_cli_user(api_url, access_token).organizations)


def list_cli_projects(api_url: str, access_token: str) -> list[dict[str, str]]:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="GET",
        url=urljoin(origin + "/", "api/v1/projects"),
        api_key=access_token,
    )
    if status != 200:
        raise _cli_error(
            status,
            body,
            fallback="USER_AUTH_EXPIRED",
            explanation="Could not list Frontier projects.",
            next_action="Run `frontier login`, then retry.",
        )
    rows = body.get("projects") if isinstance(body.get("projects"), list) else []
    return [
        {
            "id": str(item.get("id") or ""),
            "name": str(item.get("name") or ""),
            "organizationId": str(item.get("organizationId") or ""),
            "organizationName": str(item.get("organizationName") or ""),
            "organizationSlug": str(item.get("organizationSlug") or ""),
            "role": str(item.get("role") or ""),
        }
        for item in rows
        if isinstance(item, dict)
    ]


def get_cli_project(api_url: str, access_token: str, project: str) -> dict[str, str]:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    status, body = _request(
        method="GET",
        url=urljoin(origin + "/", f"api/v1/projects/{quote(project, safe='')}"),
        api_key=access_token,
    )
    if status != 200:
        raise _cli_error(
            status,
            body,
            fallback="ORGANIZATION_NOT_FOUND",
            explanation="Could not read that Frontier project.",
            next_action="Run `frontier project list`.",
        )
    return {
        "id": str(body.get("id") or ""),
        "name": str(body.get("name") or ""),
        "organizationId": str(body.get("organizationId") or ""),
        "organizationName": str(body.get("organizationName") or ""),
        "organizationSlug": str(body.get("organizationSlug") or ""),
        "role": str(body.get("role") or ""),
        "warehouseType": str(body.get("warehouseType") or ""),
    }


def create_cli_project(
    api_url: str,
    access_token: str,
    *,
    name: str,
    organization: str | None,
    warehouse_type: str | None,
    idempotency_key: str,
) -> CreatedProject:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    payload: dict[str, Any] = {"name": name}
    if organization:
        payload["organization"] = organization
    if warehouse_type:
        payload["warehouseType"] = warehouse_type
    status, body = _request(
        method="POST",
        url=urljoin(origin + "/", "api/v1/projects"),
        api_key=access_token,
        payload=payload,
        extra_headers={"Idempotency-Key": idempotency_key},
    )
    if status not in {200, 201}:
        raise _cli_error(
            status,
            body,
            fallback="PROJECT_CREATE_FAILED",
            explanation="Could not create the Frontier project.",
            next_action="Confirm your organization role, then retry.",
        )
    return CreatedProject(
        id=str(body.get("id") or ""),
        name=str(body.get("name") or name),
        organization_id=str(body.get("organizationId") or ""),
        organization_name=str(body.get("organizationName") or ""),
        organization_slug=str(body.get("organizationSlug") or ""),
        warehouse_type=str(body.get("warehouseType") or "snowflake"),
        created=bool(body.get("created", status == 201)),
    )


def issue_cli_project_key(
    api_url: str,
    access_token: str,
    project: str,
    *,
    key_name: str | None = None,
) -> IssuedProjectKey:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    payload: dict[str, Any] = {}
    if key_name:
        payload["name"] = key_name
    status, body = _request(
        method="POST",
        url=urljoin(origin + "/", f"api/v1/projects/{quote(project, safe='')}/api-keys"),
        api_key=access_token,
        payload=payload,
    )
    if status not in {200, 201}:
        raise _cli_error(
            status,
            body,
            fallback="PROJECT_KEY_ISSUE_FAILED",
            explanation="The project exists, but Frontier could not issue a project API key.",
            next_action="Run `frontier project key create PROJECT --profile NAME`.",
        )
    api_key = str(body.get("apiKey") or "")
    if not api_key:
        raise InstallError(
            "PROJECT_KEY_ISSUE_FAILED",
            "The project exists, but Frontier did not return a project API key.",
            cause="The key issuance response omitted apiKey.",
            next_action="Run `frontier project key create PROJECT --profile NAME`.",
        )
    return IssuedProjectKey(
        id=str(body.get("id") or ""),
        project_id=str(body.get("projectId") or ""),
        project_name=str(body.get("projectName") or ""),
        organization_id=str(body.get("organizationId") or ""),
        organization_name=str(body.get("organizationName") or ""),
        key_prefix=str(body.get("keyPrefix") or key_prefix(api_key)),
        api_key=api_key,
    )


def revoke_cli_user_token(api_url: str, access_token: str) -> None:
    origin = api_url.rstrip("/") or DEFAULT_API_URL
    _request(
        method="POST",
        url=urljoin(origin + "/", "api/v1/cli/logout"),
        api_key=access_token,
    )
