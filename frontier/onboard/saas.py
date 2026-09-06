from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urljoin

from frontier.credentials import StoredCredentials, key_prefix
from frontier.errors import InstallError
from frontier.onboard.constants import DEFAULT_API_URL


@dataclass(frozen=True)
class WhoAmI:
    organization: str
    project: str
    api_key_prefix: str
    api_url: str


@dataclass(frozen=True)
class RunnerVersions:
    minimum_supported: str
    latest_stable: str


@dataclass(frozen=True)
class DraftManifestResult:
    version: int
    status: str
    review_url: str


def _request(
    *,
    method: str,
    url: str,
    api_key: str | None = None,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = 30,
) -> tuple[int, dict[str, Any]]:
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
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
    project = str(body.get("project") or "").strip()
    organization = str(body.get("organization") or "").strip()
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
        latest_stable=str(body.get("latestStable") or "0.1.1"),
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
    status, body = _request(
        method="POST",
        url=urljoin(
            origin + "/",
            f"api/v1/projects/{quote(creds.project, safe='')}/manifests",
        ),
        api_key=creds.api_key,
        payload={"document": document, "changedBy": "cli"},
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
    review = str(body.get("reviewUrl") or f"{origin}/manifests?version={version}")
    return DraftManifestResult(
        version=version,
        status=str(body.get("status") or "draft"),
        review_url=review,
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
