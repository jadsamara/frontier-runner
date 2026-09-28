from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from frontier.cli import main
from frontier.context import in_recognized_ci
from frontier.credentials import (
    StoredCredentials,
    load_stored_credentials,
    load_user_authorization,
    save_credentials,
    save_user_authorization,
    StoredUserAuthorization,
)
from frontier.errors import InstallError
from frontier.onboard.project_commands import create_project_with_optional_profile
from frontier.onboard.user_auth import complete_user_login, pkce_s256
from frontier.profiles import get_profile, load_selected_profile_name


def _user_auth(api_url: str) -> StoredUserAuthorization:
    return StoredUserAuthorization(
        api_url=api_url,
        access_token="fru_testtokenxxxxxxxxxxxx",
        email="owner@acme.test",
        user_id="user-1",
        expires_at="9999-01-01T00:00:00+00:00",
        issuer=api_url,
        audience="frontier-cli",
    )


class FakeSaas(BaseHTTPRequestHandler):
    store: dict

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _send(self, status: int, body: dict) -> None:
        payload = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:  # noqa: N802
        body = self._read_json()
        if self.path.endswith("/api/v1/cli/device/start"):
            self.store["challenge"] = body.get("codeChallenge")
            self.store["state"] = body.get("state")
            self._send(
                200,
                {
                    "deviceCode": "device-code-1",
                    "userCode": "ABCD-EFGH",
                    "verificationUri": "http://example/cli/authorize",
                    "verificationUriComplete": "http://example/cli/authorize?user_code=ABCD-EFGH",
                    "expiresIn": 60,
                    "interval": 1,
                    "issuer": "http://127.0.0.1",
                    "audience": "frontier-cli",
                },
            )
            return
        if self.path.endswith("/api/v1/cli/device/token"):
            if body.get("state") != self.store.get("state"):
                self._send(401, {"error": "mismatch", "code": "USER_AUTH_STATE_MISMATCH"})
                return
            if self.store.get("consumed"):
                self._send(401, {"error": "used", "code": "USER_AUTH_EXPIRED"})
                return
            self.store["consumed"] = True
            self._send(
                200,
                {
                    "accessToken": "fru_issuedtokenxxxxxxxxxx",
                    "tokenType": "Bearer",
                    "expiresIn": 3600,
                    "issuer": "http://127.0.0.1",
                    "audience": "frontier-cli",
                    "user": {"id": "user-1", "email": "owner@acme.test"},
                },
            )
            return
        if "/api-keys" in self.path:
            auth = self.headers.get("Authorization") or ""
            if auth.startswith("Bearer frn_"):
                self._send(403, {"error": "forbidden", "code": "ORGANIZATION_PROJECT_CREATE_FORBIDDEN"})
                return
            self._send(
                201,
                {
                    "id": "key-1",
                    "projectId": "project-456",
                    "projectName": "frontier_benchmark",
                    "organizationId": "org-123",
                    "organizationName": "Test Org",
                    "keyPrefix": "frn_secretkey1",
                    "apiKey": "frn_secretkey1notforlogs",
                },
            )
            return
        if self.path.endswith("/api/v1/projects"):
            auth = self.headers.get("Authorization") or ""
            if auth.startswith("Bearer frn_"):
                self._send(403, {"error": "forbidden", "code": "ORGANIZATION_PROJECT_CREATE_FORBIDDEN"})
                return
            if self.store.get("created"):
                self._send(
                    200,
                    {
                        "id": "project-456",
                        "name": "frontier_benchmark",
                        "organizationId": "org-123",
                        "organizationName": "Test Org",
                        "organizationSlug": "test-org",
                        "warehouseType": "snowflake",
                        "created": False,
                    },
                )
                return
            self.store["created"] = True
            self._send(
                201,
                {
                    "id": "project-456",
                    "name": body.get("name") or "frontier_benchmark",
                    "organizationId": "org-123",
                    "organizationName": "Test Org",
                    "organizationSlug": "test-org",
                    "warehouseType": "snowflake",
                    "created": True,
                },
            )
            return
        self._send(404, {"error": "missing"})

    def do_GET(self) -> None:  # noqa: N802
        if self.path.endswith("/api/v1/auth/user"):
            self._send(
                200,
                {
                    "userId": "user-1",
                    "email": "owner@acme.test",
                    "expiresAt": "9999-01-01T00:00:00.000Z",
                    "issuer": "http://127.0.0.1",
                    "audience": "frontier-cli",
                    "organizations": [
                        {
                            "id": "org-123",
                            "name": "Test Org",
                            "slug": "test-org",
                            "role": "owner",
                        }
                    ],
                },
            )
            return
        if self.path.endswith("/api/v1/projects"):
            self._send(
                200,
                {
                    "projects": [
                        {
                            "id": "project-123",
                            "name": "test_frontier",
                            "organizationId": "org-123",
                            "organizationName": "Test Org",
                            "organizationSlug": "test-org",
                            "role": "owner",
                            "warehouseType": "snowflake",
                        },
                        {
                            "id": "project-456",
                            "name": "frontier_benchmark",
                            "organizationId": "org-123",
                            "organizationName": "Test Org",
                            "organizationSlug": "test-org",
                            "role": "owner",
                            "warehouseType": "snowflake",
                        },
                    ]
                },
            )
            return
        if self.path.endswith("/api/v1/auth/whoami"):
            self._send(
                200,
                {
                    "organizationId": "org-123",
                    "organizationName": "Test Org",
                    "projectId": "project-456",
                    "projectName": "frontier_benchmark",
                    "apiKeyPrefix": "frn_secretkey1…",
                },
            )
            return
        self._send(404, {"error": "missing"})


@pytest.fixture
def fake_saas(monkeypatch):
    store: dict = {}
    handler = type("Handler", (FakeSaas,), {"store": store})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = __import__("threading").Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"http://127.0.0.1:{server.server_address[1]}"
    yield origin, store
    server.shutdown()
    server.server_close()


def test_pkce_s256_is_urlsafe() -> None:
    challenge = pkce_s256("a" * 43)
    assert "=" not in challenge
    assert len(challenge) == 43


def test_browser_login_stores_user_token_not_project_key(fake_saas, capsys) -> None:
    origin, store = fake_saas
    args = SimpleNamespace(api_url=origin, _sleep=lambda _s: None, _open_browser=lambda _url: True, _clock=lambda: 0)
    # clock always 0 and expires_in 60 so loop runs; first poll returns token
    store["clock_base"] = 0
    assert complete_user_login(args, sleeper=lambda _s: None, opener=lambda _url: True, clock=lambda: 0) == 0
    out = capsys.readouterr().out
    assert "Signed in as a Frontier user" in out
    assert "fru_issuedtokenxxxxxxxxxx" not in out
    stored = load_user_authorization(origin)
    assert stored is not None
    assert stored.access_token == "fru_issuedtokenxxxxxxxxxx"
    assert load_stored_credentials() is None


def test_project_create_stores_key_in_named_profile(fake_saas, tmp_path: Path, capsys) -> None:
    origin, _store = fake_saas
    save_user_authorization(_user_auth(origin))
    args = SimpleNamespace(
        api_url=origin,
        project_name="frontier_benchmark",
        profile_name="benchmark",
        organization="org-123",
        target="benchmark",
        profiles=None,
        warehouse=None,
        no_profile=False,
        use=True,
        force=False,
        non_interactive=True,
        project_dir=str(tmp_path),
        create_project=None,
        name=None,
    )
    (tmp_path / "dbt_project.yml").write_text("name: frontier_benchmark\nprofile: jaffle_shop\n")
    assert create_project_with_optional_profile(args) == 0
    out = capsys.readouterr().out
    assert "frn_secretkey1notforlogs" not in out
    assert "Frontier profile: benchmark" in out
    record = get_profile("benchmark")
    assert record is not None
    assert record.organization_id == "org-123"
    assert record.project_id == "project-456"
    assert "apiKey" not in json.dumps(record.__dict__)
    creds = load_stored_credentials(profile_name="benchmark")
    assert creds is not None
    assert creds.api_key == "frn_secretkey1notforlogs"
    assert load_selected_profile_name(tmp_path) == "benchmark"


def test_profile_create_create_project_uses_same_path(fake_saas, tmp_path: Path) -> None:
    origin, _store = fake_saas
    save_user_authorization(_user_auth(origin))
    assert (
        main(
            [
                "profile",
                "create",
                "benchmark",
                "--create-project",
                "frontier_benchmark",
                "--organization",
                "org-123",
                "--target",
                "benchmark",
                "--api-url",
                origin,
                "--non-interactive",
                "--project-dir",
                str(tmp_path),
            ]
        )
        == 0
    )
    record = get_profile("benchmark")
    assert record is not None
    assert record.project_id == "project-456"


def test_project_api_key_cannot_create_project(fake_saas, capsys) -> None:
    origin, _store = fake_saas
    save_credentials(
        StoredCredentials(
            api_url=origin,
            api_key="frn_existingprojectkeyxxxx",
            project="test_frontier",
            organization="Test Org",
            organization_id="org-123",
            project_id="project-123",
        )
    )
    assert (
        main(
            [
                "project",
                "create",
                "frontier_benchmark",
                "--organization",
                "org-123",
                "--api-url",
                origin,
                "--non-interactive",
                "--no-profile",
            ]
        )
        == 1
    )
    err = capsys.readouterr().err
    assert "USER_AUTH_REQUIRED" in err
    assert "frn_existingprojectkeyxxxx" not in err


def test_ci_refuses_project_create(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert in_recognized_ci() is True
    with pytest.raises(InstallError) as error:
        create_project_with_optional_profile(
            SimpleNamespace(non_interactive=False, api_url="https://example", project_name="x")
        )
    assert error.value.code == "USER_AUTH_REQUIRED"


def test_project_list_maps_local_profiles_without_keys(fake_saas, capsys) -> None:
    origin, _store = fake_saas
    save_user_authorization(_user_auth(origin))
    save_credentials(
        StoredCredentials(
            api_url=origin,
            api_key="frn_existingprojectkeyxxxx",
            project="test_frontier",
            organization="Test Org",
            organization_id="org-123",
            project_id="project-123",
        ),
        profile_name="repair",
    )
    from frontier.profiles import ProfileRecord, upsert_profile

    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url=origin,
            organization_id="org-123",
            organization_name="Test Org",
            project_id="project-123",
            project_name="test_frontier",
            credential_ref="repair",
        ),
        force=True,
    )
    assert main(["project", "list", "--api-url", origin]) == 0
    out = capsys.readouterr().out
    assert "test_frontier" in out
    assert "repair" in out
    assert "frn_existingprojectkeyxxxx" not in out


def test_existing_project_key_login_still_works(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        "frontier.onboard.commands.whoami",
        lambda api_url, api_key: __import__(
            "frontier.onboard.saas", fromlist=["WhoAmI"]
        ).WhoAmI("Acme", "jaffle_shop", "frn_abcdefgh…", api_url, "org-1", "proj-1"),
    )
    args = SimpleNamespace(
        api_key=True,
        api_url="https://frontier.example",
        frontier_profile=None,
        _getpass=lambda prompt: "frn_abcdefghijklmnopqrstuvwxyz",
    )
    from frontier.onboard.commands import cmd_login

    assert cmd_login(args) == 0
    assert "abcdefghijklmnopqrstuvwxyz" not in capsys.readouterr().out


def test_keychain_failure_uses_0600_file(fake_saas, tmp_path: Path, monkeypatch) -> None:
    origin, _store = fake_saas
    monkeypatch.setattr("frontier.credentials._set_keyring_entry", lambda *_args, **_kwargs: False)
    save_user_authorization(_user_auth(origin))
    from frontier.credentials import default_fallback_path
    import stat

    path = default_fallback_path()
    assert path.is_file()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "fru_testtokenxxxxxxxxxxxx" in path.read_text()


def test_use_selects_only_current_repository(fake_saas, tmp_path: Path) -> None:
    origin, _store = fake_saas
    save_user_authorization(_user_auth(origin))
    repo_a = tmp_path / "a"
    repo_b = tmp_path / "b"
    repo_a.mkdir()
    repo_b.mkdir()
    (repo_a / "dbt_project.yml").write_text("name: frontier_benchmark\nprofile: jaffle_shop\n")
    args = SimpleNamespace(
        api_url=origin,
        project_name="frontier_benchmark",
        profile_name="benchmark",
        organization="org-123",
        target="benchmark",
        profiles=None,
        warehouse=None,
        no_profile=False,
        use=True,
        force=True,
        non_interactive=True,
        project_dir=str(repo_a),
        create_project=None,
        name=None,
    )
    assert create_project_with_optional_profile(args) == 0
    assert load_selected_profile_name(repo_a) == "benchmark"
    assert load_selected_profile_name(repo_b) is None


def test_non_interactive_multiple_orgs_require_organization(monkeypatch) -> None:
    monkeypatch.setattr(
        "frontier.onboard.project_commands.list_cli_organizations",
        lambda *_args, **_kwargs: [
            {"id": "org-a", "name": "A", "slug": "a", "role": "owner"},
            {"id": "org-b", "name": "B", "slug": "b", "role": "owner"},
        ],
    )
    monkeypatch.setattr(
        "frontier.onboard.project_commands.require_user_authorization",
        lambda api_url: _user_auth(api_url),
    )
    with pytest.raises(InstallError) as error:
        create_project_with_optional_profile(
            SimpleNamespace(
                non_interactive=True,
                api_url="https://example",
                project_name="frontier_benchmark",
                organization=None,
                no_profile=True,
            )
        )
    assert error.value.code == "ORGANIZATION_REQUIRED"
