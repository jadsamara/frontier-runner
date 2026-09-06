from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from types import SimpleNamespace

import pytest

from frontier.cli import main
from frontier.credentials import (
    StoredCredentials,
    delete_credentials,
    load_credentials,
    load_stored_credentials,
    save_credentials,
)
from frontier.dbt_artifacts import load_manifest
from frontier.errors import InstallError
from frontier.onboard.discover import suggest_models
from frontier.onboard.github import (
    api_and_snowflake_secrets_separated,
    render_workflow,
    validate_workflow_yaml,
)
from frontier.onboard.hashkey import HASH_KEY_BYTES, generate_entity_hash_key
from frontier.onboard.versions import version_at_least
from frontier import __version__
from tests.conftest import FIXTURES


def _project(tmp_path: Path) -> Path:
    project = tmp_path / "jaffle_shop"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: jaffle_shop\nprofile: jaffle_shop\n")
    subprocess.run(["git", "init"], cwd=project, check=True, capture_output=True)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://github.com/acme/jaffle_shop.git"],
        cwd=project,
        check=True,
        capture_output=True,
    )
    return project


def test_init_detects_dbt_project_and_preserves_existing_config(tmp_path: Path, capsys) -> None:
    project = _project(tmp_path)
    assert main(["init", "--yes", str(project)]) == 0
    config = project / ".frontier" / "config.yml"
    assert config.is_file()
    text = config.read_text()
    assert "jaffle_shop" in text
    assert "api_url:" in text
    assert "frn_" not in text
    gitignore = (project / ".gitignore").read_text()
    assert "target/frontier-*.json" in gitignore
    assert ".frontier/cache/" in gitignore
    config.write_text(text + "# keep\n")
    assert main(["init", "--yes", str(project)]) == 0
    assert "# keep" in config.read_text()
    out = capsys.readouterr().out
    assert "Kept existing" in out


def test_init_refuses_missing_dbt_project(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    assert main(["init", "--yes", str(empty)]) == 1


def test_credentials_file_fallback_and_logout(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "credentials"
    monkeypatch.setenv("FRONTIER_CREDENTIALS_FILE", str(path))
    monkeypatch.setattr("frontier.credentials._set_keyring", lambda payload: False)
    monkeypatch.setattr("frontier.credentials._get_keyring", lambda: None)
    monkeypatch.setattr("frontier.credentials._delete_keyring", lambda: None)
    creds = StoredCredentials(
        api_url="https://frontier.example",
        api_key="frn_abcdefghijklmnopqrstuvwxyz",
        project="jaffle_shop",
        organization="Acme",
    )
    assert save_credentials(creds) == "file"
    assert path.is_file()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    loaded = load_credentials()
    assert loaded is not None
    assert loaded.api_key == creds.api_key
    assert loaded.prefix() == "frn_abcdefgh…"
    delete_credentials()
    assert not path.exists()
    assert load_credentials() is None


def test_login_validates_and_redacts_key(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("FRONTIER_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setattr("frontier.credentials._set_keyring", lambda payload: False)
    monkeypatch.setattr("frontier.credentials._get_keyring", lambda: None)

    def fake_whoami(api_url: str, api_key: str):
        from frontier.onboard.saas import WhoAmI

        assert api_key == "frn_abcdefghijklmnopqrstuvwxyz"
        return WhoAmI("Acme", "jaffle_shop", "frn_abcdefgh…", api_url)

    monkeypatch.setattr("frontier.onboard.commands.whoami", fake_whoami)
    args = SimpleNamespace(
        api_key=True,
        api_url="https://frontier.example",
        _getpass=lambda prompt: "frn_abcdefghijklmnopqrstuvwxyz",
    )
    from frontier.onboard.commands import cmd_login

    assert cmd_login(args) == 0
    out = capsys.readouterr().out
    assert "Authenticated" in out
    assert "Organization: Acme" in out
    assert "Project: jaffle_shop" in out
    assert "frn_abcdefgh…" in out
    assert "abcdefghijklmnopqrstuvwxyz" not in out
    assert main(["auth", "status"]) == 0
    status_out = capsys.readouterr().out
    assert "abcdefghijklmnopqrstuvwxyz" not in status_out
    assert main(["logout"]) == 0
    assert main(["auth", "status"]) == 1


def test_isolated_keyring_never_loads_os_keyring_module(monkeypatch) -> None:
    calls: list[int] = []
    monkeypatch.setattr("frontier.credentials._load_keyring", lambda: calls.append(1) or None)
    creds = StoredCredentials(
        api_url="https://frontier.example",
        api_key="frn_isolated_keyring_value",
        project="jaffle_shop",
        organization="zetra",
    )
    assert save_credentials(creds) == "keyring"
    loaded = load_stored_credentials()
    assert loaded is not None
    assert loaded.api_key == creds.api_key
    delete_credentials()
    assert load_stored_credentials() is None
    assert calls == []


def test_manifest_fetch_requires_login(dbt_project: Path, capsys) -> None:
    assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 1
    err = capsys.readouterr().err
    assert "AUTH_REQUIRED: Run `frontier login --api-key`." in err
    assert "FRONTIER_DEMO_API_KEY" not in err
    assert "to upload" not in err


def test_manifest_fetch_uses_stored_credentials_across_processes(dbt_project: Path) -> None:
    from tests.test_semantic import _active_payload

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            del format, args

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path.endswith("/api/v1/auth/whoami"):
                payload = {
                    "organization": "zetra",
                    "project": "jaffle_shop",
                    "apiKeyPrefix": "frn_subproc12…",
                }
            elif path.endswith("/manifests/active"):
                payload = _active_payload()
            else:
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    runner_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(runner_root) + os.pathsep + env.get("PYTHONPATH", "")
    env.pop("FRONTIER_API_KEY", None)
    env.pop("FRONTIER_DEMO_API_KEY", None)
    env.pop("FRONTIER_API_URL", None)
    host, port = server.server_address
    origin = f"http://{host}:{port}"
    login_code = (
        "from types import SimpleNamespace\n"
        "from frontier.onboard.commands import cmd_login\n"
        "raise SystemExit(cmd_login(SimpleNamespace("
        f"api_key=True, api_url={origin!r}, "
        '_getpass=lambda prompt: "frn_subprocess_stored_key")))\n'
    )
    try:
        login = subprocess.run(
            [sys.executable, "-c", login_code],
            cwd=str(runner_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert login.returncode == 0, login.stderr
        assert "Authenticated" in login.stdout
        assert "FRONTIER_API_KEY" not in env

        fetch = subprocess.run(
            [sys.executable, "-m", "frontier", "manifest", "fetch", "--project-dir", str(dbt_project)],
            cwd=str(runner_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert fetch.returncode == 0, fetch.stderr
        assert "source: saas_active" in fetch.stdout
        pin = json.loads((dbt_project / "target" / "frontier-manifest.json").read_text())
        assert pin["project"] == "jaffle_shop"

        logout = subprocess.run(
            [sys.executable, "-m", "frontier", "logout"],
            cwd=str(runner_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert logout.returncode == 0, logout.stderr

        denied = subprocess.run(
            [sys.executable, "-m", "frontier", "manifest", "fetch", "--project-dir", str(dbt_project)],
            cwd=str(runner_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert denied.returncode == 1
        assert "AUTH_REQUIRED: Run `frontier login --api-key`." in denied.stderr
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_discover_suggests_customer_summary_draft_only() -> None:
    manifest = load_manifest(FIXTURES / "manifest.json")
    suggestions = suggest_models(manifest)
    names = [item.model for item in suggestions]
    assert "customer_summary" in names
    assert "stg_customers" not in names
    assert "frontier_affected_customers" not in names
    selected = next(item for item in suggestions if item.model == "customer_summary")
    assert selected.entity == "customer"
    assert selected.entity_key == "customer_id"
    assert {source.name for source in selected.sources} >= {"stg_customers", "stg_orders"}
    document = selected.to_semantic_document()
    assert all(source["origin"] == "inferred" for source in document["sources"])


def test_discover_uploads_draft(tmp_path: Path, monkeypatch, capsys, dbt_project: Path) -> None:
    uploaded: dict = {}

    def fake_upload(creds, document):
        uploaded["document"] = document
        uploaded["project"] = creds.project
        from frontier.onboard.saas import DraftManifestResult

        return DraftManifestResult(1, "draft", "https://example.test/manifests?version=1")

    monkeypatch.setattr("frontier.onboard.commands.upload_draft_manifest", fake_upload)
    monkeypatch.setattr(
        "frontier.onboard.commands._require_credentials",
        lambda: StoredCredentials("https://example.test", "frn_testkeyxxxx", "jaffle_shop"),
    )
    assert main(["discover", "--yes", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "Detected model: customer_summary" in out
    assert "Draft manifest created: version 1" in out
    assert "not active" in out.lower() or "This draft is not active" in out
    assert uploaded["document"]["model"] == "customer_summary"
    assert uploaded["document"]["sources"][0]["origin"] == "inferred"
    assert "activate" in out.lower()


def test_doctor_json_is_redacted_and_nonzero_on_failure(
    tmp_path: Path, capsys, monkeypatch
) -> None:
    monkeypatch.setattr("frontier.onboard.doctor.saas_reachable", lambda url: False)
    project = _project(tmp_path)
    assert main(["doctor", "--json", "--skip-warehouse", str(project)]) == 1
    payload = json.loads(capsys.readouterr().out)
    dumped = json.dumps(payload)
    assert "password" not in dumped.lower() or "********" in dumped
    assert payload["ok"] is False
    assert any(check["id"] == "auth" and check["ok"] is False for check in payload["checks"])


def test_workflow_generation_validates_yaml_and_separates_secrets() -> None:
    text = render_workflow(runner_version="0.1.0", profile_name="jaffle_shop")
    loaded = validate_workflow_yaml(text)
    assert loaded["name"] == "Frontier"
    assert "FRONTIER_BLOCKING: \"false\"" in text
    assert "frontier-runner[snowflake]==0.1.0" in text
    assert "github.com/jadsamara/frontier-runner/releases/download/v0.1.0" in text
    assert "git+" not in text
    assert api_and_snowflake_secrets_separated(text)
    assert "SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}" in text
    prove = text.split("- name: Generate impact assessment", 1)[1]
    assert "FRONTIER_API_KEY" not in prove.split("- name: Upload assessment", 1)[0]
    upload = text.split("- name: Upload assessment", 1)[1]
    assert "SNOWFLAKE_PASSWORD" not in upload


def test_setup_github_writes_workflow(tmp_path: Path, capsys, monkeypatch) -> None:
    import shutil

    original_which = shutil.which
    monkeypatch.setattr(
        shutil,
        "which",
        lambda name: None if name == "gh" else original_which(name),
    )
    project = _project(tmp_path)
    assert main(["setup", "github", "--yes", "--force", str(project)]) == 0
    path = project / ".github" / "workflows" / "frontier.yml"
    assert path.is_file()
    text = path.read_text()
    validate_workflow_yaml(text)
    out = capsys.readouterr().out
    assert "Workflow created:" in out
    assert "Commit this file" in out
    assert "frn_" not in text
    assert generate_entity_hash_key() not in text


def test_hash_key_has_256_bits_and_is_not_printed_by_default(capsys, monkeypatch) -> None:
    monkeypatch.setattr("frontier.onboard.commands.shutil.which", lambda name: None)
    monkeypatch.setattr("frontier.onboard.commands.prompt_yes_no", lambda *args, **kwargs: False)
    assert main(["setup", "hash-key"]) == 0
    out = capsys.readouterr().out
    assert "Entity hash key:" in out
    key = generate_entity_hash_key()
    assert len(bytes.fromhex(key)) >= HASH_KEY_BYTES
    # default output is a prefix, not a 64-char hex key
    assert not any(len(line.strip()) == 64 and all(c in "0123456789abcdef" for c in line.strip()) for line in out.splitlines())


def test_runner_version_compatibility() -> None:
    assert version_at_least("0.1.0", "0.1.0")
    assert version_at_least("0.2.0", "0.1.0")
    assert not version_at_least("0.0.9", "0.1.0")
    assert __version__ == "0.1.1"


def test_install_error_includes_stable_code() -> None:
    error = InstallError(
        "MANIFEST_NOT_ACTIVE",
        "No active semantic manifest exists for this project.",
        cause="The draft has not been activated.",
        next_action="Review and activate the draft at https://example.test/manifests.",
        docs_path="/docs/semantic-manifest",
    )
    text = str(error)
    assert "MANIFEST_NOT_ACTIVE" in text
    assert "Likely cause:" in text
    assert "Next:" in text
    assert "Docs:" in text


def test_demo_change_does_not_modify_sql(dbt_project: Path) -> None:
    before = {path: path.read_text() for path in dbt_project.rglob("*.sql")} if False else {}
    sql_files = list(dbt_project.rglob("*.sql"))
    snapshots = {path: path.read_text() if path.is_file() else "" for path in sql_files}
    assert main(["demo", "change", "--project-dir", str(dbt_project)]) == 0
    for path, text in snapshots.items():
        assert path.read_text() == text


def test_package_metadata_excludes_saas_and_fixtures() -> None:
    root = Path(__file__).resolve().parents[1]
    manifest_in = (root / "MANIFEST.in").read_text()
    assert "prune tests" in manifest_in
    pyproject = (root / "pyproject.toml").read_text()
    assert 'name = "frontier-runner"' in pyproject
    assert "snowflake" in pyproject
    assert "bigquery" not in pyproject
    assert (root / "LICENSE").is_file()
    assert (root / "CHANGELOG.md").is_file()
    publish = (root / ".github" / "workflows" / "publish.yml").read_text()
    assert "pypa/gh-action-pypi-publish" in publish
    assert 'tags:' in publish
    assert "Smoke-install the wheel" in publish


def test_wheel_installs_and_reports_version(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    probe = subprocess.run(
        ["python3", "-m", "build", "--help"],
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0:
        pytest.skip("python -m build is not installed")
    dist = tmp_path / "dist"
    env = {**os.environ, "PYTHONPATH": "", "PIP_DISABLE_PIP_VERSION_CHECK": "1"}
    subprocess.run(
        ["python3", "-m", "build", "--wheel", "--outdir", str(dist)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    wheels = list(dist.glob("frontier_runner-*.whl"))
    assert wheels
    venv = tmp_path / "venv"
    subprocess.run(["python3", "-m", "venv", str(venv)], check=True, capture_output=True, env=env)
    pip = venv / "bin" / "pip"
    frontier = venv / "bin" / "frontier"
    subprocess.run(
        [str(pip), "install", "--force-reinstall", str(wheels[0])],
        check=True,
        capture_output=True,
        env=env,
    )
    version = subprocess.check_output([str(frontier), "--version"], text=True, env=env)
    assert "0.1.1" in version
    names = subprocess.check_output(["python3", "-m", "zipfile", "-l", str(wheels[0])], text=True)
    assert "tests/" not in names
    assert "fixtures/" not in names
    assert ".env" not in names
