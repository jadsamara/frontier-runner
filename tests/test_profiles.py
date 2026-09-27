from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

import yaml

from frontier.cli import build_parser
from frontier.context import (
    assert_artifact_matches_context,
    assessment_identity_fields,
    ensure_named_profile_ids,
    ExecutionContext,
    in_recognized_ci,
    resolve_api_origin,
    resolve_dbt_target,
    resolve_execution_context,
    resolve_profiles_yml_path,
    select_frontier_profile_name,
    warehouse_connect_kwargs,
)
from frontier.identity import IDENTITY_INCOMPLETE_CODE
from frontier.credentials import (
    StoredCredentials,
    keyring_username,
    load_stored_credentials,
    save_credentials,
)
from frontier.errors import InstallError
from frontier.onboard.saas import WhoAmI
from frontier.profiles import (
    ProfileRecord,
    get_profile,
    load_registry,
    load_selected_profile_name,
    upsert_profile,
    validate_profile_name,
    write_selected_profile,
)


REPAIR_KEY = "frn_repair_key_abcdefghijklmnop"
BENCH_KEY = "frn_bench_key_abcdefghijklmnop"
LEGACY_KEY = "frn_legacy_key_abcdefghijklmnop"
ORG_B_KEY = "frn_org_b_key_abcdefghijklmnop"
NAMES_ONLY_KEY = "frn_names_only_key_abcdefghij"


def _whoami_for(api_url: str, api_key: str) -> WhoAmI:
    mapping = {
        REPAIR_KEY: ("Test Org", "test_frontier", "org_a", "project_a"),
        BENCH_KEY: ("Test Org", "frontier_benchmark", "org_a", "project_b"),
        LEGACY_KEY: ("Test Org", "legacy_project", "org_a", "project_legacy"),
        ORG_B_KEY: ("Test Org", "test_frontier", "org_b", "project_a"),
        NAMES_ONLY_KEY: ("Test Org", "test_frontier", "", ""),
        "frn_abcdefghijklmnopqrstuvwxyz": ("Acme", "jaffle_shop", "org_acme", "project_jaffle"),
    }
    org, project, org_id, project_id = mapping[api_key]
    return WhoAmI(org, project, f"{api_key[:12]}…", api_url, org_id, project_id)


def _patch_whoami(monkeypatch) -> None:
    monkeypatch.setattr("frontier.onboard.profile_commands.whoami", _whoami_for)
    monkeypatch.setattr("frontier.onboard.commands.whoami", _whoami_for)
    monkeypatch.setattr("frontier.onboard.saas.whoami", _whoami_for)
    monkeypatch.setattr("frontier.context._whoami_identity", _whoami_for)


def _write_dbt_project(root: Path, *, targets: tuple[str, ...] = ("dev", "benchmark")) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    project = root / "jaffle_shop"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: jaffle_shop\nprofile: jaffle_shop\n")
    profiles_dir = root / "dbt-profiles"
    profiles_dir.mkdir()
    outputs = {
        name: {
            "type": "snowflake",
            "account": "example",
            "user": "tester",
            "password": "super-secret-password",
            "database": "FRONTIER_TEST",
            "schema": name.upper(),
            "warehouse": "COMPUTE_WH",
        }
        for name in targets
    }
    (profiles_dir / "profiles.yml").write_text(
        yaml.safe_dump(
            {
                "jaffle_shop": {
                    "target": "dev",
                    "outputs": outputs,
                }
            }
        )
    )
    return project


def _args(**kwargs):
    values = {
        "frontier_profile": None,
        "api_url": None,
        "target": None,
        "profiles": None,
        "project_dir": ".",
        "api_key": False,
        "force": False,
        "yes": False,
        "name": None,
        "_getpass": None,
        "offline": False,
        "remove_metadata": False,
        "blocking": False,
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def test_profile_name_rejects_path_traversal_and_empty() -> None:
    for name in ("", "../etc", "foo/bar", "bad name", ".", "..", "a" * 80, "nope\x00"):
        try:
            validate_profile_name(name)
        except InstallError as error:
            assert error.code == "FRONTIER_PROFILE_INVALID_NAME"
        else:
            raise AssertionError(name)


def test_create_first_and_second_profile(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    monkeypatch.chdir(project)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path / "dbt-profiles"))
    from frontier.onboard.profile_commands import cmd_profile_create

    assert (
        cmd_profile_create(
            _args(
                name="repair",
                api_key=True,
                api_url="https://frontier.example.com",
                target="dev",
                _getpass=lambda prompt: REPAIR_KEY,
                project_dir=str(project),
            )
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Frontier profile: repair" in out
    assert "test_frontier" in out
    assert REPAIR_KEY not in out
    stored = load_stored_credentials(profile_name="repair")
    assert stored is not None
    assert stored.api_key == REPAIR_KEY
    assert stored.project == "test_frontier"
    assert keyring_username("repair") == "profile:repair"

    assert (
        cmd_profile_create(
            _args(
                name="benchmark",
                api_key=True,
                api_url="https://frontier.example.com",
                target="benchmark",
                _getpass=lambda prompt: BENCH_KEY,
                project_dir=str(project),
            )
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "frontier_benchmark" in out
    assert BENCH_KEY not in out
    repair = load_stored_credentials(profile_name="repair")
    bench = load_stored_credentials(profile_name="benchmark")
    assert repair is not None and bench is not None
    assert repair.api_key != bench.api_key
    from frontier.profiles import get_profile

    repair_meta = get_profile("repair")
    bench_meta = get_profile("benchmark")
    assert repair_meta.organization_id == "org_a"
    assert repair_meta.project_id == "project_a"
    assert bench_meta.organization_id == "org_a"
    assert bench_meta.project_id == "project_b"
    assert repair_meta.project_id != bench_meta.project_id


def test_secure_prompt_flag_does_not_take_raw_key() -> None:
    help_text = " ".join(build_parser().format_help().split())
    assert "local named execution context" in help_text
    ns = build_parser().parse_args(["profile", "create", "repair", "--api-key"])
    assert ns.api_key is True
    assert ns.name == "repair"


def test_file_fallback_supports_multiple_profiles_0600(monkeypatch) -> None:
    monkeypatch.setattr("frontier.credentials._set_keyring", lambda payload: False)
    monkeypatch.setattr("frontier.credentials._get_keyring", lambda: None)
    monkeypatch.setattr("frontier.credentials._set_keyring_entry", lambda username, payload: False)
    monkeypatch.setattr("frontier.credentials._get_keyring_entry", lambda username: None)
    path = Path(os.environ["FRONTIER_CREDENTIALS_FILE"])
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
        ),
        profile_name="repair",
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=BENCH_KEY,
            project="frontier_benchmark",
            organization="Test Org",
        ),
        profile_name="benchmark",
    )
    assert path.is_file()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    payload = json.loads(path.read_text())
    assert payload["version"] == 1
    assert "repair" in payload["profiles"]
    assert "benchmark" in payload["profiles"]
    assert load_stored_credentials(profile_name="repair").api_key == REPAIR_KEY
    assert load_stored_credentials(profile_name="benchmark").api_key == BENCH_KEY


def test_legacy_credential_remains_usable() -> None:
    legacy = StoredCredentials(
        api_url="https://frontier.example.com",
        api_key=LEGACY_KEY,
        project="legacy_project",
        organization="Test Org",
    )
    assert save_credentials(legacy) == "keyring"
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
        ),
        profile_name="repair",
    )
    still = load_stored_credentials()
    assert still is not None
    assert still.api_key == LEGACY_KEY
    named = load_stored_credentials(profile_name="repair")
    assert named is not None
    assert named.api_key == REPAIR_KEY


def test_profile_list_never_shows_key(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_list, cmd_profile_use

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            target="dev",
            _getpass=lambda prompt: REPAIR_KEY,
            project_dir=str(project),
            force=True,
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            project_dir=str(project),
            force=True,
        )
    )
    capsys.readouterr()
    cmd_profile_use(_args(name="benchmark", project_dir=str(project), offline=True))
    capsys.readouterr()
    assert cmd_profile_list(_args(project_dir=str(project))) == 0
    out = capsys.readouterr().out
    assert "repair" in out
    assert "benchmark" in out
    assert "*" in out
    assert "frontier_benchmark" in out
    assert REPAIR_KEY not in out
    assert BENCH_KEY not in out
    assert "credential=keychain" in out


def test_use_profile_persists_per_project(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    repo_a = _write_dbt_project(tmp_path / "a")
    repo_b = _write_dbt_project(tmp_path / "b")
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_use

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(repo_a),
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(repo_a),
        )
    )
    cmd_profile_use(_args(name="repair", project_dir=str(repo_a), offline=True))
    cmd_profile_use(_args(name="benchmark", project_dir=str(repo_b), offline=True))
    assert load_selected_profile_name(repo_a) == "repair"
    assert load_selected_profile_name(repo_b) == "benchmark"
    assert ".frontier/state.yml" in (repo_a / ".gitignore").read_text()
    assert "state.yml" in (repo_a / ".frontier" / ".gitignore").read_text()


def test_unknown_profile_fails_closed(tmp_path: Path) -> None:
    try:
        resolve_execution_context(_args(frontier_profile="missing"), tmp_path)
    except InstallError as error:
        assert error.code == "FRONTIER_PROFILE_NOT_FOUND"
    else:
        raise AssertionError("expected missing profile to fail")


def test_missing_profile_credential_fails_closed(tmp_path: Path) -> None:
    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url="https://frontier.example.com",
            project_name="test_frontier",
            organization_name="Test Org",
            credential_ref="repair",
        ),
        force=True,
    )
    try:
        resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    except InstallError as error:
        assert error.code == "FRONTIER_PROFILE_CREDENTIAL_MISSING"
    else:
        raise AssertionError("expected missing credential to fail")


def test_env_and_flag_selection_precedence(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_use

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    cmd_profile_use(_args(name="repair", project_dir=str(project), offline=True))
    monkeypatch.setenv("FRONTIER_PROFILE", "benchmark")
    name, source = select_frontier_profile_name(_args(), project)
    assert name == "benchmark" and source == "env"
    name, source = select_frontier_profile_name(_args(frontier_profile="repair"), project)
    assert name == "repair" and source == "flag"


def test_api_key_env_mismatch_fails(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    monkeypatch.setenv("FRONTIER_API_KEY", BENCH_KEY)
    try:
        resolve_execution_context(_args(frontier_profile="repair"), project)
    except InstallError as error:
        assert error.code == "FRONTIER_PROFILE_IDENTITY_MISMATCH"
        assert BENCH_KEY not in str(error)
    else:
        raise AssertionError("expected identity mismatch")


def test_api_origin_and_profiles_and_target_precedence(tmp_path: Path, monkeypatch) -> None:
    record = ProfileRecord(
        name="benchmark",
        api_url="https://profile.example.com",
        project_name="frontier_benchmark",
        dbt_target="benchmark",
        profiles_path=str(tmp_path / "profile-profiles.yml"),
        credential_ref="benchmark",
    )
    origin, source = resolve_api_origin(
        _args(api_url="https://flag.example.com"),
        record=record,
        creds=None,
        local=None,
    )
    assert origin == "https://flag.example.com" and source == "flag"
    origin, source = resolve_api_origin(_args(), record=record, creds=None, local=None)
    assert origin == "https://profile.example.com"
    path = resolve_profiles_yml_path(_args(profiles=str(tmp_path / "explicit.yml")), record=record)
    assert path == tmp_path / "explicit.yml"
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path / "dbt-dir"))
    path = resolve_profiles_yml_path(_args(), record=record)
    assert path == tmp_path / "dbt-dir" / "profiles.yml"
    monkeypatch.delenv("DBT_PROFILES_DIR", raising=False)
    path = resolve_profiles_yml_path(_args(), record=record)
    assert path == tmp_path / "profile-profiles.yml"
    assert resolve_dbt_target(_args(target="ci"), record=record) == "ci"
    monkeypatch.setenv("FRONTIER_DBT_TARGET", "from-env")
    assert resolve_dbt_target(_args(), record=record) == "from-env"
    monkeypatch.delenv("FRONTIER_DBT_TARGET", raising=False)
    assert resolve_dbt_target(_args(), record=record) == "benchmark"


def test_no_profile_target_unchanged(tmp_path: Path) -> None:
    ctx = resolve_execution_context(_args(), tmp_path)
    assert ctx.profile_name is None
    assert ctx.dbt_target is None
    assert ctx.profiles_path is None


def test_ci_ignores_local_selection_unless_explicit(tmp_path: Path, monkeypatch) -> None:
    project = _write_dbt_project(tmp_path)
    write_selected_profile(project, "benchmark")
    upsert_profile(
        ProfileRecord(
            name="benchmark",
            api_url="https://frontier.example.com",
            project_name="frontier_benchmark",
            credential_ref="benchmark",
        ),
        force=True,
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=BENCH_KEY,
            project="frontier_benchmark",
        ),
        profile_name="benchmark",
    )
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    assert in_recognized_ci() is True
    name, source = select_frontier_profile_name(_args(), project)
    assert name is None and source == "none"
    monkeypatch.setenv("FRONTIER_PROFILE", "benchmark")
    name, source = select_frontier_profile_name(_args(), project)
    assert name == "benchmark" and source == "env"


def test_artifact_mismatch_and_switch_back(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            target="dev",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    payload = {
        "project": "test_frontier",
        "assessmentIdentity": {
            "runnerVersion": "0.2.2",
            "invocationId": "run-1",
            "apiOrigin": "https://frontier.example.com",
            "organizationId": "org_a",
            "organizationName": "Test Org",
            "projectId": "project_a",
            "projectName": "test_frontier",
            "dbtProject": "jaffle_shop",
            "dbtTarget": "dev",
            "frontierProfile": "repair",
        },
    }
    repair_ctx = resolve_execution_context(_args(frontier_profile="repair"), project)
    assert_artifact_matches_context(payload, repair_ctx, dbt_project="jaffle_shop", dbt_target="dev")
    bench_ctx = resolve_execution_context(_args(frontier_profile="benchmark"), project)
    try:
        assert_artifact_matches_context(
            payload,
            bench_ctx,
            dbt_project="jaffle_shop",
            dbt_target="benchmark",
        )
    except InstallError as error:
        assert error.code == "ARTIFACT_PROFILE_MISMATCH"
        assert BENCH_KEY not in str(error)
    else:
        raise AssertionError("expected artifact mismatch")
    assert_artifact_matches_context(
        payload,
        resolve_execution_context(_args(frontier_profile="repair"), project),
        dbt_project="jaffle_shop",
        dbt_target="dev",
    )


def test_legacy_artifact_without_identity_is_compatible(tmp_path: Path) -> None:
    ctx = resolve_execution_context(_args(), tmp_path)
    assert_artifact_matches_context(
        {"project": "jaffle_shop", "assessmentIdentity": {"runnerVersion": "0.2.2", "invocationId": "x"}},
        ctx,
        dbt_project="jaffle_shop",
        dbt_target="dev",
    )


def test_logout_removes_only_selected_profile_credential(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.commands import cmd_logout
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    assert cmd_logout(_args(frontier_profile="benchmark")) == 0
    out = capsys.readouterr().out
    assert "benchmark" in out
    assert load_stored_credentials(profile_name="benchmark") is None
    assert load_stored_credentials(profile_name="repair") is not None
    assert REPAIR_KEY not in out


def test_remove_active_profile_requires_force(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_remove, cmd_profile_use

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    cmd_profile_use(_args(name="benchmark", project_dir=str(project), offline=True))
    capsys.readouterr()
    assert cmd_profile_remove(_args(name="benchmark", project_dir=str(project))) == 1
    assert "benchmark" in load_registry().profiles
    assert cmd_profile_remove(_args(name="benchmark", project_dir=str(project), force=True, yes=True)) == 0
    assert "benchmark" not in load_registry().profiles
    assert load_selected_profile_name(project) is None


def test_selected_profile_used_by_discover_manifest_prove_cdc_upload(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    ctx = resolve_execution_context(_args(frontier_profile="benchmark"), project)
    assert ctx.creds is not None
    assert ctx.creds.api_key == BENCH_KEY
    assert ctx.project_name == "frontier_benchmark"
    assert ctx.dbt_target == "benchmark"
    kwargs = warehouse_connect_kwargs(_args(frontier_profile="benchmark"), ctx)
    assert kwargs["target"] == "benchmark"


def test_setup_github_never_embeds_profile_secrets(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    monkeypatch.setattr(
        "frontier.onboard.commands.detect_project",
        lambda directory: SimpleNamespace(
            github_origin="https://github.com/acme/jaffle-shop.git",
            git_root=directory,
            adapter_type="snowflake",
            profile_name="jaffle_shop",
            dbt_project_name="jaffle_shop",
            default_branch="main",
            dbt_project_yml=directory / "dbt_project.yml",
            targets=("dev", "benchmark"),
            manifest_path=None,
            dbt_installed=True,
            gh_installed=False,
            missing=(),
            project_dir=directory,
        ),
    )
    from frontier.onboard.commands import cmd_setup_github
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_use

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    cmd_profile_use(_args(name="benchmark", project_dir=str(project), offline=True))
    monkeypatch.setattr("frontier.onboard.commands.shutil.which", lambda name: None)
    capsys.readouterr()
    assert (
        cmd_setup_github(
            _args(project_dir=str(project), yes=True, force=True, blocking=False, frontier_profile=None)
        )
        == 0
    )
    out = capsys.readouterr().out
    workflow = (project / ".github" / "workflows" / "frontier.yml").read_text()
    assert BENCH_KEY not in workflow
    assert "profile:benchmark" not in workflow
    assert "state.yml" not in workflow
    assert "secrets.FRONTIER_API_KEY" in workflow
    assert "does not follow" in out
    assert BENCH_KEY not in out


def test_doctor_reports_selected_target(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path / "dbt-profiles"))
    from frontier.onboard.doctor import run_doctor
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    monkeypatch.setattr("frontier.onboard.doctor.saas_reachable", lambda url: False)
    monkeypatch.setattr("frontier.onboard.doctor.whoami", _whoami_for)
    checks = run_doctor(
        project,
        skip_warehouse=True,
        args=_args(frontier_profile="benchmark", project_dir=str(project)),
    )
    by_id = {check.id: check for check in checks}
    assert by_id["frontier_profile"].detail == "benchmark"
    assert by_id["dbt_target"].detail == "benchmark"
    assert by_id["frontier_profile_identity"].ok is True
    assert by_id["frontier_profile_identity"].detail == "org_a / project_b"


def test_auth_status_and_help_redaction(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    project = _write_dbt_project(tmp_path)
    from frontier.onboard.commands import cmd_auth_status
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(project),
        )
    )
    capsys.readouterr()
    assert cmd_auth_status(_args(frontier_profile="benchmark", project_dir=str(project))) == 0
    out = capsys.readouterr().out
    assert "Frontier profile: benchmark" in out
    assert "frontier_benchmark" in out
    assert BENCH_KEY not in out


def test_create_rejects_names_only_whoami(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    try:
        cmd_profile_create(
            _args(
                name="repair",
                api_key=True,
                api_url="https://frontier.example.com",
                _getpass=lambda prompt: NAMES_ONLY_KEY,
                force=True,
                project_dir=str(tmp_path),
            )
        )
    except InstallError as error:
        assert error.code == IDENTITY_INCOMPLETE_CODE
        assert "immutable organization and project IDs" in error.explanation
    else:
        raise AssertionError("expected incomplete identity")
    assert "repair" not in load_registry().profiles


def test_legacy_login_still_works_with_names_only_server(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.setattr("frontier.credentials._set_keyring", lambda payload: False)
    monkeypatch.setattr("frontier.credentials._get_keyring", lambda: None)

    def names_only(api_url: str, api_key: str) -> WhoAmI:
        return WhoAmI("Test Org", "legacy_project", f"{api_key[:12]}…", api_url)

    monkeypatch.setattr("frontier.onboard.commands.whoami", names_only)
    from frontier.onboard.commands import cmd_login

    assert (
        cmd_login(
            _args(
                api_key=True,
                api_url="https://frontier.example.com",
                _getpass=lambda prompt: LEGACY_KEY,
            )
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Authenticated" in out
    assert "legacy_project" in out
    assert LEGACY_KEY not in out


def test_name_only_profile_upgrades_after_authenticated_whoami(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url="https://frontier.example.com",
            organization_name="Test Org",
            project_name="test_frontier",
            dbt_target="dev",
            credential_ref="repair",
        ),
        force=True,
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
        ),
        profile_name="repair",
    )
    ctx = resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    assert ctx.organization_id == "org_a"
    assert ctx.project_id == "project_a"
    loaded = get_profile("repair")
    assert loaded.organization_id == "org_a"
    assert loaded.project_id == "project_a"
    assert loaded.dbt_target == "dev"
    assert loaded.credential_ref == "repair"
    stored = load_stored_credentials(profile_name="repair")
    assert stored is not None
    assert stored.api_key == REPAIR_KEY


def test_name_only_profile_does_not_upgrade_because_names_match(tmp_path: Path, monkeypatch) -> None:
    def names_only(api_url: str, api_key: str) -> WhoAmI:
        return WhoAmI("Test Org", "test_frontier", f"{api_key[:12]}…", api_url)

    monkeypatch.setattr("frontier.context._whoami_identity", names_only)
    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url="https://frontier.example.com",
            organization_name="Test Org",
            project_name="test_frontier",
            credential_ref="repair",
        ),
        force=True,
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
        ),
        profile_name="repair",
    )
    try:
        resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    except InstallError as error:
        assert error.code == IDENTITY_INCOMPLETE_CODE
    else:
        raise AssertionError("expected incomplete identity")
    loaded = get_profile("repair")
    assert loaded.organization_id == ""
    assert loaded.project_id == ""


def test_name_only_profile_unavailable_saas_fails_closed(tmp_path: Path, monkeypatch) -> None:
    def unreachable(api_url: str, api_key: str):
        raise InstallError(
            "SAAS_UNREACHABLE",
            "Frontier SaaS is not reachable.",
            cause="network error",
            next_action="retry",
        )

    monkeypatch.setattr("frontier.context._whoami_identity", unreachable)
    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url="https://frontier.example.com",
            organization_name="Test Org",
            project_name="test_frontier",
            credential_ref="repair",
        ),
        force=True,
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
        ),
        profile_name="repair",
    )
    try:
        resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    except InstallError as error:
        assert error.code == IDENTITY_INCOMPLETE_CODE
    else:
        raise AssertionError("expected incomplete identity")
    assert get_profile("repair").organization_id == ""


def test_organization_and_project_rename_same_ids_refresh_names(tmp_path: Path, monkeypatch) -> None:
    def renamed(api_url: str, api_key: str) -> WhoAmI:
        return WhoAmI("Renamed Org", "renamed_project", f"{api_key[:12]}…", api_url, "org_a", "project_a")

    monkeypatch.setattr("frontier.context._whoami_identity", renamed)
    upsert_profile(
        ProfileRecord(
            name="repair",
            api_url="https://frontier.example.com",
            organization_id="org_a",
            organization_name="Test Org",
            project_id="project_a",
            project_name="test_frontier",
            credential_ref="repair",
        ),
        force=True,
    )
    save_credentials(
        StoredCredentials(
            api_url="https://frontier.example.com",
            api_key=REPAIR_KEY,
            project="test_frontier",
            organization="Test Org",
            organization_id="org_a",
            project_id="project_a",
        ),
        profile_name="repair",
    )
    record = get_profile("repair")
    creds = load_stored_credentials(profile_name="repair")
    assert creds is not None
    updated = ensure_named_profile_ids(record, creds, revalidate=True)
    assert updated.organization_id == "org_a"
    assert updated.project_id == "project_a"
    assert updated.organization_name == "Renamed Org"
    assert updated.project_name == "renamed_project"


def test_same_names_different_project_ids_fail(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    monkeypatch.setenv("FRONTIER_API_KEY", BENCH_KEY)
    try:
        resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    except InstallError as error:
        assert error.code == "FRONTIER_PROFILE_IDENTITY_MISMATCH"
        assert "project_a" in str(error)
        assert "project_b" in str(error)
        assert BENCH_KEY not in str(error)
    else:
        raise AssertionError("expected identity mismatch")


def test_same_project_name_different_organizations_fail(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    monkeypatch.setenv("FRONTIER_API_KEY", ORG_B_KEY)
    try:
        resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    except InstallError as error:
        assert error.code == "FRONTIER_PROFILE_IDENTITY_MISMATCH"
        assert "org_a" in str(error)
        assert "org_b" in str(error)
    else:
        raise AssertionError("expected organization mismatch")


def test_environment_key_matching_ids_succeeds(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    monkeypatch.setenv("FRONTIER_API_KEY", REPAIR_KEY)
    ctx = resolve_execution_context(_args(frontier_profile="repair"), tmp_path)
    assert ctx.organization_id == "org_a"
    assert ctx.project_id == "project_a"
    assert ctx.creds is not None
    assert ctx.creds.source == "FRONTIER_API_KEY"


def test_named_profile_artifact_contains_ids_and_no_secrets(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            target="benchmark",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    ctx = resolve_execution_context(_args(frontier_profile="benchmark"), tmp_path)
    fields = assessment_identity_fields(ctx, dbt_target="benchmark", dbt_project="jaffle_shop")
    assert fields["organizationId"] == "org_a"
    assert fields["projectId"] == "project_b"
    assert fields["frontierProfile"] == "benchmark"
    blob = json.dumps(fields)
    assert BENCH_KEY not in blob
    assert "profile:" not in blob
    assert "profiles.yml" not in blob
    assert str(tmp_path) not in blob
    assert "password" not in blob


def test_named_profile_artifact_generation_fails_without_ids() -> None:
    ctx = ExecutionContext(
        profile_name="repair",
        profile_source="flag",
        api_url="https://frontier.example.com",
        api_url_source="profile",
        creds=None,
        organization_id="",
        organization_name="Test Org",
        project_id="",
        project_name="test_frontier",
        dbt_target="dev",
        profiles_path=None,
        local=None,
        record=None,
    )
    try:
        assessment_identity_fields(ctx, dbt_target="dev", dbt_project="jaffle_shop")
    except InstallError as error:
        assert error.code == IDENTITY_INCOMPLETE_CODE
    else:
        raise AssertionError("expected incomplete identity")


def test_names_alone_do_not_permit_or_prevent_upload(tmp_path: Path, monkeypatch) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create

    cmd_profile_create(
        _args(
            name="repair",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: REPAIR_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    payload = {
        "project": "test_frontier",
        "assessmentIdentity": {
            "runnerVersion": "0.2.2",
            "invocationId": "run-1",
            "apiOrigin": "https://frontier.example.com",
            "organizationName": "Test Org",
            "projectName": "test_frontier",
            "frontierProfile": "repair",
        },
    }
    bench_ctx = resolve_execution_context(_args(frontier_profile="benchmark"), tmp_path)
    assert_artifact_matches_context(
        payload,
        bench_ctx,
        dbt_project="jaffle_shop",
        dbt_target="benchmark",
    )


def test_profile_status_shows_verified_ids(tmp_path: Path, monkeypatch, capsys) -> None:
    _patch_whoami(monkeypatch)
    from frontier.onboard.profile_commands import cmd_profile_create, cmd_profile_status

    cmd_profile_create(
        _args(
            name="benchmark",
            api_key=True,
            api_url="https://frontier.example.com",
            _getpass=lambda prompt: BENCH_KEY,
            force=True,
            project_dir=str(tmp_path),
        )
    )
    capsys.readouterr()
    assert cmd_profile_status(_args(frontier_profile="benchmark", project_dir=str(tmp_path))) == 0
    out = capsys.readouterr().out
    assert "Frontier profile: benchmark" in out
    assert "Organization: Test Org (org_a)" in out
    assert "Project: frontier_benchmark (project_b)" in out
    assert "Identity: verified" in out
    assert BENCH_KEY not in out
    assert "frn_" not in out

