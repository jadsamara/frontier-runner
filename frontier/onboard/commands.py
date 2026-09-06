from __future__ import annotations

import getpass
import json
import os
import shutil
import subprocess
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from frontier.credentials import (
    StoredCredentials,
    default_fallback_path,
    delete_credentials,
    key_prefix,
    resolve_api_credential,
    save_credentials,
    try_resolve_api_credential,
)
from frontier.dbt_artifacts import load_manifest
from frontier.errors import InstallError
from frontier.local_config import (
    LocalFrontierConfig,
    config_path,
    load_local_config,
    write_local_config,
)
from frontier.onboard.constants import DEFAULT_API_URL, DOCS_ORIGIN
from frontier.onboard.demo import demo_change_instructions
from frontier.onboard.detect import detect_project
from frontier.onboard.discover import ModelSuggestion, suggest_models
from frontier.onboard.doctor import doctor_failed, doctor_json, format_doctor, run_doctor
from frontier.onboard.gitignore import ensure_gitignore
from frontier.onboard.github import (
    SNOWFLAKE_SECRETS,
    render_workflow,
    validate_workflow_yaml,
    workflow_path,
    write_workflow,
)
from frontier.onboard.hashkey import generate_entity_hash_key, hash_key_prefix
from frontier.onboard.permissions import snowflake_permission_sql
from frontier.onboard.prompt import prompt_choice, prompt_text, prompt_yes_no
from frontier.onboard.saas import (
    fetch_runner_versions,
    upload_draft_manifest,
    whoami,
)
from frontier.onboard.versions import current_runner_version, version_at_least
from frontier.warehouse import load_dbt_profile_output

Reader = Callable[[str], str]


def _assume_yes(args: Any) -> bool:
    return bool(getattr(args, "yes", False) or getattr(args, "force", False))


def cmd_signup(args: Any) -> int:
    url = f"{(getattr(args, 'api_url', None) or DEFAULT_API_URL).rstrip('/')}/sign-up"
    print(f"Open {url} to create an organization and project.")
    print("Copy the project API key once, then run `frontier login --api-key`.")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    return 0


def cmd_login(args: Any) -> int:
    api_url = (getattr(args, "api_url", None) or DEFAULT_API_URL).rstrip("/")
    if not getattr(args, "api_key", False):
        print("Browser login is not available in this runner version.")
        print("Run: frontier login --api-key")
        print(f"Create a key at {api_url}/settings after signing in.")
        return 1
    reader = getattr(args, "_getpass", None) or getpass.getpass
    try:
        api_key = str(reader("Project API key: ")).strip()
    except (EOFError, KeyboardInterrupt) as error:
        raise InstallError(
            "AUTH_CANCELLED",
            "Login cancelled.",
            cause="No API key was entered.",
            next_action="Re-run `frontier login --api-key`.",
            docs_path="/docs/quick-start",
        ) from error
    if not api_key:
        raise InstallError(
            "AUTH_INVALID",
            "No API key was entered.",
            cause="Hidden input was empty.",
            next_action="Paste the project API key shown once at project creation.",
            docs_path="/docs/quick-start",
        )
    identity = whoami(api_url, api_key)
    creds = StoredCredentials(
        api_url=api_url,
        api_key=api_key,
        project=identity.project,
        organization=identity.organization,
    )
    backend = save_credentials(creds)
    print("Authenticated")
    print(f"Organization: {identity.organization or '(unknown)'}")
    print(f"Project: {identity.project}")
    print(f"API key: {identity.api_key_prefix}")
    if backend == "file":
        path = default_fallback_path()
        print(
            "Warning: secure keychain storage is unavailable. "
            f"Credentials were stored in {path} with permission 0600.",
        )
    return 0


def cmd_logout(_args: Any) -> int:
    delete_credentials()
    print("Signed out. Local API credentials were removed.")
    return 0


def cmd_auth_status(_args: Any) -> int:
    creds = try_resolve_api_credential()
    if not creds:
        print("Not authenticated")
        print("Next: frontier login --api-key")
        return 1
    try:
        identity = whoami(creds.api_url, creds.api_key)
    except InstallError:
        print("Credentials stored, but Frontier rejected them.")
        print(f"API key: {key_prefix(creds.api_key)}")
        print("Next: frontier login --api-key")
        return 1
    print("Authenticated")
    print(f"Organization: {identity.organization or creds.organization or '(unknown)'}")
    print(f"Project: {identity.project}")
    print(f"API key: {identity.api_key_prefix}")
    return 0


def cmd_init(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    detection = detect_project(project_dir)
    if detection.dbt_project_yml is None:
        raise InstallError(
            "DBT_PROJECT_MISSING",
            "No dbt_project.yml was found in this directory.",
            cause="frontier init must run from a dbt project root.",
            next_action="cd into the dbt project, then retry `frontier init`.",
            docs_path="/docs/quick-start",
        )
    assume = _assume_yes(args)
    project = detection.dbt_project_name or prompt_text(
        "dbt project name",
        default="jaffle_shop",
        assume="jaffle_shop" if assume else None,
    )
    target = detection.targets[0] if detection.targets else "dev"
    if len(detection.targets) > 1 and "dev" in detection.targets:
        target = "dev"
    if len(detection.targets) > 1 and not assume:
        target = prompt_choice("dbt target", list(detection.targets))
    branch = detection.default_branch or "main"
    api_url = prompt_text(
        "Frontier API URL",
        default=DEFAULT_API_URL,
        assume=DEFAULT_API_URL if assume else None,
    )
    path = config_path(project_dir)
    if path.exists() and not getattr(args, "force", False):
        if not prompt_yes_no(f"Overwrite {path}?", default=False, assume_yes=False):
            print(f"Kept existing {path}")
            ensure_gitignore(project_dir)
            return 0
    config = LocalFrontierConfig(
        project=project,
        api_url=api_url.rstrip("/") or DEFAULT_API_URL,
        dbt_project_dir=".",
        dbt_target=target,
        git_provider="github",
        default_branch=branch,
    )
    written = write_local_config(project_dir, config, force=True)
    ensure_gitignore(project_dir)
    print(f"Wrote {written}")
    if detection.github_origin:
        print(f"GitHub: {detection.github_origin}")
    if detection.adapter_type:
        print(f"Adapter: {detection.adapter_type}")
    print("Next: frontier login --api-key")
    return 0


def _ensure_manifest(project_dir: Path) -> Path:
    path = project_dir / "target" / "manifest.json"
    if path.is_file():
        return path
    dbt = shutil.which("dbt")
    if not dbt:
        raise InstallError(
            "DBT_ARTIFACT_MISSING",
            "No current dbt manifest was found.",
            cause="target/manifest.json is missing and dbt is not installed.",
            next_action="Run `dbt compile`, then retry `frontier discover`.",
            docs_path="/docs/troubleshooting#dbt-artifact-missing",
        )
    completed = subprocess.run(
        [dbt, "compile"],
        cwd=project_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0 or not path.is_file():
        raise InstallError(
            "DBT_ARTIFACT_MISSING",
            "No current dbt manifest was found.",
            cause=(completed.stderr or completed.stdout or "dbt compile failed")[:400],
            next_action="Run `dbt compile`, then retry `frontier discover`.",
            docs_path="/docs/troubleshooting#dbt-artifact-missing",
        )
    return path


def _select_suggestion(
    suggestions: list[ModelSuggestion],
    *,
    assume_yes: bool,
    model: str | None,
) -> ModelSuggestion:
    if model:
        for suggestion in suggestions:
            if suggestion.model == model:
                return suggestion
        raise InstallError(
            "DISCOVER_MODEL_UNKNOWN",
            f"Model '{model}' was not among the inferred marts.",
            cause="The name did not match a suggested target model.",
            next_action="Run `frontier discover` and choose a numbered model.",
            docs_path="/docs/semantic-manifest",
        )
    if len(suggestions) == 1 or assume_yes:
        return suggestions[0]
    options = [
        f"{item.model} (entity {item.entity}, key {item.entity_key}, {item.confidence})"
        for item in suggestions
    ]
    chosen = prompt_choice("Multiple plausible target models", options)
    index = options.index(chosen)
    return suggestions[index]


def _require_credentials() -> StoredCredentials:
    creds = resolve_api_credential()
    identity = whoami(creds.api_url, creds.api_key)
    return StoredCredentials(
        api_url=creds.api_url,
        api_key=creds.api_key,
        project=identity.project or creds.project,
        organization=identity.organization or creds.organization,
    )


def cmd_discover(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    manifest_path = _ensure_manifest(project_dir)
    manifest = load_manifest(manifest_path)
    suggestions = suggest_models(manifest)
    if not suggestions:
        raise InstallError(
            "DISCOVER_NO_MODELS",
            "No likely target marts were found in the dbt manifest.",
            cause="Staging/intermediate/demo models were skipped.",
            next_action="Name the mart you want assessed, then retry `frontier discover --model <name>`.",
            docs_path="/docs/semantic-manifest",
        )
    selected = _select_suggestion(
        suggestions,
        assume_yes=_assume_yes(args),
        model=getattr(args, "model", None),
    )
    grain_display = selected.grain.replace("_", " ")
    print(f"Detected model: {selected.model}")
    print(f"Suggested entity: {selected.entity}")
    print(f"Suggested key: {selected.entity_key}")
    print(f"Suggested grain: {grain_display}")
    print(f"Sources: {', '.join(source.name for source in selected.sources)}")
    print(f"Inferred confidence: {selected.confidence}")
    creds = _require_credentials()
    local = load_local_config(project_dir)
    if local and local.project and local.project != creds.project:
        print(
            f"Warning: local project '{local.project}' does not match "
            f"authenticated project '{creds.project}'. Uploading to '{creds.project}'.",
        )
    result = upload_draft_manifest(creds, selected.to_semantic_document())
    print(f"Draft manifest created: version {result.version}")
    print("This draft is not active. Review and activate it in Frontier before CI.")
    print(f"Review: {result.review_url}")
    return 0


def cmd_doctor(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    checks = run_doctor(project_dir, skip_warehouse=bool(getattr(args, "skip_warehouse", False)))
    if getattr(args, "json", False):
        print(json.dumps(doctor_json(checks), indent=2))
    else:
        print(format_doctor(checks))
    return 1 if doctor_failed(checks) else 0


def _gh(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["gh", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def _set_github_secret(name: str, value: str) -> None:
    completed = subprocess.run(
        ["gh", "secret", "set", name],
        input=value,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise InstallError(
            "GITHUB_SECRET_MISSING",
            f"{name} is not configured in GitHub Actions.",
            cause=(completed.stderr or "gh secret set failed")[:300],
            next_action="Run `frontier setup github` with a logged-in `gh` CLI.",
            docs_path="/docs/github",
        )


def _existing_github_secrets() -> set[str]:
    completed = _gh("secret", "list", "--json", "name")
    if completed.returncode != 0:
        return set()
    try:
        rows = json.loads(completed.stdout or "[]")
    except json.JSONDecodeError:
        return set()
    names = set()
    for row in rows:
        if isinstance(row, dict) and row.get("name"):
            names.add(str(row["name"]))
        elif isinstance(row, str):
            names.add(row)
    if names:
        return names
    return {line.split()[0] for line in completed.stdout.splitlines() if line.split()}


def cmd_setup_github(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    detection = detect_project(project_dir)
    if not detection.github_origin:
        raise InstallError(
            "GITHUB_REMOTE_MISSING",
            "No GitHub origin was detected.",
            cause="git remote origin is missing or is not github.com.",
            next_action="Set origin to a GitHub repository, then retry.",
            docs_path="/docs/github",
        )
    local = load_local_config(project_dir)
    creds = try_resolve_api_credential()
    assume = _assume_yes(args)
    blocking = bool(getattr(args, "blocking", False))
    path = workflow_path(project_dir, detection.git_root)
    if path.exists() and not getattr(args, "force", False):
        if not prompt_yes_no(
            f"Overwrite {path}?",
            default=False,
            assume_yes=assume,
        ):
            print(f"Kept existing {path}")
            return 0
    database, warehouse, schema = "DEV", "COMPUTE_WH", "DBT_CI"
    try:
        output = load_dbt_profile_output(
            project_dir,
            target=local.dbt_target if local else None,
        )
        database = str(output.get("database") or database)
        warehouse = str(output.get("warehouse") or warehouse)
        schema = str(output.get("schema") or schema)
    except Exception:
        pass
    text = render_workflow(
        runner_version=current_runner_version(),
        profile_name=detection.profile_name or detection.dbt_project_name or "dbt_project",
        default_branch=(local.default_branch if local else None) or detection.default_branch or "main",
        database=database,
        warehouse=warehouse,
        schema=schema,
        blocking=blocking,
    )
    write_workflow(path, text, force=True)
    validate_workflow_yaml(path.read_text())
    print(f"Workflow created: {path}")
    print("Commit this file and open a test PR.")

    generate_key = prompt_yes_no(
        "Generate a FRONTIER_ENTITY_HASH_KEY now?",
        default=True,
        assume_yes=assume,
    )
    hash_key = generate_entity_hash_key() if generate_key else None
    if hash_key:
        print(f"Entity hash key: {hash_key_prefix(hash_key)}")
        print("Rotating this key changes entity fingerprints across assessments.")
        print("The hash key is not sent to Frontier SaaS.")

    if shutil.which("gh") and prompt_yes_no(
        "Create GitHub Actions secrets with `gh`?",
        default=True,
        assume_yes=assume,
    ):
        existing = _existing_github_secrets()
        if creds:
            _set_github_secret("FRONTIER_API_URL", creds.api_url)
            _set_github_secret("FRONTIER_API_KEY", creds.api_key)
        else:
            print("Skipped FRONTIER_API_* secrets (not logged in).")
        if hash_key:
            _set_github_secret("FRONTIER_ENTITY_HASH_KEY", hash_key)
        snowflake_present = all(name in existing for name in SNOWFLAKE_SECRETS)
        replace = True
        if snowflake_present:
            replace = prompt_yes_no(
                "Snowflake secrets already exist. Replace them?",
                default=False,
                assume_yes=False,
            )
        if replace and not snowflake_present:
            print(
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD "
                "in GitHub Actions (values are not printed).",
            )
        print("GitHub secrets updated (values not printed).")
    elif hash_key and prompt_yes_no(
        "Copy the entity hash key to the clipboard?",
        default=False,
        assume_yes=False,
    ):
        _copy_to_clipboard(hash_key)
        print("Copied. Paste it into the FRONTIER_ENTITY_HASH_KEY GitHub secret.")
    print(f"Docs: {DOCS_ORIGIN}/docs/github")
    return 0


def _copy_to_clipboard(value: str) -> None:
    for command in (("pbcopy",), ("xclip", "-selection", "clipboard"), ("wl-copy",)):
        if shutil.which(command[0]):
            subprocess.run(command, input=value, check=False, text=True)
            return
    raise InstallError(
        "CLIPBOARD_UNAVAILABLE",
        "Could not copy the hash key to the clipboard.",
        cause="No pbcopy, xclip, or wl-copy command was found.",
        next_action="Create the GitHub secret with `gh secret set FRONTIER_ENTITY_HASH_KEY`.",
        docs_path="/docs/security",
    )


def cmd_setup_hash_key(args: Any) -> int:
    key = generate_entity_hash_key()
    print(f"Entity hash key: {hash_key_prefix(key)}")
    print("Rotating this key changes entity fingerprints across assessments.")
    print("The hash key is not sent to Frontier SaaS.")
    assume = _assume_yes(args)
    if shutil.which("gh") and prompt_yes_no(
        "Store as GitHub secret FRONTIER_ENTITY_HASH_KEY?",
        default=True,
        assume_yes=assume,
    ):
        _set_github_secret("FRONTIER_ENTITY_HASH_KEY", key)
        print("Stored FRONTIER_ENTITY_HASH_KEY (value not printed).")
        return 0
    if getattr(args, "copy", False) or prompt_yes_no(
        "Copy the complete key to the clipboard?",
        default=False,
        assume_yes=False,
    ):
        _copy_to_clipboard(key)
        print("Copied. Paste it into GitHub Actions secrets.")
    if getattr(args, "print_key", False):
        print(key)
    return 0


def cmd_demo_change(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    detection = detect_project(project_dir)
    print(demo_change_instructions(project_dir, detection.dbt_project_name))
    print("Frontier will not commit or push these changes.")
    return 0


def cmd_update_check(args: Any) -> int:
    local = None
    project_dir = Path(getattr(args, "project_dir_opt", None) or getattr(args, "project_dir", ".") or ".").expanduser().resolve()
    try:
        local = load_local_config(project_dir)
    except Exception:
        local = None
    creds = try_resolve_api_credential()
    api_url = (getattr(args, "api_url", None) or (local.api_url if local else None) or (creds.api_url if creds else DEFAULT_API_URL))
    versions = fetch_runner_versions(str(api_url))
    current = current_runner_version()
    print(f"Installed: {current}")
    print(f"Minimum supported: {versions.minimum_supported}")
    print(f"Latest stable: {versions.latest_stable}")
    if not version_at_least(current, versions.minimum_supported):
        raise InstallError(
            "RUNNER_UNSUPPORTED",
            f"This runner ({current}) is below the minimum supported version ({versions.minimum_supported}).",
            cause="The installed CLI is too old for this Frontier SaaS.",
            next_action=f'pipx install "frontier-runner[snowflake]=={versions.latest_stable}"',
            docs_path="/docs/troubleshooting#runner-version",
        )
    if parse_notice_needed(versions.latest_stable) and current != versions.latest_stable:
        print(f"A newer runner is available: {versions.latest_stable}")
        print("Frontier does not auto-update. Upgrade when you are ready.")
    return 0


def parse_notice_needed(latest: str) -> bool:
    cache = Path.home() / ".cache" / "frontier" / "update-check"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if cache.is_file():
        raw = cache.read_text().strip()
        if raw.startswith(today):
            return False
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(f"{today} {latest}\n")
    return True


def maybe_version_notice(api_url: str) -> None:
    if os.environ.get("FRONTIER_HIDE_UPDATE_NOTICE") == "1":
        return
    try:
        versions = fetch_runner_versions(api_url)
        current = current_runner_version()
        if version_at_least(current, versions.latest_stable):
            return
        if not parse_notice_needed(versions.latest_stable):
            return
        print(
            f"Notice: frontier {versions.latest_stable} is available "
            f"(installed {current}). Run `frontier update-check`.",
            file=sys.stderr,
        )
    except Exception:
        return


def cmd_permissions(args: Any) -> int:
    project_dir = Path(getattr(args, "project_dir_opt", None) or args.project_dir or ".").expanduser().resolve()
    local = load_local_config(project_dir)
    database, schema, warehouse = "DEV", "DBT_DEV", "COMPUTE_WH"
    try:
        output = load_dbt_profile_output(
            project_dir,
            target=local.dbt_target if local else None,
        )
        database = str(output.get("database") or database)
        schema = str(output.get("schema") or schema)
        warehouse = str(output.get("warehouse") or warehouse)
    except Exception:
        pass
    print(
        snowflake_permission_sql(
            database=database,
            schema=schema,
            warehouse=warehouse,
            include_cdc=bool(getattr(args, "cdc", False)),
            include_query_history=bool(getattr(args, "query_history", False)),
        ),
        end="",
    )
    return 0
