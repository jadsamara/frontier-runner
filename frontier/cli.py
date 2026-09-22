from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Sequence

from frontier import __version__
from frontier.api import (
    build_ingest_payload,
    redact_api_key,
    upload_run,
)
from frontier.credentials import resolve_api_credential, try_resolve_api_credential
from frontier.comment import maybe_upsert_pr_comment
from frontier.config import (
    ConfigError,
    FrontierConfig,
    load_frontier_config,
    saas_runtime_config,
    should_recommend_rebuild,
    sql_change_rebuild_recommended_pct,
    write_init_config,
)
from frontier.artifacts import require_current_artifacts
from frontier.compare import (
    compare_manifests,
    comparison_for_ingest,
    compiled_sql_pair_for_sql_change,
    format_compare_report,
    sql_change_reference_relation,
    stamp_impact_execution,
)
from frontier.github import base_commit_sha, default_external_run_id, env_flag, github_source
from frontier.dbt_artifacts import (
    format_inspect_report,
    inspect_report,
    load_manifest,
    load_run_results,
)
from frontier.frontier import (
    FrontierResult,
    current_frontier_metrics_sql,
    frontier_result_to_dict,
    load_change_events_csv,
    percent_rows_avoided,
    run_frontier,
)
from frontier.execute import (
    affected_keys_relation,
    generate_targeted_sql,
    isolated_location,
    open_isolated_run,
    snapshot_execute,
    sql_change_impact_queries,
)
from frontier.certification import (
    FULL_REBUILD_RECOMMENDED,
    build_assessment_dimensions,
    enrich_certification_record,
    filter_v1_sql_certified,
)
from frontier.hashing import entity_hash_key_from_env, hmac_entity_id
from frontier.environment import ENVIRONMENT_MISMATCH, assess_artifact_environment
from frontier.impact import CANDIDATE_SET_ANALYSIS_FAILED, CANDIDATE_SET_EMPTY, CANDIDATE_SET_NONEMPTY, evaluate_discovery_counts
from frontier.sql_fingerprint import sql_dialect
from frontier.snapshot import collect_source_relations
from frontier.progress import (
    configure_stdio,
    elapsed_ms,
    failure_status,
    log_step,
    redact_failure_reason,
)
from frontier.cdc.config import cdc_config_path, load_cdc_config, overlay_cdc_with_manifest
from frontier.cdc.consume import consume_all, project_name_for
from frontier.semantic import (
    PinnedSemanticManifest,
    allow_local_manifest,
    default_pin_path,
    fetch_active_manifest,
    pin_manifest,
    print_semantic_manifest,
    resolve_semantic_manifest,
    validate_pinned_against_dbt,
    validate_pinned_document,
)
from frontier.cdc.prove import prove_batch
from frontier.cdc.store import SnowflakeCdcStore
from frontier.cdc.upload import upload_cdc_batch
from frontier.proof import (
    apply_resolved_delete,
    measure_mutation_proof,
    measure_sql_change_proof,
    mutation_source_key,
    proof_validation_results,
    recorded_proof,
    recorded_sql_change_affected,
    recorded_sql_change_proof,
    recommended_sql_change_proof,
    required_sql_change_proof,
    failed_execution_sql_change_proof,
    resolve_deleted_order,
    sql_change_proof_validation_results,
)
from frontier.warehouse import (
    FakeWarehouse,
    WarehouseAdapter,
    connect_warehouse,
    describe_adapter,
    load_dbt_profile_output,
    normalize_warehouse_type,
)
from frontier.onboard.commands import (
    cmd_auth_status,
    cmd_demo_change,
    cmd_discover,
    cmd_doctor,
    cmd_init as cmd_onboard_init,
    cmd_login,
    cmd_logout,
    cmd_permissions,
    cmd_setup_github,
    cmd_setup_hash_key,
    cmd_signup,
    cmd_update_check,
    maybe_version_notice,
)
from frontier.onboard.constants import DEFAULT_API_URL
from frontier.onboard.routes import (
    derive_source_route,
    impact_returns_entity_key,
    sql_change_required_sources,
)
from frontier.local_config import load_local_config
from frontier.validation import (
    ValidationResult,
    collect_validation_results,
    evidence_level,
    overall_status,
    sql_change_narrow_frontier_result,
    SQL_CHANGE_NARROW_FRONTIER,
)

RUN_FILE_NAME = "frontier-run.json"
INVOCATION_FILE_NAME = "frontier-run.invocation"

PHASE_ARTIFACT = "artifact comparison"
PHASE_IMPACT = "impact-query execution"
PHASE_MATERIALIZE = "candidate materialization"
PHASE_TARGET_BASE = "targeted base execution"
PHASE_TARGET_HEAD = "targeted head execution"
PHASE_CONFIRM = "confirmation"
PHASE_UPLOAD = "upload"


def _filter_v1_executable(comparison: dict[str, Any] | None) -> bool:
    eligibility = (comparison or {}).get("staticEligibility") or {}
    if eligibility.get("eligible") is True and eligibility.get("compiled") is True:
        return True
    for row in (comparison or {}).get("modified") or []:
        row_eligibility = row.get("staticEligibility") or {}
        if row_eligibility.get("eligible") is True and row_eligibility.get("compiled") is True:
            return True
    return False


def _invocation_path(run_file: Path) -> Path:
    return run_file.with_name(INVOCATION_FILE_NAME)


def _write_invocation_stamp(run_file: Path, run_id: str) -> None:
    run_file.parent.mkdir(parents=True, exist_ok=True)
    _invocation_path(run_file).write_text(run_id.strip() + "\n")


def _read_invocation_stamp(run_file: Path) -> str:
    path = _invocation_path(run_file)
    if not path.is_file():
        return ""
    return path.read_text().strip()


def _redacted_failure_reason(error: BaseException) -> str:
    text = redact_failure_reason(error)
    return text[:512] if text else type(error).__name__


def _collect_warehouse_job(warehouse: Any, phase: str) -> dict[str, Any] | None:
    query_id = getattr(warehouse, "last_query_id", None)
    if not query_id:
        return None
    profile: dict[str, Any] = {}
    getter = getattr(warehouse, "get_query_profile", None)
    if callable(getter):
        try:
            profile = dict(getter(str(query_id)) or {})
        except Exception:
            profile = {}
    elapsed = profile.get("elapsed_ms")
    if elapsed is None:
        elapsed = profile.get("total_elapsed_ms")
    return {
        "phase": phase,
        "query_id": str(query_id),
        "elapsed_ms": elapsed,
        "bytes_scanned": profile.get("bytes_scanned"),
        "total_bytes_processed": profile.get("total_bytes_processed"),
        "cloud_services_credits": profile.get("cloud_services_credits"),
    }


def _sum_job_bytes(isolated: Any | None, extra: Sequence[dict[str, Any]] | None = None) -> int | None:
    records = list(extra or [])
    records.extend(getattr(isolated, "job_metrics", None) or [])
    total = 0
    found = False
    for record in records:
        value = record.get("bytes_scanned")
        if value is None:
            value = record.get("total_bytes_processed")
        if value is not None:
            total += int(value)
            found = True
    return total if found else None


def _reraise_prove_failure(error: BaseException, *, phase: str, code: str) -> None:
    if isinstance(error, ConfigError):
        raise error
    raise ConfigError(f"{phase}: {code}: {_redacted_failure_reason(error)}") from error


def _write_failed_prove_run(
    args: argparse.Namespace,
    *,
    config,
    run_id: str,
    error: BaseException,
    sql_comparison: dict[str, Any] | None,
    manifest,
    phase: str,
    code: str,
) -> Path | None:
    """Overwrite frontier-run.json with this invocation's failed assessment."""
    output = Path(args.output) if getattr(args, "output", None) else _target_dir(_project_dir(args)) / RUN_FILE_NAME
    args.run_id = run_id
    reason = _redacted_failure_reason(error)
    phase_name = (phase or "execution")[:64]
    code_name = (code or "EXECUTION_FAILED")[:64]
    text = str(error)
    if isinstance(error, ConfigError) and ENVIRONMENT_MISMATCH in text:
        code_name = ENVIRONMENT_MISMATCH
    result = FrontierResult(
        full_entity_count=1,
        frontier_entity_count=1,
        percent_rows_avoided=0.0,
        change_events=[],
        affected_entities=[],
        frontier_sql="",
        metrics_sql="",
        full_rebuild_required=True,
        execution_failed=True,
        failure_phase=phase_name,
        failure_code=code_name,
        failure_reason=reason[:512],
        proof_status="EXECUTION_FAILED",
        execution_reasons=(f"{code_name}: {reason}",),
    )
    comparison = dict(sql_comparison) if sql_comparison else None
    if comparison is not None:
        comparison["fullRebuildRequired"] = True
        comparison["narrowFrontierSafe"] = False
    validations = [
        ValidationResult(
            test_name="assert_frontier_execution",
            status="failed",
            difference_count=1,
            message=f"{phase_name}: {code_name}: {reason[:400]}",
        )
    ]
    try:
        path = _emit_run(
            args,
            config=config,
            manifest=manifest,
            result=result,
            validations=validations,
            sql_comparison=comparison,
        )
        print(f"Wrote failed assessment to {path}", flush=True)
        return path
    except Exception as write_error:
        try:
            if output.is_file():
                output.unlink()
            _write_invocation_stamp(output, run_id)
        except OSError:
            pass
        print(
            f"Could not write failed assessment ({type(write_error).__name__}); "
            "invalidated the previous run file.",
            flush=True,
        )
        return None


def _assessment_identity(
    *,
    run_id: str,
    dbt_target: str | None = None,
    profile_database: str | None = None,
    profile_schema: str | None = None,
    pinned_version: int | None = None,
    pinned_fingerprint: str | None = None,
) -> dict[str, Any]:
    identity: dict[str, Any] = {
        "runnerVersion": __version__,
        "invocationId": run_id,
    }
    if dbt_target:
        identity["dbtTarget"] = dbt_target
    if profile_database:
        identity["profileDatabase"] = profile_database
    if profile_schema:
        identity["profileSchema"] = profile_schema
    if pinned_version is not None:
        identity["pinnedManifestVersion"] = pinned_version
    if pinned_fingerprint:
        identity["pinnedManifestFingerprint"] = pinned_fingerprint
    return identity


def _print_job_metrics(
    isolated: Any | None,
    warehouse: Any,
    extra: Sequence[dict[str, Any]] | None = None,
) -> None:
    records = list(extra or [])
    records.extend(getattr(isolated, "job_metrics", None) or [])
    if not records:
        query_id = getattr(warehouse, "last_query_id", None)
        if query_id:
            records = [{"phase": "warehouse", "query_id": query_id}]
    for record in records:
        job_id = record.get("query_id") or record.get("job_id")
        phase = record.get("phase") or "job"
        if job_id:
            print(f"{phase} job: {job_id}", flush=True)
        status = record.get("status")
        if status:
            print(f"{phase} status: {status}", flush=True)
        elapsed = record.get("elapsed_ms")
        if elapsed is not None:
            print(f"{phase} elapsed: {elapsed} ms", flush=True)
        queue_ms = record.get("queue_ms")
        if queue_ms is not None:
            print(f"{phase} queue: {queue_ms} ms", flush=True)
        elif "queue_ms" in record:
            print(f"{phase} queue: unavailable", flush=True)
        execution_ms = record.get("execution_ms")
        if execution_ms is not None:
            print(f"{phase} execution: {execution_ms} ms", flush=True)
        elif "execution_ms" in record:
            print(f"{phase} execution: unavailable", flush=True)
        bytes_processed = record.get("total_bytes_processed")
        if bytes_processed is None:
            bytes_processed = record.get("bytes_scanned")
        if bytes_processed is not None:
            print(
                f"{phase} bytes processed: {bytes_processed} "
                "(warehouse processing metric, not candidate count or cost savings)",
                flush=True,
            )
        elif record.get("metrics_available") is False:
            print(f"{phase} scan metrics: unavailable", flush=True)


def _assert_cdc_supported(warehouse: Any) -> None:
    kind = str(getattr(warehouse, "warehouse_type", "") or "")
    if kind != "snowflake":
        label = kind or "this warehouse"
        raise ConfigError(
            f"CDC is not available for {label}. Frontier CDC requires Snowflake Streams.",
        )


def _pluralize_entity(entity: str) -> str:
    token = (entity or "entity").strip() or "entity"
    if token.endswith("s"):
        return token
    return f"{token}s"


def _print_phase(name: str, duration_ms: int | None = None, *, skipped: str | None = None) -> None:
    if skipped:
        print(f"{name}: skipped ({skipped})", flush=True)
        return
    print(f"{name}: {duration_ms} ms", flush=True)


def _project_dir(args: argparse.Namespace) -> Path:
    flag = getattr(args, "project_dir_opt", None)
    value = flag or getattr(args, "project_dir", ".") or "."
    return Path(value).expanduser().resolve()


def _config_path(args: argparse.Namespace, project_dir: Path) -> Path:
    if getattr(args, "config", None):
        return Path(args.config).expanduser().resolve()
    return project_dir / "frontier.yml"


def _api_url(args: argparse.Namespace, config=None, creds=None, local=None) -> str:
    return str(
        getattr(args, "api_url", None)
        or os.environ.get("FRONTIER_API_URL")
        or (creds.api_url if creds and getattr(creds, "api_url", None) else None)
        or (local.api_url if local and getattr(local, "api_url", None) else None)
        or (getattr(config, "api_url", None) if config is not None else None)
        or DEFAULT_API_URL
    ).rstrip("/")


def _saas_project_name(project_dir: Path, creds=None, local=None) -> str:
    if creds and getattr(creds, "project", None):
        return str(creds.project).strip()
    if local and local.project:
        return local.project
    return project_dir.name


def _resolve_runtime_config(
    args: argparse.Namespace,
    project_dir: Path,
    *,
    dbt_manifest=None,
):
    local = load_local_config(project_dir)
    explicit = getattr(args, "manifest_file", None)
    creds = None if explicit else try_resolve_api_credential()
    api_key = creds.api_key if creds else None
    use_local_yml = allow_local_manifest(args) and not api_key and not explicit
    legacy_path = _config_path(args, project_dir)
    legacy = None
    if use_local_yml:
        legacy = load_frontier_config(legacy_path)
    elif legacy_path.is_file() and not api_key and not explicit:
        # Explicit --config without SaaS credentials still loads the legacy mapping.
        if getattr(args, "config", None):
            legacy = load_frontier_config(legacy_path)
    api_url = _api_url(args, legacy, creds, local)
    if legacy is not None:
        config = legacy
    else:
        config = saas_runtime_config(
            project=_saas_project_name(project_dir, creds, local),
            api_url=api_url,
            environment=local.dbt_target if local else "dev",
        )
    config, _pinned = resolve_semantic_manifest(
        args,
        project_dir=project_dir,
        config=config,
        dbt_manifest=dbt_manifest,
        api_url=api_url,
        api_key=api_key,
    )
    model_name = (getattr(args, "model", None) or "").strip()
    if model_name:
        config = replace(config, model=replace(config.model, name=model_name))
    return config


def _pinned_compare_fields(config) -> tuple[int | None, str | None]:
    pinned = getattr(config, "pinned", None) if config is not None else None
    version = getattr(pinned, "version", None)
    fingerprint = getattr(pinned, "fingerprint", None)
    if not isinstance(version, int):
        version = None
    if not isinstance(fingerprint, str) or not fingerprint.strip():
        fingerprint = None
    return version, fingerprint


def _ingest_manifest_fields(config) -> dict[str, Any]:
    pinned = getattr(config, "pinned", None)
    if not isinstance(pinned, PinnedSemanticManifest):
        return {}
    if pinned.source == "local_override":
        return {"manifest_source": "local_override"}
    return {
        "semantic_manifest_id": pinned.id,
        "semantic_manifest_version": pinned.version,
        "semantic_manifest_fingerprint": pinned.fingerprint,
        "manifest_source": pinned.source,
    }


def _target_dir(project_dir: Path) -> Path:
    return project_dir / "target"


def _compiled_root_for(manifest_path: Path) -> Path:
    return manifest_path.parent / "compiled"


def _load_sql_comparison(
    args: argparse.Namespace,
    *,
    pr_manifest,
    project_dir: Path,
    config: FrontierConfig | None = None,
) -> dict[str, Any] | None:
    base_path = getattr(args, "base_manifest", None)
    if not base_path:
        return None
    base_manifest_path = Path(base_path).expanduser().resolve()
    base_manifest = load_manifest(base_manifest_path)
    pr_path = pr_manifest.path or (_target_dir(project_dir) / "manifest.json")
    runtime = config
    if runtime is None:
        config_path = _config_path(args, project_dir)
        if config_path.is_file():
            try:
                runtime = load_frontier_config(config_path)
            except ConfigError:
                runtime = None
    entity_key = runtime.model.key if runtime else None
    confirmed_keys = (
        tuple(
            dict.fromkeys(
                [
                    runtime.model.key,
                    *[relation.change_key for relation in runtime.relations.values()],
                ]
            )
        )
        if runtime
        else ()
    )
    target_name = runtime.model.name if runtime else None
    manifest_version, manifest_fingerprint = _pinned_compare_fields(runtime)
    comparison = compare_manifests(
        base_manifest,
        pr_manifest,
        base_compiled_root=_compiled_root_for(base_manifest_path),
        pr_compiled_root=_compiled_root_for(Path(pr_path)),
        base_commit_sha=base_commit_sha(),
        pr_commit_sha=(os.environ.get("GITHUB_SHA") or "").strip() or None,
        entity_key=entity_key,
        confirmed_keys=confirmed_keys,
        target_name=target_name,
        semantic_manifest_version=manifest_version,
        semantic_manifest_fingerprint=manifest_fingerprint,
    )
    return comparison.to_dict()


def cmd_init(args: argparse.Namespace) -> int:
    if getattr(args, "legacy_yml", False):
        project_dir = _project_dir(args)
        path = Path(args.config).expanduser().resolve() if args.config else project_dir / "frontier.yml"
        write_init_config(path, force=args.force)
        print(f"Wrote {path}")
        return 0
    return cmd_onboard_init(args)


def cmd_inspect(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    config = _resolve_runtime_config(args, project_dir, dbt_manifest=manifest)
    if manifest.project_name != config.project:
        if config.path is not None:
            raise ConfigError(
                f"frontier.yml project '{config.project}' does not match manifest '{manifest.project_name}'",
            )
        print(
            f"Warning: dbt project '{manifest.project_name}' does not match "
            f"Frontier project '{config.project}'.",
            flush=True,
        )
    report = inspect_report(manifest, config.model.name)
    print(format_inspect_report(report))
    comparison = _load_sql_comparison(
        args,
        pr_manifest=manifest,
        project_dir=project_dir,
        config=config,
    )
    if comparison:
        print()
        print(format_compare_report(comparison))

    missing = [
        name
        for name in config.relations
        if all(node.name != name for node in manifest.nodes.values() if node.resource_type == "model")
    ]
    if missing:
        raise ConfigError(f"Configured relations not in the manifest: {', '.join(missing)}")
    print("\nSemantic routes:")
    print(f"  Target: {config.model.name}")
    print(f"  Entity: {config.model.entity}")
    print(f"  Key: {config.model.key}")
    pinned = getattr(config, "pinned", None)
    target_node = None
    try:
        target_node = manifest.find_model(config.model.name)
    except Exception:
        target_node = None
    source_items = list(pinned.sources) if pinned is not None else []
    relation_names = list(config.relations)
    names = [item.name for item in source_items] or relation_names
    sql_blockers = 0
    cdc_blockers = 0
    for name in names:
        relation = config.relations.get(name)
        source = next((item for item in source_items if item.name == name), None)
        status = source.route_status if source else "unknown"
        change_key = (source.change_key if source else None) or (
            relation.change_key if relation else ""
        )
        node = None
        try:
            node = manifest.find_model(name)
        except Exception:
            node = None
        reason = ""
        if source and source.evidence:
            reason = source.evidence[0]
        columns = set(getattr(node, "columns", ()) or ()) if node else set()
        if node and change_key and columns and change_key not in columns and change_key != "unresolved":
            status = "INVALID"
            reason = f"column {change_key} does not exist"
        if source and source.join_route == "direct" and columns and config.model.key not in columns:
            status = "INVALID"
            reason = f"column {config.model.key} does not exist"
        sql_blocker = bool(source and source.sql_change_blocker)
        cdc_blocker = bool(source and (source.cdc_blocker or status != "VERIFIED"))
        if sql_blocker:
            sql_blockers += 1
        if cdc_blocker:
            cdc_blockers += 1
        print(f"  {name}")
        print(f"    change key: {change_key or 'unknown'}")
        print(f"    status: {status}")
        if reason:
            print(f"    reason: {reason}")
        if source and source.evidence and len(source.evidence) > 1:
            print(f"    evidence: {source.evidence[0]}")
        derived = None
        if node is not None and target_node is not None:
            try:
                derived = derive_source_route(
                    manifest,
                    node,
                    target_node,
                    entity_key=config.model.key,
                )
            except Exception:
                derived = None
        if derived is not None:
            print(f"    derived key: {derived.change_key}")
            if derived.route_path:
                path = " → ".join(f"{hop.model}.{hop.column}" for hop in derived.route_path)
                print(f"    derived route: {path}")
            elif derived.join_route and derived.join_route != "direct":
                print(f"    derived route: {derived.join_route.replace(' -> ', ' → ')}")
        elif source and source.route_path:
            path = " → ".join(
                f"{hop.get('model')}.{hop.get('column')}" if isinstance(hop, dict) else f"{hop.model}.{hop.column}"
                for hop in source.route_path
            )
            print(f"    derived route: {path}")
        print(f"    SQL-change blocker: {'yes' if sql_blocker else 'no'}")
        print(f"    CDC blocker when {name} changes: {'yes' if cdc_blocker else 'no'}")
        if relation is not None:
            print(f"    route: {relation.route.kind}")
    print(f"  SQL-change ready: {'yes' if sql_blockers == 0 else 'no'}")
    adapter = (manifest.adapter_type or "").lower()
    if adapter in {"bigquery", "redshift"}:
        print(f"  CDC ready: unavailable ({adapter})")
    else:
        print(f"  CDC ready: {'yes' if cdc_blockers == 0 else 'no'}")
    return 0


def _use_dry_run(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "dry_run", False)) or env_flag("FRONTIER_DRY_RUN")


def _run_mode(args: argparse.Namespace) -> str:
    return "fixture" if _use_dry_run(args) else "live"


def _candidate_set_origin(
    change_events: list[Any],
    sql_comparison: dict[str, Any] | None,
) -> str | None:
    has_sql = _sql_change_present(sql_comparison)
    has_events = bool(change_events)
    if has_sql and has_events:
        return "union"
    if has_sql:
        return "sql_change"
    if has_events:
        return "event"
    return None


def _wants_blocking(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "blocking", False)) or env_flag("FRONTIER_BLOCKING")


def _proof_sql_pair(
    args: argparse.Namespace,
    *,
    config,
    manifest,
    project_dir: Path,
    sql_comparison: dict[str, Any] | None,
) -> tuple[str | None, str | None]:
    pr_root = _compiled_root_for(_target_dir(project_dir) / "manifest.json")
    base_manifest = None
    base_root = None
    base_path = getattr(args, "base_manifest", None)
    if base_path:
        resolved = Path(base_path).expanduser().resolve()
        base_manifest = load_manifest(resolved)
        base_root = _compiled_root_for(resolved)
    return compiled_sql_pair_for_sql_change(
        target_name=config.model.name,
        pr_manifest=manifest,
        base_manifest=base_manifest,
        pr_compiled_root=pr_root,
        base_compiled_root=base_root,
        sql_comparison=sql_comparison,
    )


def _sql_change_present(comparison: dict[str, Any] | None) -> bool:
    if not comparison:
        return False
    return bool(
        comparison.get("modified") or comparison.get("added") or comparison.get("removed")
    )


def _load_events(args: argparse.Namespace, project_dir: Path, sql_comparison: dict[str, Any] | None) -> list:
    explicit = bool(getattr(args, "events", None))
    path = Path(args.events) if args.events else project_dir / "seeds" / "change_events.csv"
    if _sql_change_present(sql_comparison) and not explicit:
        return []
    return load_change_events_csv(
        path,
        required=explicit or not _sql_change_present(sql_comparison),
    )


def _sql_change_queries(
    comparison: dict[str, Any] | None,
    *,
    persist: bool,
    target_name: str | None = None,
) -> tuple[tuple[str, ...], bool]:
    queries, required = sql_change_impact_queries(comparison, target_name=target_name)
    if not persist:
        return (), False
    return queries, required


def _impact_attempted(result) -> bool:
    if getattr(result, "sql_change_candidate_count", None) is not None:
        return True
    return any("failed" in str(reason).lower() for reason in getattr(result, "execution_reasons", ()))


def _stamp_sql_comparison(args: argparse.Namespace, comparison: dict[str, Any] | None, result) -> dict[str, Any] | None:
    stamped = stamp_impact_execution(
        comparison,
        run_mode=_run_mode(args),
        full_rebuild_required=bool(getattr(result, "full_rebuild_required", False)),
        sql_change_executed=getattr(result, "sql_change_candidate_count", None) is not None
        or getattr(result, "changed_source_row_count", None) is not None,
        impact_attempted=_impact_attempted(result),
        proof_status=getattr(result, "proof_status", None),
        failure_phase=getattr(result, "failure_phase", None),
        failure_code=getattr(result, "failure_code", None),
        failure_reason=getattr(result, "failure_reason", None),
    )
    if stamped is None:
        return None
    eligibility = stamped.get("staticEligibility") or {}
    for row in stamped.get("modified") or []:
        row_eligibility = row.get("staticEligibility") or {}
        if row_eligibility.get("eligible") or row_eligibility.get("candidateFingerprint"):
            eligibility = {**eligibility, **row_eligibility}
            break
    compiled = bool(eligibility.get("compiled") and eligibility.get("candidateFingerprint"))
    if not compiled:
        for row in stamped.get("modified") or []:
            if row.get("queryFingerprint") and (row.get("staticEligibility") or {}).get("compiled"):
                compiled = True
                eligibility = {**eligibility, **(row.get("staticEligibility") or {})}
                if row.get("queryFingerprint"):
                    eligibility.setdefault("candidateFingerprint", row["queryFingerprint"])
                break
    confirmed = getattr(result, "proof_status", None) == "CONFIRMED"
    execution_failed = bool(getattr(result, "execution_failed", False))
    eligible = eligibility.get("eligible") is True
    snapshot = getattr(result, "source_snapshot", None)
    static_certified = filter_v1_sql_certified(
        eligible=eligible,
        compiled=compiled,
        confirmed=confirmed,
        execution_failed=execution_failed,
    )
    dimensions = build_assessment_dimensions(
        snapshot=snapshot,
        static_certified=static_certified,
        execution_failed=execution_failed,
        execution_ran=bool(
            getattr(result, "sql_change_candidate_count", None) is not None
            or getattr(result, "changed_source_row_count", None) is not None
            or getattr(result, "proof_status", None)
            in {
                "CONFIRMED",
                "CANDIDATES_EXECUTED",
                "TARGETED_BASE_EXECUTED",
                "TARGETED_HEAD_EXECUTED",
            }
        ),
        full_rebuild_recommended=bool(getattr(result, "full_rebuild_recommended", False)),
        targeted_ran=getattr(result, "proof_status", None)
        in {"CONFIRMED", "TARGETED_HEAD_EXECUTED", "TARGETED_BASE_EXECUTED"},
        frontier_bytes=getattr(result, "frontier_bytes_scanned", None),
        full_comparison_bytes=getattr(result, "full_comparison_bytes_scanned", None),
        warehouse_credits=getattr(result, "warehouse_credits", None),
        candidates_confirmed=confirmed,
        confirmation_failed=getattr(result, "failure_phase", None) == "CONFIRMED",
        full_reference_validated=bool(getattr(result, "full_reference_validated", False)),
        full_reference_failed=getattr(result, "failure_phase", None) == "FULL_REFERENCE",
        failure_phase=getattr(result, "failure_phase", None) if execution_failed else None,
        failure_code=getattr(result, "failure_code", None) if execution_failed else None,
        failure_reason=getattr(result, "failure_reason", None) if execution_failed else None,
    )
    enrich_certification_record(
        dimensions,
        eligibility=eligibility,
        snapshot=snapshot,
        candidate_fingerprint=eligibility.get("candidateFingerprint"),
        execution_failed=execution_failed,
    )
    stamped.update(dimensions)
    if (stamped.get("economics") or {}).get("decision") == FULL_REBUILD_RECOMMENDED:
        stamped["fullRebuildRecommended"] = True
    _stamp_candidate_set_state(
        stamped,
        compiled=compiled,
        execution_failed=execution_failed,
        candidate_count=getattr(result, "sql_change_candidate_count", None),
    )
    return stamped


def _stamp_candidate_set_state(
    comparison: dict[str, Any],
    *,
    compiled: bool,
    execution_failed: bool,
    candidate_count: int | None,
) -> None:
    """Empty is genuine only after successful compile and execution."""
    if execution_failed:
        for row in comparison.get("modified") or []:
            if row.get("candidateSetState") == CANDIDATE_SET_EMPTY and not compiled:
                row["candidateSetState"] = CANDIDATE_SET_ANALYSIS_FAILED
        return
    if not compiled or candidate_count is None:
        return
    state = CANDIDATE_SET_EMPTY if candidate_count == 0 else CANDIDATE_SET_NONEMPTY
    for row in comparison.get("modified") or []:
        if row.get("queryFingerprint") or (row.get("staticEligibility") or {}).get("compiled"):
            if row.get("candidateSetState") in {None, "not_evaluated"}:
                row["candidateSetState"] = state


def _apply_rebuild_to_comparison(
    comparison: dict[str, Any] | None,
    result,
    validations: list,
) -> dict[str, Any] | None:
    updated = dict(comparison) if comparison is not None else None
    if getattr(result, "full_rebuild_recommended", False):
        if updated is None:
            updated = {}
        updated["fullRebuildRecommended"] = True
    if not getattr(result, "full_rebuild_required", False):
        return updated if updated is not None else comparison
    if updated is not None:
        updated["fullRebuildRequired"] = True
        updated["narrowFrontierSafe"] = False
        return updated
    validations.append(
        ValidationResult(
            test_name=SQL_CHANGE_NARROW_FRONTIER,
            status="failed",
            difference_count=1,
            message="; ".join(result.execution_reasons) or "FULL_REBUILD_REQUIRED",
        )
    )
    return comparison


def _format_measured(value: Any) -> str:
    return "Not measured" if value is None else str(value)


def _print_assessment_dimensions(comparison: dict[str, Any] | None) -> None:
    if not comparison:
        return
    certification = comparison.get("certification") or {}
    if certification.get("status"):
        line = f"Certification: {certification.get('status')}"
        if certification.get("failureCode"):
            line += f" ({certification.get('failureCode')})"
        print(line)
        if certification.get("oldPlanFingerprint"):
            print(f"Old plan fingerprint: {certification.get('oldPlanFingerprint')}")
        if certification.get("newPlanFingerprint"):
            print(f"New plan fingerprint: {certification.get('newPlanFingerprint')}")
        if certification.get("changedFilterNodeId"):
            print(f"Changed filter node: {certification.get('changedFilterNodeId')}")
        if certification.get("candidateQueryFingerprint"):
            print(f"Candidate query fingerprint: {certification.get('candidateQueryFingerprint')}")
    validation = comparison.get("validation") or {}
    if validation.get("status"):
        print(f"Validation status: {validation.get('status')}")
    economics = comparison.get("economics") or {}
    if economics.get("decision"):
        print(f"Economics: {economics.get('decision')}")
    execution = comparison.get("execution") or {}
    if execution.get("status"):
        print(f"Execution: {execution.get('status')}")
    snapshot = comparison.get("sourceSnapshot") or {}
    if snapshot:
        print(
            "Source snapshot: "
            f"{snapshot.get('mode') or 'none'} "
            f"{snapshot.get('assurance') or 'NONE'} "
            f"{snapshot.get('identifier') or ''}".strip()
        )
    boundary = comparison.get("baselineBoundary") or {}
    if boundary:
        print(
            "Baseline: this assessment compares two SQL versions at one pinned source snapshot; "
            "the existing materialized dbt mart is not that snapshot; "
            "in-place production repair is not certified."
        )


def _print_origin_counts(result) -> None:
    if result.event_candidate_count is not None:
        print(f"Event candidates: {result.event_candidate_count}")
    if result.sql_change_candidate_count is not None:
        print(f"SQL-change candidates: {result.sql_change_candidate_count}")
    if result.union_candidate_count is not None:
        print(f"Union candidates: {result.union_candidate_count}")
    if result.full_rebuild_required:
        print("Impact: FULL_REBUILD_REQUIRED")
    elif getattr(result, "full_rebuild_recommended", False):
        print("Impact: FULL_REBUILD_RECOMMENDED")


def _write_run_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _emit_run(
    args: argparse.Namespace,
    *,
    config,
    manifest,
    result,
    validations,
    extra_metrics: dict[str, Any] | None = None,
    sql_comparison: dict[str, Any] | None = None,
) -> Path:
    include_entity_ids = args.include_entity_ids or config.upload.include_entity_ids
    hash_entity_ids = args.hash_entity_ids or config.upload.hash_entity_ids
    send_raw_ids = include_entity_ids and not hash_entity_ids
    hash_key = None if send_raw_ids else entity_hash_key_from_env()
    details = frontier_result_to_dict(
        result,
        config=config,
        include_entity_ids=send_raw_ids,
        hash_key=hash_key,
    )
    model = manifest.find_model(config.model.name)
    if not model.database or not model.schema:
        raise ConfigError("Manifest model is missing database/schema")
    run_id = args.run_id or default_external_run_id(config.project)
    metrics = dict(details["metrics"])
    if extra_metrics:
        metrics.update(extra_metrics)
    sql_comparison = stamp_impact_execution(
        sql_comparison,
        run_mode=_run_mode(args),
        full_rebuild_required=bool(getattr(result, "full_rebuild_required", False)),
        sql_change_executed=getattr(result, "sql_change_candidate_count", None) is not None,
        impact_attempted=_impact_attempted(result),
        proof_status=getattr(result, "proof_status", None),
        failure_phase=getattr(result, "failure_phase", None),
        failure_code=getattr(result, "failure_code", None),
        failure_reason=getattr(result, "failure_reason", None),
    )
    sql_comparison = _stamp_sql_comparison(args, sql_comparison, result)
    sql_check = sql_change_narrow_frontier_result(sql_comparison)
    if sql_check is not None:
        validations.append(sql_check)
    payload = build_ingest_payload(
        external_run_id=run_id,
        project=config.project,
        environment=config.environment,
        database=str(model.database),
        schema=str(model.schema),
        model_unique_id=model.unique_id,
        model_name=config.model.name,
        entity_type=config.model.entity,
        entity_key=config.model.key,
        grain=config.model.grain,
        metrics=metrics,
        change_events=details["changeEvents"],
        affected_entities=details["affectedEntities"],
        validation_results=[
            {
                "testName": item.test_name,
                "status": item.status,
                "differenceCount": item.difference_count,
                **({"message": item.message} if item.message else {}),
            }
            for item in validations
        ],
        evidence_level=evidence_level(validations),
        status=overall_status(validations),
        git=github_source(),
        entity_ids_hashed=not send_raw_ids,
        warehouse_type=normalize_warehouse_type(manifest.adapter_type),
        sql_comparison=comparison_for_ingest(sql_comparison),
        run_mode=_run_mode(args),
        candidate_set_origin=_candidate_set_origin(
            details["changeEvents"],
            sql_comparison,
        ),
        **_ingest_manifest_fields(config),
        runner_version=__version__,
        dbt_target=getattr(args, "target", None) or getattr(config, "environment", None),
        assessment_identity=_assessment_identity(
            run_id=run_id,
            dbt_target=getattr(args, "target", None) or getattr(config, "environment", None),
            profile_database=str(model.database) if model.database else None,
            profile_schema=str(model.schema) if model.schema else None,
            pinned_version=_pinned_compare_fields(config)[0],
            pinned_fingerprint=_pinned_compare_fields(config)[1],
        ),
    )
    output = Path(args.output) if args.output else _target_dir(_project_dir(args)) / RUN_FILE_NAME
    _write_run_file(output, payload)
    return output


def cmd_run(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    config = _resolve_runtime_config(args, project_dir, dbt_manifest=manifest)
    run_results = load_run_results(_target_dir(project_dir) / "run_results.json")
    sql_comparison = _load_sql_comparison(
        args,
        pr_manifest=manifest,
        project_dir=project_dir,
        config=config,
    )
    events = _load_events(args, project_dir, sql_comparison)

    dry_run = _use_dry_run(args)
    warehouse: WarehouseAdapter
    run_id = args.run_id or default_external_run_id(config.project)
    persist = not dry_run
    if dry_run:
        warehouse = FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3)],
                "order_id in (1)": [(36901,)],
                "order_id in (-1)": [],
                "difference_count": [(0,)],
            }
        )
        print("Using in-memory warehouse (--dry-run). No live warehouse session.")
    else:
        warehouse = connect_warehouse(
            project_dir,
            profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
            target=args.target,
        )
        print(f"{warehouse.warehouse_type}: " + json.dumps(describe_adapter(warehouse)))

    base_sql, after_sql = _proof_sql_pair(
        args,
        config=config,
        manifest=manifest,
        project_dir=project_dir,
        sql_comparison=sql_comparison,
    )

    try:
        sql_change_queries, sql_change_required = _sql_change_queries(
            sql_comparison,
            persist=persist,
            target_name=config.model.name,
        )
        source_snapshot = None
        if persist:
            model = manifest.find_model(config.model.name)
            capture = getattr(warehouse, "capture_snapshot", None)
            if callable(capture):
                source_snapshot = capture(
                    collect_source_relations(
                        *sql_change_queries,
                        base_sql or "",
                        after_sql or "",
                        f"select * from {model.relation}",
                        dialect=warehouse.dialect,
                    )
                )
        result = run_frontier(
            config,
            manifest=manifest,
            events=events,
            warehouse=warehouse,
            run_id=run_id,
            persist=persist,
            sql_change_queries=sql_change_queries,
            sql_change_required=sql_change_required,
            before_sql=base_sql,
            after_sql=after_sql,
            source_snapshot=source_snapshot,
        )
        validations = collect_validation_results(
            config=config,
            manifest=manifest,
            run_results=run_results,
            events=events,
            result=result,
            warehouse=None if dry_run else warehouse,
        )
    finally:
        warehouse.close()

    sql_comparison = _stamp_sql_comparison(
        args,
        _apply_rebuild_to_comparison(sql_comparison, result, validations),
        result,
    )
    output = _emit_run(
        args,
        config=config,
        manifest=manifest,
        result=result,
        validations=validations,
        sql_comparison=sql_comparison,
    )
    print(f"Full entities: {result.full_entity_count}")
    print(f"Frontier: {result.frontier_entity_count}")
    print(f"Rows avoided: {result.percent_rows_avoided}%")
    _print_origin_counts(result)
    _print_assessment_dimensions(sql_comparison)
    print("Validation:")
    for item in validations:
        print(f"  - {item.test_name}: {item.status} (differences={item.difference_count})")
    print(f"Wrote {output}")
    return 0


def _warehouse_location() -> tuple[str, str]:
    database = (os.environ.get("FRONTIER_WAREHOUSE_DATABASE") or "DATA_AGENT_DEV").strip()
    schema = (os.environ.get("FRONTIER_WAREHOUSE_SCHEMA") or "DBT_DEV").strip()
    return database, schema


def cmd_record_failure(args: argparse.Namespace) -> int:
    """Write a failed assessment without reading dbt artifacts.

    Used when dbt build fails so CI cannot upload a previous manifest or
    run_results.json as if it belonged to this commit.
    """
    project_dir = _project_dir(args)
    config = _resolve_runtime_config(args, project_dir)
    include_entity_ids = args.include_entity_ids or config.upload.include_entity_ids
    hash_entity_ids = args.hash_entity_ids or config.upload.hash_entity_ids
    send_raw_ids = include_entity_ids and not hash_entity_ids
    hash_key = None if send_raw_ids else entity_hash_key_from_env()
    reason = (args.reason or "").strip() or "dbt build failed; no current artifacts"
    sentinel = "unavailable"
    commit = (os.environ.get("GITHUB_SHA") or "").strip() or sentinel

    def maybe_hash(*, entity_type: str, entity_key: str, value: str) -> str:
        if hash_key is None:
            return value
        return hmac_entity_id(
            hash_key,
            project=config.project,
            entity_type=entity_type,
            entity_key=entity_key,
            value=value,
        )

    database, schema = _warehouse_location()
    payload = build_ingest_payload(
        external_run_id=args.run_id or default_external_run_id(config.project),
        project=config.project,
        environment=config.environment,
        database=database,
        schema=schema,
        model_unique_id=f"model.{config.project}.{config.model.name}",
        model_name=config.model.name,
        entity_type=config.model.entity,
        entity_key=config.model.key,
        grain=config.model.grain,
        metrics={
            "fullEntityCount": 1,
            "frontierEntityCount": 0,
            "percentRowsAvoided": 100.0,
        },
        change_events=[
            {
                "eventId": "ci_dbt_build",
                "sourceModel": "dbt_build",
                "operation": "update",
                "entityKey": "commit_sha",
                "entityValue": maybe_hash(
                    entity_type="commit",
                    entity_key="commit_sha",
                    value=commit,
                ),
            }
        ],
        affected_entities=[
            {
                "entityType": config.model.entity,
                "entityKey": config.model.key,
                "entityValue": maybe_hash(
                    entity_type=config.model.entity,
                    entity_key=config.model.key,
                    value=sentinel,
                ),
                "reason": reason,
            }
        ],
        validation_results=[
            {
                "testName": "dbt_build",
                "status": "failed",
                "differenceCount": 1,
                "message": reason,
            }
        ],
        evidence_level="none",
        status="failed",
        git=github_source(),
        entity_ids_hashed=not send_raw_ids,
        warehouse_type=normalize_warehouse_type(
            os.environ.get("FRONTIER_WAREHOUSE_TYPE") or "snowflake",
        ),
        run_mode="live",
        candidate_set_origin="event",
        **_ingest_manifest_fields(config),
    )
    output = Path(args.output) if args.output else _target_dir(project_dir) / RUN_FILE_NAME
    _write_run_file(output, payload)
    print(f"Wrote failed assessment to {output}")
    return 0


def _generate_targeted_sql_pair(
    *,
    before_sql: str,
    after_sql: str,
    entity_key: str,
    run_id: str,
    model_database: str | None,
    model_schema: str | None,
    dialect: str = "snowflake",
) -> None:
    database, schema = isolated_location(
        model_database=model_database,
        model_schema=model_schema,
        dialect=dialect,
    )
    relation = affected_keys_relation(
        run_id,
        database=database,
        schema=schema,
        dialect=dialect,
    )
    generate_targeted_sql(
        before_sql,
        entity_key=entity_key,
        affected_relation=relation,
        dialect=dialect,
    )
    generate_targeted_sql(
        after_sql,
        entity_key=entity_key,
        affected_relation=relation,
        dialect=dialect,
    )


def cmd_prove(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    config = _resolve_runtime_config(args, project_dir, dbt_manifest=manifest)
    run_results = load_run_results(_target_dir(project_dir) / "run_results.json")
    run_id = args.run_id or default_external_run_id(config.project)
    args.run_id = run_id
    output = Path(args.output) if args.output else _target_dir(project_dir) / RUN_FILE_NAME
    _write_invocation_stamp(output, run_id)
    if output.is_file():
        output.unlink()
    log_step("compare started")
    started = time.perf_counter()
    try:
        sql_comparison = _load_sql_comparison(
            args,
            pr_manifest=manifest,
            project_dir=project_dir,
            config=config,
        )
    except Exception as error:
        log_step(
            "compare completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        _write_failed_prove_run(
            args,
            config=config,
            run_id=run_id,
            error=error,
            sql_comparison=None,
            manifest=manifest,
            phase="artifact comparison",
            code="EXECUTION_FAILED",
        )
        _reraise_prove_failure(error, phase="artifact comparison", code="EXECUTION_FAILED")
    log_step("compare completed", duration_ms=elapsed_ms(started), status="ok")
    _print_phase(PHASE_ARTIFACT, elapsed_ms(started))
    events = _load_events(args, project_dir, sql_comparison)
    sql_change_demo = _sql_change_present(sql_comparison)

    log_step("canonical predicate selection started")
    started = time.perf_counter()
    try:
        selected_queries, sql_change_required = sql_change_impact_queries(
            sql_comparison,
            target_name=config.model.name,
        )
    except Exception as error:
        log_step(
            "canonical predicate selection completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step(
        "canonical predicate selection completed",
        duration_ms=elapsed_ms(started),
        status=f"ok queries={len(selected_queries)}",
    )

    dry_run = _use_dry_run(args)
    if dry_run and os.environ.get("GITHUB_ACTIONS") == "true":
        raise ConfigError(
            "FRONTIER_DRY_RUN is not allowed in GitHub Actions prove. "
            "Customer CI must execute against DATA_AGENT_DEV.DBT_CI."
        )
    persist = not dry_run
    sql_change_queries = selected_queries if persist else ()
    sql_proof = None
    proof = None
    warehouse: WarehouseAdapter | None = None
    base_sql, after_sql = _proof_sql_pair(
        args,
        config=config,
        manifest=manifest,
        project_dir=project_dir,
        sql_comparison=sql_comparison,
    )
    skip_certified = persist and not _filter_v1_executable(sql_comparison)
    if skip_certified:
        sql_change_queries = ()
    impact_unavailable = sql_change_required and not selected_queries
    dialect = sql_dialect(manifest.adapter_type) or "snowflake"
    if dry_run:
        log_step("Snowflake connector import started")
        log_step("Snowflake connector import completed", status="skipped:dry-run")
        log_step("Snowflake connection started")
        log_step("Snowflake connected", status="skipped:dry-run")
        warehouse = FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3 if not sql_change_demo else 12)],
                "order_id in (1)": [(36901,)],
                "order_id in (5)": [(781,)],
                "difference_count": [(0,)],
            }
        )
        print("Using in-memory warehouse (--dry-run). No live warehouse session.", flush=True)
        if sql_change_demo:
            sql_proof = recorded_sql_change_proof()
        else:
            proof = recorded_proof()
            events = apply_resolved_delete(
                events,
                order_id=proof.deleted_order_id,
                customer_id=proof.deleted_order_customer_id,
            )
    elif skip_certified:
        log_step("Snowflake connector import started")
        log_step("Snowflake connection started")
        live: WarehouseAdapter | None = None
        try:
            live = connect_warehouse(
                project_dir,
                profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
                target=args.target,
            )
            profile: dict[str, Any] = {}
            try:
                profile = load_dbt_profile_output(
                    project_dir,
                    profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
                    target=args.target,
                )
            except ConfigError:
                profile = {}
            model = manifest.find_model(config.model.name)
            env = assess_artifact_environment(
                base_sql=base_sql or "",
                pr_sql=after_sql or "",
                candidate_sql="",
                base_database=model.database,
                pr_database=model.database,
                profile_database=str(profile.get("database") or ""),
                dialect=live.dialect or dialect,
            )
            if env.ok:
                warehouse = live
                live = None
                dialect = warehouse.dialect or dialect
                print(
                    f"{warehouse.warehouse_type}: " + json.dumps(describe_adapter(warehouse)),
                    flush=True,
                )
                log_step("Snowflake connected", status="ok:metrics-only uncertified")
                print(
                    "Skipping targeted warehouse execution: filter-v1 certification rejected "
                    "this plan; legacy impact SQL is diagnostic only and must not be executed.",
                    flush=True,
                )
            else:
                log_step(
                    "Snowflake connected",
                    status="skipped:environment mismatch uncertified",
                )
                print(
                    "Skipping warehouse execution: filter-v1 certification rejected this plan; "
                    f"{env.reason or ENVIRONMENT_MISMATCH}. "
                    "legacy impact SQL is diagnostic only.",
                    flush=True,
                )
                warehouse = FakeWarehouse(
                    {"full_entity_count": [(1, 1)], "difference_count": [(0,)]}
                )
        except Exception as error:
            log_step(
                "Snowflake connected",
                status=failure_status(error),
            )
            warehouse = FakeWarehouse(
                {"full_entity_count": [(1, 1)], "difference_count": [(0,)]}
            )
            print(
                "Skipping warehouse execution: filter-v1 certification rejected this plan; "
                "legacy impact SQL is diagnostic only.",
                flush=True,
            )
        finally:
            if live is not None:
                live.close()
    else:
        try:
            warehouse = connect_warehouse(
                project_dir,
                profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
                target=args.target,
            )
        except Exception as error:
            _write_failed_prove_run(
                args,
                config=config,
                run_id=run_id,
                error=error,
                sql_comparison=sql_comparison,
                manifest=manifest,
                phase="warehouse connection",
                code="EXECUTION_FAILED",
            )
            _reraise_prove_failure(error, phase="warehouse connection", code="EXECUTION_FAILED")
        print(f"{warehouse.warehouse_type}: " + json.dumps(describe_adapter(warehouse)), flush=True)
        dialect = warehouse.dialect or dialect
        if persist and sql_change_queries:
            profile = {}
            try:
                profile = load_dbt_profile_output(
                    project_dir,
                    profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
                    target=args.target,
                )
            except ConfigError:
                profile = {}
            model = manifest.find_model(config.model.name)
            env = assess_artifact_environment(
                base_sql=base_sql or "",
                pr_sql=after_sql or "",
                candidate_sql=sql_change_queries[0] if sql_change_queries else "",
                base_database=model.database,
                pr_database=model.database,
                profile_database=str(profile.get("database") or ""),
                dialect=dialect,
            )
            if not env.ok:
                warehouse.close()
                _write_failed_prove_run(
                    args,
                    config=config,
                    run_id=run_id,
                    error=ConfigError(env.reason or ENVIRONMENT_MISMATCH),
                    sql_comparison=sql_comparison,
                    manifest=manifest,
                    phase="environment check",
                    code=ENVIRONMENT_MISMATCH,
                )
                raise ConfigError(env.reason or ENVIRONMENT_MISMATCH)
        if not sql_change_demo:
            try:
                deleted_order_id, deleted_customer_id = resolve_deleted_order(
                    manifest,
                    warehouse,
                    proof=config.proof,
                    source_key=mutation_source_key(config),
                    entity_key=config.model.key,
                )
            except ConfigError as error:
                warehouse.close()
                _write_failed_prove_run(
                    args,
                    config=config,
                    run_id=run_id,
                    error=error,
                    sql_comparison=sql_comparison,
                    manifest=manifest,
                    phase="mutation proof",
                    code="EXECUTION_FAILED",
                )
                _reraise_prove_failure(error, phase="mutation proof", code="EXECUTION_FAILED")
            events = apply_resolved_delete(
                events,
                order_id=deleted_order_id,
                customer_id=deleted_customer_id,
            )
    if persist and selected_queries:
        changed_models = [
            str(row.get("name") or "")
            for row in ((sql_comparison or {}).get("modified") or [])
        ]
        pinned = getattr(config, "pinned", None)
        required_routes = sql_change_required_sources(
            pinned.sources if pinned is not None else (),
            impact_sql=selected_queries[0],
            entity_key=config.model.key,
            changed_models=changed_models,
        )
        if sql_comparison is not None:
            sql_comparison = dict(sql_comparison)
            sql_comparison["requiredRouteIds"] = list(required_routes)
        if required_routes and not impact_returns_entity_key(
            selected_queries[0],
            config.model.key,
        ):
            raise ConfigError(
                "ROUTE_UNRESOLVED: SQL-change requires verified routes for "
                + ", ".join(required_routes)
            )
    log_step("targeted SQL generation started")
    started = time.perf_counter()
    if impact_unavailable or skip_certified:
        log_step(
            "targeted SQL generation completed",
            duration_ms=elapsed_ms(started),
            status="skipped:filter-v1 uncertified" if skip_certified else "skipped:impact SQL unavailable",
        )
    elif base_sql and after_sql:
        try:
            model = manifest.find_model(config.model.name)
            _generate_targeted_sql_pair(
                before_sql=base_sql,
                after_sql=after_sql,
                entity_key=config.model.key,
                run_id=run_id,
                model_database=model.database,
                model_schema=model.schema,
                dialect=warehouse.dialect if warehouse is not None else dialect,
            )
        except Exception as error:
            log_step(
                "targeted SQL generation completed",
                duration_ms=elapsed_ms(started),
                status=failure_status(error),
            )
            if warehouse is not None:
                warehouse.close()
            _write_failed_prove_run(
                args,
                config=config,
                run_id=run_id,
                error=error,
                sql_comparison=sql_comparison,
                manifest=manifest,
                phase="targeted SQL generation",
                code="EXECUTION_FAILED",
            )
            _reraise_prove_failure(error, phase="targeted SQL generation", code="EXECUTION_FAILED")
        log_step(
            "targeted SQL generation completed",
            duration_ms=elapsed_ms(started),
            status="ok",
        )
    else:
        log_step(
            "targeted SQL generation completed",
            duration_ms=elapsed_ms(started),
            status="skipped",
        )

    isolated = None
    prove_jobs: list[dict[str, Any]] = []
    rebuild_recommended = False
    discovered_source_rows: int | None = None
    discovered_candidates: int | None = None
    source_snapshot = None
    if persist and warehouse is not None and sql_change_queries:
        model = manifest.find_model(config.model.name)
        capture = getattr(warehouse, "capture_snapshot", None)
        if callable(capture):
            relations = collect_source_relations(
                *(sql_change_queries or ()),
                base_sql or "",
                after_sql or "",
                f"select * from {model.relation}",
                dialect=warehouse.dialect,
            )
            source_snapshot = capture(relations)
            log_step(
                "source snapshot captured",
                status=getattr(source_snapshot, "assurance", "NONE"),
            )
    elif persist and not sql_change_queries:
        log_step(
            "source snapshot captured",
            status="skipped:filter-v1 uncertified",
        )
    try:
        if persist and sql_change_demo and sql_change_queries:
            log_step("impact query submission started")
            started = time.perf_counter()
            log_step("impact query submitted")
            try:
                discovered_source_rows, discovered_candidates = evaluate_discovery_counts(
                    sql_change_queries[0],
                    config.model.key,
                    warehouse,
                    snapshot=source_snapshot,
                )
                job = _collect_warehouse_job(warehouse, "impact-query")
                if job:
                    prove_jobs.append(job)
            except Exception as error:
                log_step(
                    "impact query completed",
                    duration_ms=elapsed_ms(started),
                    status=failure_status(error),
                )
                raise
            log_step(
                "impact query completed",
                duration_ms=elapsed_ms(started),
                status="ok",
            )
            _print_phase(PHASE_IMPACT, elapsed_ms(started))
            log_step(f"candidate count calculated {discovered_candidates}")
            print(f"SQL-change candidates: {discovered_candidates}", flush=True)
            print(f"Changed source rows: {discovered_source_rows}", flush=True)
            log_step("threshold decision started")
            started = time.perf_counter()
            metrics_sql = current_frontier_metrics_sql(
                manifest,
                config.model.name,
                dialect=warehouse.dialect,
            )
            try:
                metric_rows = snapshot_execute(
                    warehouse,
                    metrics_sql,
                    source_snapshot,
                    phase="discovery",
                )
                job = _collect_warehouse_job(warehouse, "threshold-metrics")
                if job:
                    prove_jobs.append(job)
            except Exception as error:
                log_step(
                    "threshold decision completed",
                    duration_ms=elapsed_ms(started),
                    status=failure_status(error),
                )
                raise
            full_count = int(metric_rows[0][0]) if metric_rows else 0
            threshold = sql_change_rebuild_recommended_pct(config)
            rebuild_recommended = should_recommend_rebuild(
                discovered_candidates,
                full_count,
                threshold,
            )
            if rebuild_recommended:
                share = discovered_candidates / full_count * 100 if full_count else 0
                log_step(
                    "threshold decision completed",
                    duration_ms=elapsed_ms(started),
                    status="FULL_REBUILD_RECOMMENDED",
                )
                print(
                    f"Candidate set is {share:.1f}% of "
                    f"{full_count} entities (threshold {threshold:g}%). "
                    "FULL_REBUILD_RECOMMENDED; skipping targeted proof.",
                    flush=True,
                )
                log_step("candidate materialization started")
                log_step(
                    "candidate materialization completed",
                    status="skipped:FULL_REBUILD_RECOMMENDED",
                )
                log_step("targeted base execution started")
                log_step(
                    "targeted base execution completed",
                    status="skipped:FULL_REBUILD_RECOMMENDED",
                )
                log_step("targeted head execution started")
                log_step(
                    "targeted head execution completed",
                    status="skipped:FULL_REBUILD_RECOMMENDED",
                )
                log_step("confirmation started")
                log_step("confirmation completed", status="skipped:FULL_REBUILD_RECOMMENDED")
                _print_phase(PHASE_MATERIALIZE, skipped="FULL_REBUILD_RECOMMENDED")
                _print_phase(PHASE_TARGET_BASE, skipped="FULL_REBUILD_RECOMMENDED")
                _print_phase(PHASE_TARGET_HEAD, skipped="FULL_REBUILD_RECOMMENDED")
                _print_phase(PHASE_CONFIRM, skipped="FULL_REBUILD_RECOMMENDED")
            else:
                log_step(
                    "threshold decision completed",
                    duration_ms=elapsed_ms(started),
                    status="targeted-proof",
                )
        elif persist and sql_change_demo and sql_change_required and not sql_change_queries:
            log_step("impact query submission started")
            log_step("impact query submitted", status="skipped")
            log_step("impact query completed", status="skipped:impact SQL unavailable")
            log_step("candidate count calculated 0", status="skipped")
            log_step("threshold decision completed", status="skipped:impact SQL unavailable")
            log_step("candidate materialization started")
            log_step(
                "candidate materialization completed",
                status="skipped:FULL_REBUILD_REQUIRED",
            )
            log_step("targeted base execution started")
            log_step(
                "targeted base execution completed",
                status="skipped:FULL_REBUILD_REQUIRED",
            )
            log_step("targeted head execution started")
            log_step(
                "targeted head execution completed",
                status="skipped:FULL_REBUILD_REQUIRED",
            )
            log_step("confirmation started")
            log_step("confirmation completed", status="skipped:FULL_REBUILD_REQUIRED")
            _print_phase(PHASE_IMPACT, skipped="impact SQL unavailable")
            _print_phase(PHASE_MATERIALIZE, skipped="FULL_REBUILD_REQUIRED")
            _print_phase(PHASE_TARGET_BASE, skipped="FULL_REBUILD_REQUIRED")
            _print_phase(PHASE_TARGET_HEAD, skipped="FULL_REBUILD_REQUIRED")
            _print_phase(PHASE_CONFIRM, skipped="FULL_REBUILD_REQUIRED")
        elif sql_change_demo:
            log_step("impact query submission started")
            log_step("impact query submitted", status="skipped:dry-run")
            log_step("impact query completed", status="skipped:dry-run")
            candidate_count = sql_proof.candidate_frontier_count if sql_proof else 0
            log_step(
                f"candidate count calculated {candidate_count}",
                status="skipped:dry-run",
            )
            print(f"SQL-change candidates: {candidate_count}", flush=True)
            log_step("threshold decision started")
            log_step("threshold decision completed", status="skipped:dry-run")
            log_step("candidate materialization started")
            log_step("candidate materialization completed", status="skipped:dry-run")
            log_step("targeted base execution started")
            log_step("targeted base execution completed", status="skipped:dry-run")
            log_step("targeted head execution started")
            log_step("targeted head execution completed", status="skipped:dry-run")
            log_step("confirmation started")
            log_step("confirmation completed", status="skipped:dry-run")
            log_step("SQL-change proof started")
            log_step("SQL-change proof completed", status="skipped:dry-run")
            log_step("cleanup started")
            log_step("cleanup completed", status="skipped:dry-run")
            _print_phase(PHASE_IMPACT, skipped="dry-run")
            _print_phase(PHASE_MATERIALIZE, skipped="dry-run")
            _print_phase(PHASE_TARGET_BASE, skipped="dry-run")
            _print_phase(PHASE_TARGET_HEAD, skipped="dry-run")
            _print_phase(PHASE_CONFIRM, skipped="dry-run")
        skip_targeted = rebuild_recommended or (persist and impact_unavailable)
        if persist and not skip_targeted:
            model = manifest.find_model(config.model.name)
            isolated = open_isolated_run(
                warehouse,
                run_id=run_id,
                entity_key=config.model.key,
                model_database=model.database,
                model_schema=model.schema,
                model_relation=model.relation,
                snapshot=source_snapshot,
            )
        result = run_frontier(
            config,
            manifest=manifest,
            events=events,
            warehouse=warehouse,
            run_id=run_id,
            persist=persist and not skip_targeted,
            sql_change_queries=() if skip_targeted else sql_change_queries,
            sql_change_required=False if skip_targeted else sql_change_required,
            before_sql=base_sql,
            after_sql=after_sql,
            isolated_run=isolated,
            confirm=not skip_targeted,
            full_rebuild_recommended=rebuild_recommended,
            source_snapshot=source_snapshot,
        )
        if source_snapshot is not None:
            result.source_snapshot = source_snapshot
        if discovered_candidates is not None:
            result.changed_source_row_count = discovered_source_rows
            if result.sql_change_candidate_count is None:
                result.sql_change_candidate_count = discovered_candidates
            if result.union_candidate_count is None:
                result.union_candidate_count = discovered_candidates
        if isolated is not None and sql_change_demo:
            for phase in (PHASE_MATERIALIZE, PHASE_TARGET_BASE, PHASE_TARGET_HEAD, PHASE_CONFIRM):
                if phase in isolated.phase_timings:
                    _print_phase(phase, isolated.phase_timings[phase])
        if rebuild_recommended:
            result.sql_change_candidate_count = discovered_candidates
            result.union_candidate_count = discovered_candidates
            result.event_candidate_count = result.event_candidate_count or 0
            result.changed_source_row_count = discovered_source_rows
            result.full_rebuild_recommended = True
            if discovered_candidates is not None:
                frontier_count = min(discovered_candidates, result.full_entity_count)
                result.frontier_entity_count = frontier_count
                result.percent_rows_avoided = percent_rows_avoided(
                    result.full_entity_count,
                    frontier_count,
                )
        if persist and impact_unavailable:
            result.full_rebuild_required = True
            result.proof_status = "FULL_REBUILD_REQUIRED"
            result.failure_phase = "FULL_REBUILD_REQUIRED"
            result.failure_code = "IMPACT_SQL_UNAVAILABLE"
            result.failure_reason = "SQL impact query unavailable"
            reasons = list(result.execution_reasons)
            if not any("unavailable" in reason.lower() for reason in reasons):
                reasons.append("SQL impact query unavailable")
            result.execution_reasons = tuple(reasons)
            result.frontier_entity_count = result.full_entity_count
            result.percent_rows_avoided = percent_rows_avoided(
                result.full_entity_count,
                result.full_entity_count,
            )
        skip_proof_metrics = (
            rebuild_recommended
            or result.full_rebuild_required
            or bool(getattr(result, "execution_failed", False))
        )
        if sql_change_demo:
            if sql_proof is None:
                if rebuild_recommended:
                    log_step("SQL-change proof started")
                    sql_proof = recommended_sql_change_proof(
                        full_entity_count=result.full_entity_count,
                        candidate_count=discovered_candidates or 0,
                        changed_source_row_count=discovered_source_rows or 0,
                    )
                    log_step("SQL-change proof completed", status="skipped:FULL_REBUILD_RECOMMENDED")
                    result.proof_status = "FULL_REBUILD_RECOMMENDED"
                elif getattr(result, "execution_failed", False):
                    log_step("SQL-change proof started")
                    sql_proof = failed_execution_sql_change_proof(
                        full_entity_count=result.full_entity_count,
                        candidate_count=discovered_candidates
                        or result.union_candidate_count
                        or 0,
                        changed_source_row_count=discovered_source_rows or 0,
                    )
                    phase = result.failure_phase or "EXECUTION_FAILED"
                    code = result.failure_code or "EXECUTION_FAILED"
                    log_step(
                        "SQL-change proof completed",
                        status=f"skipped:EXECUTION_FAILED {phase} {code}",
                    )
                    result.proof_status = "EXECUTION_FAILED"
                    print(
                        f"Execution failed at {phase}: {code}: {result.failure_reason or 'unknown'}",
                        flush=True,
                    )
                elif result.full_rebuild_required:
                    log_step("SQL-change proof started")
                    sql_proof = required_sql_change_proof(
                        full_entity_count=result.full_entity_count,
                    )
                    log_step("SQL-change proof completed", status="skipped:FULL_REBUILD_REQUIRED")
                else:
                    if not result.affected_relation or not base_sql or not after_sql:
                        raise ConfigError("SQL-change proof requires compiled base/PR SQL and affected keys")
                    confirmed_count = result.confirmed_count
                    if confirmed_count is None and result.confirmed_keys is not None:
                        confirmed_count = len(result.confirmed_keys)
                    log_step("SQL-change proof started")
                    started = time.perf_counter()
                    try:
                        sql_proof = measure_sql_change_proof(
                            config,
                            warehouse=warehouse,
                            before_sql=base_sql,
                            after_sql=after_sql,
                            affected_relation=result.affected_relation,
                            impact_sql=sql_change_queries[0] if sql_change_queries else None,
                            candidate_count=result.union_candidate_count,
                            confirmed_count=confirmed_count,
                            full_rebuild_required=result.full_rebuild_required,
                            targeted_before_relation=isolated.targeted_base_relation if isolated else None,
                            targeted_after_relation=isolated.targeted_head_relation if isolated else None,
                            reference_relation=sql_change_reference_relation(
                                target_name=config.model.name,
                                pr_manifest=manifest,
                                sql_comparison=sql_comparison,
                            ),
                            full_entity_count=result.full_entity_count,
                            changed_source_row_count=discovered_source_rows,
                            source_snapshot=source_snapshot,
                        )
                    except Exception as error:
                        log_step(
                            "SQL-change proof completed",
                            duration_ms=elapsed_ms(started),
                            status=failure_status(error),
                        )
                        raise
                    log_step(
                        "SQL-change proof completed",
                        duration_ms=elapsed_ms(started),
                        status="ok",
                    )
                    result.full_reference_validated = bool(
                        getattr(sql_proof, "full_reference_validated", False)
                    )
            if dry_run:
                result.affected_entities = recorded_sql_change_affected(
                    entity_type=config.model.entity,
                    entity_key=config.model.key,
                )
            else:
                result.affected_entities = []
            result.frontier_entity_count = min(
                sql_proof.candidate_frontier_count,
                result.full_entity_count,
            )
            result.percent_rows_avoided = percent_rows_avoided(
                result.full_entity_count,
                result.frontier_entity_count,
            )
        elif proof is None:
            proof = measure_mutation_proof(
                config,
                manifest=manifest,
                warehouse=warehouse,
                affected_relation=result.affected_relation,
            )
        log_step("validation started")
        started = time.perf_counter()
        try:
            validations = collect_validation_results(
                config=config,
                manifest=manifest,
                run_results=run_results,
                events=events,
                result=result,
                warehouse=None if dry_run else warehouse,
            )
            if sql_proof is not None and not skip_proof_metrics:
                validations.extend(sql_change_proof_validation_results(sql_proof))
            elif proof is not None:
                validations.extend(proof_validation_results(proof))
        except Exception as error:
            log_step(
                "validation completed",
                duration_ms=elapsed_ms(started),
                status=failure_status(error),
            )
            raise
        log_step("validation completed", duration_ms=elapsed_ms(started), status="ok")
        bytes_scanned = _sum_job_bytes(isolated, prove_jobs)
        if bytes_scanned is not None:
            result.frontier_bytes_scanned = bytes_scanned
    except Exception as error:
        _write_failed_prove_run(
            args,
            config=config,
            run_id=run_id,
            error=error,
            sql_comparison=sql_comparison,
            manifest=manifest,
            phase="warehouse execution",
            code="EXECUTION_FAILED",
        )
        _reraise_prove_failure(error, phase="warehouse execution", code="EXECUTION_FAILED")
    finally:
        if isolated is not None:
            isolated.cleanup()
        elif not dry_run:
            log_step("cleanup started")
            log_step("cleanup completed", status="skipped")
        if not dry_run:
            _print_job_metrics(isolated, warehouse, extra=prove_jobs)
        if warehouse is not None:
            warehouse.close()

    sql_comparison = _stamp_sql_comparison(
        args,
        _apply_rebuild_to_comparison(sql_comparison, result, validations),
        result,
    )
    assessed = sql_proof or proof
    extra_metrics = {
        "fullRowsRecomputed": assessed.full_rows_recomputed,
        "frontierRowsRecomputed": assessed.frontier_rows_recomputed,
    }
    if not rebuild_recommended and not result.full_rebuild_required:
        extra_metrics["testDurationMs"] = assessed.test_duration_ms
        extra_metrics.update(
            {
                "missingFrontierEntities": assessed.missing_frontier_entities,
                "extraFrontierEntities": assessed.extra_frontier_entities,
                "mismatchedFinalRows": assessed.mismatched_final_rows,
            }
        )
    if sql_proof is not None:
        frontier_for_metrics = min(
            sql_proof.candidate_frontier_count,
            sql_proof.full_rows_recomputed,
        )
        extra_metrics.update(
            {
                "frontierEntityCount": frontier_for_metrics,
                "percentRowsAvoided": percent_rows_avoided(
                    sql_proof.full_rows_recomputed,
                    frontier_for_metrics,
                ),
                "candidateFrontierCount": sql_proof.candidate_frontier_count,
                "eventCandidateCount": result.event_candidate_count or 0,
            }
        )
        if result.full_rebuild_required:
            extra_metrics.update(
                {
                    "confirmedFrontierCount": None,
                    "confirmedEntityCount": None,
                    "changedSourceRowCount": None,
                    "sourcePopulationCount": None,
                    "missedEntityCount": None,
                    "missingFrontierEntities": None,
                    "extraFrontierEntities": None,
                    "mismatchedFinalRows": None,
                    "mismatchedRowCount": None,
                    "beforeEntityCount": None,
                    "afterEntityCount": None,
                    "testDurationMs": None,
                }
            )
        else:
            extra_metrics.update(
                {
                    "confirmedFrontierCount": sql_proof.confirmed_frontier_count,
                    "sourcePopulationCount": sql_proof.changed_source_row_count,
                    "changedSourceRowCount": sql_proof.changed_source_row_count,
                    "beforeEntityCount": sql_proof.before_entity_count,
                    "afterEntityCount": sql_proof.after_entity_count,
                }
            )
    output = _emit_run(
        args,
        config=config,
        manifest=manifest,
        result=result,
        validations=validations,
        extra_metrics=extra_metrics,
        sql_comparison=sql_comparison,
    )
    print(f"Full rows recomputed: {assessed.full_rows_recomputed}")
    print(f"Frontier rows recomputed: {assessed.frontier_rows_recomputed}")
    print(f"Rows avoided: {assessed.rows_avoided}")
    _print_origin_counts(result)
    if sql_proof is not None:
        kinds = ((sql_comparison or {}).get("modified") or [{}])[0].get("changeKinds") or []
        operator = kinds[0] if kinds else "SQL change"
        print(f"Run mode: {'fixture' if dry_run else 'live'}")
        print(f"SQL operator: {operator}")
        compilations = list(
            dict.fromkeys(
                str(row.get("impactStatus"))
                for row in ((sql_comparison or {}).get("modified") or [])
                if row.get("impactStatus")
            )
        )
        executions = list(
            dict.fromkeys(
                str(row.get("impactExecution"))
                for row in ((sql_comparison or {}).get("modified") or [])
                if row.get("impactExecution")
            )
        )
        if compilations:
            print(f"Impact compilation: {', '.join(compilations)}")
        if executions:
            print(f"Impact execution: {', '.join(executions)}")
        if getattr(result, "proof_status", None):
            print(f"Proof status: {result.proof_status}")
        if getattr(result, "execution_failed", False) or (
            getattr(result, "failure_phase", None) and result.proof_status == "EXECUTION_FAILED"
        ):
            print(
                "Execution failure: "
                f"{result.failure_phase or 'unknown'} "
                f"{result.failure_code or ''} "
                f"{result.failure_reason or ''}".strip()
            )
        print(f"Changed source rows: {_format_measured(None if result.full_rebuild_required else sql_proof.changed_source_row_count)}")
        print(f"Candidate {_pluralize_entity(config.model.entity)}: {sql_proof.candidate_frontier_count}")
        print(f"Event-derived candidates: {result.event_candidate_count or 0}")
        print(
            f"Confirmed changed summaries: {_format_measured(None if result.full_rebuild_required else sql_proof.confirmed_frontier_count)}"
        )
        if result.full_rebuild_required:
            print("Row count: Not measured")
        else:
            print(f"Row count: {sql_proof.before_entity_count} → {sql_proof.after_entity_count}")
        print(
            f"Targeted repair: {'skipped' if rebuild_recommended or result.full_rebuild_required or getattr(result, 'execution_failed', False) else ('safe' if sql_proof.targeted_repair_safe else 'not safe')}"
        )
        print(
            f"Targeted validation: {(sql_comparison or {}).get('targetedValidation') or ('NOT_RUN' if result.full_rebuild_required else 'PASSED')}"
        )
        if sql_proof.full_rebuild_required:
            print("Full backfill: required")
        elif rebuild_recommended or sql_proof.full_rebuild_recommended:
            print("Full backfill: recommended")
        else:
            print("Full backfill: not required")
    print(
        f"Missing frontier entities: {_format_measured(None if result.full_rebuild_required else assessed.missing_frontier_entities)}"
    )
    print(
        f"Extra frontier entities: {_format_measured(None if result.full_rebuild_required else assessed.extra_frontier_entities)}"
    )
    print(
        f"Mismatched final rows: {_format_measured(None if result.full_rebuild_required else assessed.mismatched_final_rows)}"
    )
    if result.full_rebuild_required:
        print("Test duration: Not measured")
    else:
        print(f"Test duration: {assessed.test_duration_ms} ms")
    _print_assessment_dimensions(sql_comparison)
    print("Validation:")
    for item in validations:
        print(f"  - {item.test_name}: {item.status} (differences={item.difference_count})")
    print(f"Wrote {output}", flush=True)
    log_step("upload started")
    log_step("upload completed", status="skipped:run `frontier upload`")
    _print_phase(PHASE_UPLOAD, skipped="run `frontier upload`")
    if overall_status(validations) != "passed":
        print(
            "Assessment failed; wrote diagnostics for upload.",
            file=sys.stderr,
        )
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    pr_manifest_path = (
        Path(args.pr_manifest).expanduser().resolve()
        if getattr(args, "pr_manifest", None)
        else _target_dir(project_dir) / "manifest.json"
    )
    if not getattr(args, "pr_manifest", None):
        require_current_artifacts(_target_dir(project_dir))
    base_manifest_path = Path(args.base_manifest).expanduser().resolve()
    base_manifest = load_manifest(base_manifest_path)
    pr_manifest = load_manifest(pr_manifest_path)
    config = _resolve_runtime_config(args, project_dir, dbt_manifest=pr_manifest)
    entity_key = config.model.key
    confirmed_keys = tuple(
        dict.fromkeys(
            [config.model.key, *[relation.change_key for relation in config.relations.values()]]
        )
    )
    target_name = config.model.name
    manifest_version, manifest_fingerprint = _pinned_compare_fields(config)
    comparison = compare_manifests(
        base_manifest,
        pr_manifest,
        base_compiled_root=_compiled_root_for(base_manifest_path),
        pr_compiled_root=_compiled_root_for(pr_manifest_path),
        base_commit_sha=base_commit_sha(),
        pr_commit_sha=(os.environ.get("GITHUB_SHA") or "").strip() or None,
        entity_key=entity_key,
        confirmed_keys=confirmed_keys,
        target_name=target_name,
        semantic_manifest_version=manifest_version,
        semantic_manifest_fingerprint=manifest_fingerprint,
    ).to_dict()
    print(format_compare_report(comparison))
    output = Path(args.output) if args.output else _target_dir(project_dir) / "frontier-compare.json"
    _write_run_file(output, comparison)
    print(f"Wrote {output}")
    return 0


def cmd_upload(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    local = load_local_config(project_dir)
    run_file = Path(args.run_file) if args.run_file else _target_dir(project_dir) / RUN_FILE_NAME
    if not run_file.is_file():
        raise ConfigError(
            f"Missing run file {run_file}. Run `frontier prove`, `frontier run`, or `frontier record-failure` first.",
        )
    payload = json.loads(run_file.read_text())
    stamp = _read_invocation_stamp(run_file)
    original_id = str(payload.get("externalRunId") or "")
    identity = payload.get("assessmentIdentity") or {}
    if stamp:
        if original_id != stamp:
            raise ConfigError(
                f"Refusing to upload stale run file {run_file}: "
                f"externalRunId {original_id or '(missing)'} does not match the current "
                f"invocation {stamp}. Run `frontier prove` again."
            )
        invocation = str(identity.get("invocationId") or "")
        if invocation and invocation != stamp:
            raise ConfigError(
                f"Refusing to upload stale run file {run_file}: "
                f"assessmentIdentity.invocationId {invocation} does not match {stamp}."
            )
        if not payload.get("runnerVersion") and not identity:
            raise ConfigError(
                "Refusing to upload a run file that predates this invocation stamp. "
                "The current prove did not write a fresh frontier-run.json."
            )
    if (
        payload.get("runMode") == "fixture"
        and os.environ.get("GITHUB_ACTIONS") == "true"
        and not env_flag("FRONTIER_DRY_RUN")
    ):
        raise ConfigError(
            "Refusing to upload a fixture assessment from GitHub Actions. "
            "Customer CI must not set FRONTIER_DRY_RUN; live prove must execute against the warehouse."
        )
    if args.run_id:
        payload["externalRunId"] = args.run_id
    creds = resolve_api_credential()
    api_key, api_key_source = creds.api_key, creds.source
    api_url = _api_url(args, None, creds, local)
    print(
        f"Uploading {payload.get('externalRunId')} to {api_url} "
        f"as {redact_api_key(api_key)} ({api_key_source})",
        flush=True,
    )
    log_step("upload started")
    started = time.perf_counter()
    try:
        response = upload_run(payload, api_url=api_url, api_key=api_key)
    except Exception as error:
        log_step(
            "upload completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step("upload completed", duration_ms=elapsed_ms(started), status="ok")
    _print_phase(PHASE_UPLOAD, elapsed_ms(started))
    status = response.pop("_httpStatus", None)
    print(json.dumps({"httpStatus": status, **response}, indent=2))
    run_id = response.get("id")
    if isinstance(run_id, str) and not getattr(args, "skip_pr_comment", False):
        action = maybe_upsert_pr_comment(payload, api_url=api_url, run_id=run_id)
        if action:
            print(f"Pull request comment {action}")
    if _wants_blocking(args) and payload.get("status") == "failed":
        print(
            "Assessment failed; uploaded diagnostics before failing the blocking check.",
            file=sys.stderr,
        )
        return 1
    return 0


def cmd_manifest_fetch(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    local = load_local_config(project_dir)
    creds = resolve_api_credential()
    api_key = creds.api_key
    api_url = _api_url(args, None, creds, local)
    project = _saas_project_name(project_dir, creds, local)
    output = Path(args.output).expanduser().resolve() if args.output else default_pin_path(project_dir)
    started = time.perf_counter()
    log_step("manifest fetch started", prefix="manifest")
    try:
        pinned = fetch_active_manifest(api_url=api_url, api_key=api_key, project=project)
        validate_pinned_document(pinned)
        dbt_path = _target_dir(project_dir) / "manifest.json"
        if dbt_path.is_file():
            validate_pinned_against_dbt(pinned, load_manifest(dbt_path))
        pin_manifest(output, pinned)
    except Exception as error:
        log_step(
            "manifest fetch completed",
            prefix="manifest",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step("manifest fetch completed", prefix="manifest", duration_ms=elapsed_ms(started), status="ok")
    print_semantic_manifest(pinned)
    print(f"Wrote {output}", flush=True)
    return 0


def _add_project_dir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "project_dir",
        nargs="?",
        default=".",
        help="dbt project directory (default: current directory)",
    )
    parser.add_argument(
        "--project-dir",
        dest="project_dir_opt",
        help="dbt project directory (same as the positional path)",
    )


def _add_manifest_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--manifest-file",
        help="Pinned semantic manifest JSON (skips the active SaaS fetch)",
    )
    parser.add_argument(
        "--allow-local-manifest",
        action="store_true",
        help="Use frontier.yml semantic mapping when no SaaS credentials are configured",
    )


def _add_base_manifest(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--base-manifest",
        help="manifest.json compiled from the pull request base branch",
    )


def _add_run_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    parser.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    parser.add_argument("--target", help="dbt target name")
    parser.add_argument("--events", help="Change-events CSV (default: seeds/change_events.csv)")
    parser.add_argument("--output", help="Where to write frontier-run.json")
    parser.add_argument("--run-id", help="externalRunId for the resulting payload")
    parser.add_argument("--model", help="Target dbt model name")
    parser.add_argument(
        "--include-entity-ids",
        action="store_true",
        help="Upload raw entity IDs (skips FRONTIER_ENTITY_HASH_KEY)",
    )
    parser.add_argument(
        "--hash-entity-ids",
        action="store_true",
        help="HMAC-SHA-256 entity IDs even when --include-entity-ids is set",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Use the recorded fixture counts without a live warehouse",
    )
    _add_base_manifest(parser)
    _add_manifest_flags(parser)


def _cdc_path(args: argparse.Namespace, project_dir: Path) -> Path:
    return cdc_config_path(project_dir, getattr(args, "cdc_config", None))


def cmd_cdc_inspect(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    config = load_cdc_config(_cdc_path(args, project_dir))
    if (
        _config_path(args, project_dir).is_file()
        or getattr(args, "manifest_file", None)
        or try_resolve_api_credential() is not None
    ):
        dbt_path = _target_dir(project_dir) / "manifest.json"
        dbt_manifest = load_manifest(dbt_path) if dbt_path.is_file() else None
        frontier_config = _resolve_runtime_config(args, project_dir, dbt_manifest=dbt_manifest)
        config = overlay_cdc_with_manifest(config, frontier_config.pinned)
    print(f"CDC provider: {config.provider}", flush=True)
    print(f"Sources: {len(config.sources)}", flush=True)
    for source in config.sources:
        print(f"- {source.source_model}", flush=True)
        print(f"  stream: {source.stream_name}", flush=True)
        print(f"  base: {source.base_relation}", flush=True)
        print(f"  stream_relation: {source.stream_relation}", flush=True)
        print(f"  primary_key: {source.primary_key}", flush=True)
        print(f"  target_entity: {source.target_entity}", flush=True)
        print(f"  target_key: {source.target_key}", flush=True)
        required = ",".join(source.require_before_image_for) or "(none)"
        print(f"  require_before_image_for: {required}", flush=True)
    return 0


def cmd_cdc_status(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    config = load_cdc_config(_cdc_path(args, project_dir))
    warehouse = connect_warehouse(
        project_dir,
        profiles_path=Path(args.profiles).expanduser() if getattr(args, "profiles", None) else None,
        target=getattr(args, "target", None),
    )
    _assert_cdc_supported(warehouse)
    try:
        store = SnowflakeCdcStore(warehouse, config)
        log_step("status started", prefix="cdc")
        for source in config.sources:
            pending = store.stream_has_data(source.stream_relation)
            state = "pending" if pending else "empty"
            print(f"{source.stream_name}: {state}", flush=True)
        log_step("status completed", prefix="cdc", status="ok")
    finally:
        warehouse.close()
    return 0


def cmd_cdc_consume(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    config = load_cdc_config(_cdc_path(args, project_dir))
    warehouse = connect_warehouse(
        project_dir,
        profiles_path=Path(args.profiles).expanduser() if getattr(args, "profiles", None) else None,
        target=getattr(args, "target", None),
    )
    _assert_cdc_supported(warehouse)
    started = time.perf_counter()
    log_step("consume started", prefix="cdc")
    try:
        store = SnowflakeCdcStore(warehouse, config)
        results = consume_all(
            config,
            store=store,
            project_name=project_name_for(project_dir),
        )
    except Exception as error:
        log_step(
            "consume completed",
            prefix="cdc",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        warehouse.close()
        raise
    warehouse.close()
    log_step("consume completed", prefix="cdc", duration_ms=elapsed_ms(started), status="ok")
    for result in results:
        counts = result.operation_counts
        print(
            f"{result.stream_name}: {result.status} "
            f"batch={result.batch_id or '-'} "
            f"raw={result.raw_record_count} logical={result.logical_event_count} "
            f"inserts={counts['inserts']} updates={counts['updates']} "
            f"deletes={counts['deletes']}",
            flush=True,
        )
    return 0


def cmd_cdc_prove(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    cdc_config = load_cdc_config(_cdc_path(args, project_dir))
    frontier_config = _resolve_runtime_config(args, project_dir, dbt_manifest=manifest)
    cdc_config = overlay_cdc_with_manifest(cdc_config, frontier_config.pinned)
    warehouse = connect_warehouse(
        project_dir,
        profiles_path=Path(args.profiles).expanduser() if getattr(args, "profiles", None) else None,
        target=getattr(args, "target", None),
    )
    _assert_cdc_supported(warehouse)
    started = time.perf_counter()
    log_step("prove started", prefix="cdc")
    try:
        store = SnowflakeCdcStore(warehouse, cdc_config)
        result = prove_batch(
            store=store,
            warehouse=warehouse,
            cdc_config=cdc_config,
            frontier_config=frontier_config,
            manifest=manifest,
            compiled_root=_compiled_root_for(_target_dir(project_dir) / "manifest.json"),
            project_name=project_name_for(project_dir),
            batch_id=getattr(args, "batch_id", None),
            apply=bool(getattr(args, "apply", False)),
            output_dir=_target_dir(project_dir),
        )
    except Exception as error:
        log_step(
            "prove completed",
            prefix="cdc",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        warehouse.close()
        raise
    warehouse.close()
    log_step(
        "prove completed",
        prefix="cdc",
        duration_ms=elapsed_ms(started),
        status=result.status,
    )
    if result.status == "NO_BATCH":
        print("no CAPTURED batch available", flush=True)
        return 0
    print(f"logical events: {result.logical_event_count}", flush=True)
    print(f"event-derived candidates: {result.event_candidate_count}", flush=True)
    print(f"SQL-change candidates: {result.sql_change_candidate_count}", flush=True)
    print(f"union candidates: {result.union_candidate_count}", flush=True)
    print(f"confirmed changes: {result.confirmed_change_count}", flush=True)
    print(f"candidate no-ops: {result.no_op_count}", flush=True)
    print(f"missed events: {result.missed_event_count}", flush=True)
    print(f"validation: {result.validation}", flush=True)
    print(f"batch: {result.status}", flush=True)
    if result.evidence:
        print("evidence: " + ", ".join(result.evidence), flush=True)
    if result.repair_path:
        print(f"repair artifact: {result.repair_path}", flush=True)
    return 0


def cmd_cdc_upload(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    pin = default_pin_path(project_dir)
    if pin.is_file() and not getattr(args, "manifest_file", None):
        args.manifest_file = str(pin)
    cdc_config = load_cdc_config(_cdc_path(args, project_dir))
    frontier_config = _resolve_runtime_config(args, project_dir, dbt_manifest=manifest)
    cdc_config = overlay_cdc_with_manifest(cdc_config, frontier_config.pinned)
    warehouse = connect_warehouse(
        project_dir,
        profiles_path=Path(args.profiles).expanduser() if getattr(args, "profiles", None) else None,
        target=getattr(args, "target", None),
    )
    _assert_cdc_supported(warehouse)
    creds = resolve_api_credential()
    api_key, api_key_source = creds.api_key, creds.source
    api_url = _api_url(args, frontier_config, creds)
    print(
        f"Uploading CDC assessment to {api_url} "
        f"as {redact_api_key(api_key)} ({api_key_source})",
        flush=True,
    )
    started = time.perf_counter()
    log_step("upload started", prefix="cdc")
    try:
        store = SnowflakeCdcStore(warehouse, cdc_config)
        result = upload_cdc_batch(
            store=store,
            warehouse=warehouse,
            cdc_config=cdc_config,
            frontier_config=frontier_config,
            manifest=manifest,
            project_name=project_name_for(project_dir),
            api_url=api_url,
            api_key=api_key,
            batch_id=getattr(args, "batch_id", None),
        )
    except Exception as error:
        log_step(
            "upload completed",
            prefix="cdc",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        warehouse.close()
        raise
    warehouse.close()
    http_status = result.get("httpStatus")
    created = result.get("created")
    print(f"HTTP {http_status}", flush=True)
    print(f"created: {str(created).lower() if isinstance(created, bool) else created}", flush=True)
    print("assessment type: cdc", flush=True)
    print(f"batch: {result.get('batchId')}", flush=True)
    print(f"batch upload status: {(result.get('uploadStatus') or '').lower()}", flush=True)
    print(f"run: {result.get('id')}", flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="frontier",
        description="Run a change-frontier assessment in the customer environment.",
    )
    parser.add_argument("--version", action="version", version=f"frontier {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Detect the dbt project and write .frontier/config.yml")
    _add_project_dir(init)
    init.add_argument("--config", help="Unused unless --legacy-yml is set")
    init.add_argument("--force", action="store_true", help="Overwrite an existing .frontier/config.yml")
    init.add_argument("--yes", action="store_true", help="Accept detected defaults")
    init.add_argument(
        "--legacy-yml",
        action="store_true",
        help="Write the example frontier.yml instead of .frontier/config.yml",
    )
    init.set_defaults(func=cmd_init)

    signup = sub.add_parser("signup", help="Open hosted Frontier signup")
    signup.add_argument("--api-url", help="SaaS origin")
    signup.set_defaults(func=cmd_signup)

    login = sub.add_parser("login", help="Store a project API key in the OS keychain")
    login.add_argument("--api-key", action="store_true", help="Paste a project API key (hidden input)")
    login.add_argument("--api-url", help="SaaS origin")
    login.set_defaults(func=cmd_login)

    logout = sub.add_parser("logout", help="Remove stored Frontier API credentials")
    logout.set_defaults(func=cmd_logout)

    auth = sub.add_parser("auth", help="Show authentication status")
    auth_sub = auth.add_subparsers(dest="auth_command", required=True)
    auth_status = auth_sub.add_parser("status", help="Show whether the CLI is authenticated")
    auth_status.set_defaults(func=cmd_auth_status)

    discover = sub.add_parser(
        "discover",
        help="Derive and upload a generated semantic configuration from dbt artifacts",
    )
    _add_project_dir(discover)
    discover.add_argument("--yes", action="store_true", help="Select the first suggested model")
    discover.add_argument(
        "--force",
        action="store_true",
        help="Upload even when the local dbt project name differs from the authenticated Frontier project",
    )
    discover.add_argument("--model", help="Target model name")
    discover.set_defaults(func=cmd_discover)

    doctor = sub.add_parser("doctor", help="Diagnose the local Frontier installation")
    _add_project_dir(doctor)
    doctor.add_argument("--json", action="store_true", help="Print redacted JSON for support")
    doctor.add_argument("--skip-warehouse", action="store_true", help="Skip the live warehouse ping")
    doctor.set_defaults(func=cmd_doctor)

    setup = sub.add_parser("setup", help="Generate GitHub, hash-key, or warehouse permission files")
    setup_sub = setup.add_subparsers(dest="setup_command", required=True)
    setup_github = setup_sub.add_parser("github", help="Write .github/workflows/frontier.yml")
    _add_project_dir(setup_github)
    setup_github.add_argument("--force", action="store_true", help="Overwrite an existing workflow")
    setup_github.add_argument("--yes", action="store_true", help="Accept defaults")
    setup_github.add_argument(
        "--blocking",
        action="store_true",
        help="Set FRONTIER_BLOCKING=true (default is false for the first PR)",
    )
    setup_github.set_defaults(func=cmd_setup_github)
    setup_hash = setup_sub.add_parser("hash-key", help="Generate FRONTIER_ENTITY_HASH_KEY")
    setup_hash.add_argument("--yes", action="store_true", help="Store with gh when available")
    setup_hash.add_argument("--copy", action="store_true", help="Copy the complete key to the clipboard")
    setup_hash.add_argument("--print-key", action="store_true", help="Print the complete key")
    setup_hash.set_defaults(func=cmd_setup_hash_key)
    setup_snowflake = setup_sub.add_parser("snowflake", help="Print least-privilege Snowflake grants")
    _add_project_dir(setup_snowflake)
    setup_snowflake.add_argument("--cdc", action="store_true", help="Include CDC stream grants")
    setup_snowflake.add_argument("--query-history", action="store_true", help="Include query history grants")
    setup_snowflake.set_defaults(func=cmd_permissions)
    setup_bigquery = setup_sub.add_parser("bigquery", help="Print least-privilege BigQuery IAM guidance")
    _add_project_dir(setup_bigquery)
    setup_bigquery.set_defaults(func=cmd_permissions)
    setup_redshift = setup_sub.add_parser("redshift", help="Print least-privilege Redshift grants")
    _add_project_dir(setup_redshift)
    setup_redshift.set_defaults(func=cmd_permissions)

    demo = sub.add_parser("demo", help="First-test PR instructions")
    demo_sub = demo.add_subparsers(dest="demo_command", required=True)
    demo_change = demo_sub.add_parser("change", help="Explain a harmless first test PR")
    _add_project_dir(demo_change)
    demo_change.set_defaults(func=cmd_demo_change)

    update_check = sub.add_parser("update-check", help="Compare this runner to the hosted latest")
    _add_project_dir(update_check)
    update_check.add_argument("--api-url", help="SaaS origin")
    update_check.set_defaults(func=cmd_update_check)

    inspect = sub.add_parser("inspect", help="Validate the runtime mapping and print route evidence")
    _add_project_dir(inspect)
    inspect.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    _add_manifest_flags(inspect)
    _add_base_manifest(inspect)
    inspect.set_defaults(func=cmd_inspect)

    compare = sub.add_parser(
        "compare",
        help="Compare compiled SQL between base-branch and PR manifests",
    )
    _add_project_dir(compare)
    compare.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    compare.add_argument(
        "--base-manifest",
        required=True,
        help="manifest.json compiled from the pull request base branch",
    )
    compare.add_argument(
        "--pr-manifest",
        help="manifest.json compiled from the pull request (default: target/manifest.json)",
    )
    compare.add_argument("--output", help="Where to write frontier-compare.json")
    compare.add_argument("--model", help="Target dbt model name")
    _add_manifest_flags(compare)
    compare.set_defaults(func=cmd_compare)

    run = sub.add_parser("run", help="Execute frontier and validation queries")
    _add_project_dir(run)
    _add_run_flags(run)
    run.set_defaults(func=cmd_run)

    prove = sub.add_parser(
        "prove",
        help="Apply isolated mutations and prove targeted repair equals the full rebuild",
    )
    _add_project_dir(prove)
    _add_run_flags(prove)
    prove.set_defaults(func=cmd_prove)

    record_failure = sub.add_parser(
        "record-failure",
        help="Write a failed assessment without reading dbt artifacts",
    )
    _add_project_dir(record_failure)
    record_failure.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    record_failure.add_argument("--output", help="Where to write frontier-run.json")
    record_failure.add_argument("--run-id", help="externalRunId for the resulting payload")
    record_failure.add_argument(
        "--reason",
        default="dbt build failed; no current artifacts",
        help="Failure message stored on the assessment",
    )
    record_failure.add_argument(
        "--include-entity-ids",
        action="store_true",
        help="Upload raw entity IDs (skips FRONTIER_ENTITY_HASH_KEY)",
    )
    record_failure.add_argument(
        "--hash-entity-ids",
        action="store_true",
        help="HMAC-SHA-256 entity IDs even when --include-entity-ids is set",
    )
    _add_manifest_flags(record_failure)
    record_failure.set_defaults(func=cmd_record_failure)

    manifest_cmd = sub.add_parser("manifest", help="Fetch and pin the active SaaS semantic manifest")
    manifest_sub = manifest_cmd.add_subparsers(dest="manifest_command", required=True)
    manifest_fetch = manifest_sub.add_parser("fetch", help="Download the active semantic manifest")
    _add_project_dir(manifest_fetch)
    manifest_fetch.add_argument("--config", help="Unused; SaaS fetch reads .frontier/config.yml")
    manifest_fetch.add_argument(
        "--api-url",
        help="SaaS origin (default: FRONTIER_API_URL, stored credentials, or .frontier/config.yml)",
    )
    manifest_fetch.add_argument(
        "--output",
        help="Where to write the pinned manifest (default: target/frontier-manifest.json)",
    )
    manifest_fetch.set_defaults(func=cmd_manifest_fetch)

    upload = sub.add_parser("upload", help="POST aggregate results to Frontier SaaS")
    _add_project_dir(upload)
    upload.add_argument("--config", help="Unused; upload reads .frontier/config.yml for the API origin")
    upload.add_argument("--run-file", help="Path to frontier-run.json")
    upload.add_argument("--run-id", help="Override externalRunId")
    upload.add_argument(
        "--api-url",
        help="SaaS origin (default: FRONTIER_API_URL, stored credentials, or .frontier/config.yml)",
    )
    upload.add_argument(
        "--blocking",
        action="store_true",
        help="Exit 1 after a successful upload when the assessment status is failed",
    )
    upload.add_argument(
        "--skip-pr-comment",
        action="store_true",
        help="Do not post or update a GitHub pull request comment after upload",
    )
    upload.set_defaults(func=cmd_upload)

    cdc = sub.add_parser("cdc", help="Inspect, status, consume, prove, and upload Snowflake CDC streams")
    cdc_sub = cdc.add_subparsers(dest="cdc_command", required=True)
    cdc_inspect = cdc_sub.add_parser("inspect", help="Print configured CDC streams without consuming them")
    _add_project_dir(cdc_inspect)
    cdc_inspect.add_argument("--cdc-config", help="Path to frontier-cdc.yml")
    cdc_inspect.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    cdc_inspect.add_argument("--target", help="dbt target name")
    _add_manifest_flags(cdc_inspect)
    cdc_inspect.set_defaults(func=cmd_cdc_inspect)

    cdc_status = cdc_sub.add_parser("status", help="Report whether configured streams have pending data")
    _add_project_dir(cdc_status)
    cdc_status.add_argument("--cdc-config", help="Path to frontier-cdc.yml")
    cdc_status.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    cdc_status.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    cdc_status.add_argument("--target", help="dbt target name")
    cdc_status.set_defaults(func=cmd_cdc_status)

    cdc_consume = cdc_sub.add_parser("consume", help="Durably capture pending stream records")
    _add_project_dir(cdc_consume)
    cdc_consume.add_argument("--cdc-config", help="Path to frontier-cdc.yml")
    cdc_consume.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    cdc_consume.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    cdc_consume.add_argument("--target", help="dbt target name")
    cdc_consume.set_defaults(func=cmd_cdc_consume)

    cdc_prove = cdc_sub.add_parser(
        "prove",
        help="Route a captured CDC batch and run targeted proof",
    )
    _add_project_dir(cdc_prove)
    cdc_prove.add_argument("--cdc-config", help="Path to frontier-cdc.yml")
    cdc_prove.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    cdc_prove.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    cdc_prove.add_argument("--target", help="dbt target name")
    cdc_prove.add_argument("--batch-id", help="Captured batch to prove (default: oldest CAPTURED or FAILED)")
    cdc_prove.add_argument(
        "--apply",
        action="store_true",
        help="Apply the repair to the target mart (default: assessment only)",
    )
    _add_manifest_flags(cdc_prove)
    cdc_prove.set_defaults(func=cmd_cdc_prove)

    cdc_upload = cdc_sub.add_parser(
        "upload",
        help="Upload a completed CDC assessment to Frontier SaaS without re-running proof",
    )
    _add_project_dir(cdc_upload)
    cdc_upload.add_argument("--cdc-config", help="Path to frontier-cdc.yml")
    cdc_upload.add_argument("--config", help="Path to legacy frontier.yml (required only with --allow-local-manifest)")
    cdc_upload.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    cdc_upload.add_argument("--target", help="dbt target name")
    cdc_upload.add_argument("--batch-id", help="Completed batch to upload (default: newest COMPLETED not yet uploaded)")
    cdc_upload.add_argument("--api-url", help="Frontier API origin")
    _add_manifest_flags(cdc_upload)
    cdc_upload.set_defaults(func=cmd_cdc_upload)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    configure_stdio()
    parser = build_parser()
    args = parser.parse_args(argv)
    command = getattr(args, "command", None)
    if command not in {"update-check", "doctor", "login", "logout", "signup"}:
        from frontier.onboard.constants import DEFAULT_API_URL

        maybe_version_notice(str(getattr(args, "api_url", None) or DEFAULT_API_URL))
    try:
        return int(args.func(args))
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
