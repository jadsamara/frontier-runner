from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Sequence

from frontier import __version__
from frontier.api import (
    api_key_from_env,
    build_ingest_payload,
    redact_api_key,
    upload_run,
)
from frontier.comment import maybe_upsert_pr_comment
from frontier.config import (
    ConfigError,
    load_frontier_config,
    write_init_config,
)
from frontier.artifacts import require_current_artifacts
from frontier.compare import (
    compare_manifests,
    comparison_for_ingest,
    compiled_sql_pair_for_sql_change,
    format_compare_report,
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
    frontier_result_to_dict,
    load_change_events_csv,
    percent_rows_avoided,
    run_frontier,
)
from frontier.execute import open_isolated_run, sql_change_impact_queries
from frontier.hashing import entity_hash_key_from_env, hmac_entity_id
from frontier.proof import (
    apply_resolved_delete,
    measure_mutation_proof,
    measure_sql_change_proof,
    proof_validation_results,
    recorded_proof,
    recorded_sql_change_affected,
    recorded_sql_change_proof,
    resolve_deleted_order,
    sql_change_proof_validation_results,
)
from frontier.warehouse import (
    FakeWarehouse,
    WarehouseAdapter,
    connect_warehouse,
    describe_adapter,
    normalize_warehouse_type,
)
from frontier.validation import (
    ValidationResult,
    collect_validation_results,
    evidence_level,
    overall_status,
    sql_change_narrow_frontier_result,
    SQL_CHANGE_NARROW_FRONTIER,
)

RUN_FILE_NAME = "frontier-run.json"


def _project_dir(args: argparse.Namespace) -> Path:
    flag = getattr(args, "project_dir_opt", None)
    value = flag or getattr(args, "project_dir", ".") or "."
    return Path(value).expanduser().resolve()


def _config_path(args: argparse.Namespace, project_dir: Path) -> Path:
    if getattr(args, "config", None):
        return Path(args.config).expanduser().resolve()
    return project_dir / "frontier.yml"


def _target_dir(project_dir: Path) -> Path:
    return project_dir / "target"


def _compiled_root_for(manifest_path: Path) -> Path:
    return manifest_path.parent / "compiled"


def _impact_keys(args: argparse.Namespace, project_dir: Path) -> tuple[str | None, tuple[str, ...]]:
    path = _config_path(args, project_dir)
    if not path.is_file():
        return None, ()
    try:
        config = load_frontier_config(path)
    except ConfigError:
        return None, ()
    confirmed = tuple(
        dict.fromkeys(
            [
                config.model.key,
                *[relation.change_key for relation in config.relations.values()],
            ]
        )
    )
    return config.model.key, confirmed


def _load_sql_comparison(
    args: argparse.Namespace,
    *,
    pr_manifest,
    project_dir: Path,
) -> dict[str, Any] | None:
    base_path = getattr(args, "base_manifest", None)
    if not base_path:
        return None
    base_manifest_path = Path(base_path).expanduser().resolve()
    base_manifest = load_manifest(base_manifest_path)
    pr_path = pr_manifest.path or (_target_dir(project_dir) / "manifest.json")
    entity_key, confirmed_keys = _impact_keys(args, project_dir)
    comparison = compare_manifests(
        base_manifest,
        pr_manifest,
        base_compiled_root=_compiled_root_for(base_manifest_path),
        pr_compiled_root=_compiled_root_for(Path(pr_path)),
        base_commit_sha=base_commit_sha(),
        pr_commit_sha=(os.environ.get("GITHUB_SHA") or "").strip() or None,
        entity_key=entity_key,
        confirmed_keys=confirmed_keys,
    )
    return comparison.to_dict()


def cmd_init(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    path = Path(args.config).expanduser().resolve() if args.config else project_dir / "frontier.yml"
    write_init_config(path, force=args.force)
    print(f"Wrote {path}")
    return 0


def cmd_inspect(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    config = load_frontier_config(_config_path(args, project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    if manifest.project_name != config.project:
        raise ConfigError(
            f"frontier.yml project '{config.project}' does not match manifest '{manifest.project_name}'",
        )
    report = inspect_report(manifest, config.model.name)
    print(format_inspect_report(report))
    comparison = _load_sql_comparison(args, pr_manifest=manifest, project_dir=project_dir)
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
    print("\nConfigured relations:")
    for name, relation in config.relations.items():
        node = next(node for node in manifest.nodes.values() if node.name == name)
        print(f"  - {name} ({node.relation}) change_key={relation.change_key} route={relation.route.kind}")
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
) -> tuple[tuple[str, ...], bool]:
    queries, required = sql_change_impact_queries(comparison)
    if not persist:
        return (), False
    return queries, required


def _stamp_sql_comparison(args: argparse.Namespace, comparison: dict[str, Any] | None, result) -> dict[str, Any] | None:
    return stamp_impact_execution(
        comparison,
        run_mode=_run_mode(args),
        full_rebuild_required=bool(getattr(result, "full_rebuild_required", False)),
        sql_change_executed=getattr(result, "sql_change_candidate_count", None) is not None,
    )


def _apply_rebuild_to_comparison(
    comparison: dict[str, Any] | None,
    result,
    validations: list,
) -> dict[str, Any] | None:
    if not getattr(result, "full_rebuild_required", False):
        return comparison
    if comparison is not None:
        updated = dict(comparison)
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


def _print_origin_counts(result) -> None:
    if result.event_candidate_count is not None:
        print(f"Event candidates: {result.event_candidate_count}")
    if result.sql_change_candidate_count is not None:
        print(f"SQL-change candidates: {result.sql_change_candidate_count}")
    if result.union_candidate_count is not None:
        print(f"Union candidates: {result.union_candidate_count}")
    if result.full_rebuild_required:
        print("Impact: FULL_REBUILD_REQUIRED")


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
    )
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
    )
    output = Path(args.output) if args.output else _target_dir(_project_dir(args)) / RUN_FILE_NAME
    _write_run_file(output, payload)
    return output


def cmd_run(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    config = load_frontier_config(_config_path(args, project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    run_results = load_run_results(_target_dir(project_dir) / "run_results.json")
    sql_comparison = _load_sql_comparison(
        args,
        pr_manifest=manifest,
        project_dir=project_dir,
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
    config = load_frontier_config(_config_path(args, project_dir))
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
    )
    output = Path(args.output) if args.output else _target_dir(project_dir) / RUN_FILE_NAME
    _write_run_file(output, payload)
    print(f"Wrote failed assessment to {output}")
    return 0


def cmd_prove(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    require_current_artifacts(_target_dir(project_dir))
    config = load_frontier_config(_config_path(args, project_dir))
    manifest = load_manifest(_target_dir(project_dir) / "manifest.json")
    run_results = load_run_results(_target_dir(project_dir) / "run_results.json")
    sql_comparison = _load_sql_comparison(
        args,
        pr_manifest=manifest,
        project_dir=project_dir,
    )
    events = _load_events(args, project_dir, sql_comparison)
    sql_change_demo = _sql_change_present(sql_comparison)

    dry_run = _use_dry_run(args)
    if dry_run and os.environ.get("GITHUB_ACTIONS") == "true":
        raise ConfigError(
            "FRONTIER_DRY_RUN is not allowed in GitHub Actions prove. "
            "Customer CI must execute against DATA_AGENT_DEV.DBT_CI."
        )
    warehouse: WarehouseAdapter
    run_id = args.run_id or default_external_run_id(config.project)
    persist = not dry_run
    sql_proof = None
    proof = None
    if dry_run:
        warehouse = FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3 if not sql_change_demo else 12)],
                "order_id in (1)": [(36901,)],
                "order_id in (5)": [(781,)],
                "difference_count": [(0,)],
            }
        )
        print("Using in-memory warehouse (--dry-run). No live warehouse session.")
        if sql_change_demo:
            sql_proof = recorded_sql_change_proof()
        else:
            proof = recorded_proof()
            events = apply_resolved_delete(
                events,
                order_id=proof.deleted_order_id,
                customer_id=proof.deleted_order_customer_id,
            )
    else:
        warehouse = connect_warehouse(
            project_dir,
            profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
            target=args.target,
        )
        print(f"{warehouse.warehouse_type}: " + json.dumps(describe_adapter(warehouse)))
        if not sql_change_demo:
            try:
                deleted_order_id, deleted_customer_id = resolve_deleted_order(
                    manifest,
                    warehouse,
                    proof=config.proof,
                )
            except ConfigError:
                warehouse.close()
                raise
            events = apply_resolved_delete(
                events,
                order_id=deleted_order_id,
                customer_id=deleted_customer_id,
            )

    base_sql, after_sql = _proof_sql_pair(
        args,
        config=config,
        manifest=manifest,
        project_dir=project_dir,
        sql_comparison=sql_comparison,
    )

    isolated = None
    try:
        sql_change_queries, sql_change_required = _sql_change_queries(
            sql_comparison,
            persist=persist,
        )
        if persist:
            model = manifest.find_model(config.model.name)
            isolated = open_isolated_run(
                warehouse,
                run_id=run_id,
                entity_key=config.model.key,
                model_database=model.database,
                model_schema=model.schema,
                model_relation=model.relation,
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
            isolated_run=isolated,
        )
        if sql_change_demo:
            if sql_proof is None:
                if not result.affected_relation or not base_sql or not after_sql:
                    raise ConfigError("SQL-change proof requires compiled base/PR SQL and affected keys")
                sql_proof = measure_sql_change_proof(
                    config,
                    warehouse=warehouse,
                    before_sql=base_sql,
                    after_sql=after_sql,
                    affected_relation=result.affected_relation,
                    impact_sql=sql_change_queries[0] if sql_change_queries else None,
                    candidate_count=result.union_candidate_count,
                    confirmed_count=len(result.confirmed_keys) if result.confirmed_keys is not None else None,
                    full_rebuild_required=result.full_rebuild_required,
                )
            if dry_run:
                result.affected_entities = recorded_sql_change_affected(
                    entity_type=config.model.entity,
                    entity_key=config.model.key,
                )
            else:
                result.affected_entities = []
            result.frontier_entity_count = sql_proof.candidate_frontier_count
            result.percent_rows_avoided = percent_rows_avoided(
                result.full_entity_count,
                sql_proof.candidate_frontier_count,
            )
        elif proof is None:
            proof = measure_mutation_proof(
                config,
                manifest=manifest,
                warehouse=warehouse,
                affected_relation=result.affected_relation,
            )
        validations = collect_validation_results(
            config=config,
            manifest=manifest,
            run_results=run_results,
            events=events,
            result=result,
            warehouse=None if dry_run else warehouse,
        )
        if sql_proof is not None:
            validations.extend(sql_change_proof_validation_results(sql_proof))
        else:
            validations.extend(proof_validation_results(proof))
    finally:
        if isolated is not None:
            isolated.cleanup()
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
        "missingFrontierEntities": assessed.missing_frontier_entities,
        "extraFrontierEntities": assessed.extra_frontier_entities,
        "mismatchedFinalRows": assessed.mismatched_final_rows,
        "testDurationMs": assessed.test_duration_ms,
    }
    if sql_proof is not None:
        extra_metrics.update(
            {
                "frontierEntityCount": sql_proof.candidate_frontier_count,
                "percentRowsAvoided": percent_rows_avoided(
                    sql_proof.full_rows_recomputed,
                    sql_proof.candidate_frontier_count,
                ),
                "candidateFrontierCount": sql_proof.candidate_frontier_count,
                "confirmedFrontierCount": sql_proof.confirmed_frontier_count,
                "sourcePopulationCount": sql_proof.changed_source_row_count,
                "changedSourceRowCount": sql_proof.changed_source_row_count,
                "beforeEntityCount": sql_proof.before_entity_count,
                "afterEntityCount": sql_proof.after_entity_count,
                "eventCandidateCount": result.event_candidate_count or 0,
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
        print(f"Changed source rows: {sql_proof.changed_source_row_count}")
        print(f"Candidate customers: {sql_proof.candidate_frontier_count}")
        print(f"Event-derived candidates: {result.event_candidate_count or 0}")
        print(f"Confirmed changed summaries: {sql_proof.confirmed_frontier_count}")
        print(f"Row count: {sql_proof.before_entity_count} → {sql_proof.after_entity_count}")
        print(f"Targeted repair: {'safe' if sql_proof.targeted_repair_safe else 'not safe'}")
        print(f"Full backfill: {'required' if sql_proof.full_rebuild_required else 'not required'}")
    print(f"Missing frontier entities: {assessed.missing_frontier_entities}")
    print(f"Extra frontier entities: {assessed.extra_frontier_entities}")
    print(f"Mismatched final rows: {assessed.mismatched_final_rows}")
    print(f"Test duration: {assessed.test_duration_ms} ms")
    print("Validation:")
    for item in validations:
        print(f"  - {item.test_name}: {item.status} (differences={item.difference_count})")
    print(f"Wrote {output}")
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
    entity_key, confirmed_keys = _impact_keys(args, project_dir)
    comparison = compare_manifests(
        base_manifest,
        pr_manifest,
        base_compiled_root=_compiled_root_for(base_manifest_path),
        pr_compiled_root=_compiled_root_for(pr_manifest_path),
        base_commit_sha=base_commit_sha(),
        pr_commit_sha=(os.environ.get("GITHUB_SHA") or "").strip() or None,
        entity_key=entity_key,
        confirmed_keys=confirmed_keys,
    ).to_dict()
    print(format_compare_report(comparison))
    output = Path(args.output) if args.output else _target_dir(project_dir) / "frontier-compare.json"
    _write_run_file(output, comparison)
    print(f"Wrote {output}")
    return 0


def cmd_upload(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    config = load_frontier_config(_config_path(args, project_dir))
    run_file = Path(args.run_file) if args.run_file else _target_dir(project_dir) / RUN_FILE_NAME
    if not run_file.is_file():
        raise ConfigError(
            f"Missing run file {run_file}. Run `frontier prove`, `frontier run`, or `frontier record-failure` first.",
        )
    payload = json.loads(run_file.read_text())
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
    api_url = args.api_url or os.environ.get("FRONTIER_API_URL") or config.api_url
    api_key, api_key_source = api_key_from_env()
    print(
        f"Uploading {payload.get('externalRunId')} to {api_url} "
        f"as {redact_api_key(api_key)} ({api_key_source})"
    )
    response = upload_run(payload, api_url=api_url, api_key=api_key)
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


def _add_base_manifest(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--base-manifest",
        help="manifest.json compiled from the pull request base branch",
    )


def _add_run_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Path to frontier.yml")
    parser.add_argument("--profiles", help="dbt profiles.yml (default: ~/.dbt/profiles.yml)")
    parser.add_argument("--target", help="dbt target name")
    parser.add_argument("--events", help="Change-events CSV (default: seeds/change_events.csv)")
    parser.add_argument("--output", help="Where to write frontier-run.json")
    parser.add_argument("--run-id", help="externalRunId for the resulting payload")
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="frontier",
        description="Run a change-frontier assessment in the customer environment.",
    )
    parser.add_argument("--version", action="version", version=f"frontier {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Write a frontier.yml next to the dbt project")
    _add_project_dir(init)
    init.add_argument("--config", help="Path to write (default: <project-dir>/frontier.yml)")
    init.add_argument("--force", action="store_true", help="Overwrite an existing file")
    init.set_defaults(func=cmd_init)

    inspect = sub.add_parser("inspect", help="Read dbt artifacts and print model lineage")
    _add_project_dir(inspect)
    inspect.add_argument("--config", help="Path to frontier.yml")
    _add_base_manifest(inspect)
    inspect.set_defaults(func=cmd_inspect)

    compare = sub.add_parser(
        "compare",
        help="Compare compiled SQL between base-branch and PR manifests",
    )
    _add_project_dir(compare)
    compare.add_argument("--config", help="Path to frontier.yml")
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
    record_failure.add_argument("--config", help="Path to frontier.yml")
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
    record_failure.set_defaults(func=cmd_record_failure)

    upload = sub.add_parser("upload", help="POST aggregate results to Frontier SaaS")
    _add_project_dir(upload)
    upload.add_argument("--config", help="Path to frontier.yml")
    upload.add_argument("--run-file", help="Path to frontier-run.json")
    upload.add_argument("--run-id", help="Override externalRunId")
    upload.add_argument("--api-url", help="SaaS origin (default: FRONTIER_API_URL or frontier.yml)")
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
