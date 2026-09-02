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
from frontier.github import default_external_run_id, env_flag, github_source
from frontier.dbt_artifacts import (
    format_inspect_report,
    inspect_report,
    load_manifest,
    load_run_results,
)
from frontier.frontier import (
    frontier_result_to_dict,
    load_change_events_csv,
    run_frontier,
)
from frontier.hashing import entity_hash_key_from_env, hmac_entity_id
from frontier.proof import (
    apply_resolved_delete,
    measure_mutation_proof,
    proof_validation_results,
    recorded_proof,
)
from frontier.snowflake import (
    FakeWarehouse,
    Warehouse,
    describe_connection,
    load_snowflake_config,
    open_warehouse,
)
from frontier.validation import (
    collect_validation_results,
    evidence_level,
    overall_status,
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


def _wants_blocking(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "blocking", False)) or env_flag("FRONTIER_BLOCKING")


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
    events_path = Path(args.events) if args.events else project_dir / "seeds" / "change_events.csv"
    events = load_change_events_csv(events_path)

    dry_run = _use_dry_run(args)
    warehouse: Warehouse
    if dry_run:
        warehouse = FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3)],
                "order_id in (1)": [(36901,)],
                "order_id in (-1)": [],
                "difference_count": [(0,)],
            }
        )
        print("Using in-memory warehouse (--dry-run). No Snowflake session.")
    else:
        snowflake_config = load_snowflake_config(
            project_dir,
            profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
            target=args.target,
        )
        print("Snowflake: " + json.dumps(describe_connection(snowflake_config)))
        warehouse = open_warehouse(snowflake_config)

    try:
        result = run_frontier(
            config,
            manifest=manifest,
            events=events,
            warehouse=warehouse,
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

    output = _emit_run(
        args,
        config=config,
        manifest=manifest,
        result=result,
        validations=validations,
    )
    print(f"Full entities: {result.full_entity_count}")
    print(f"Frontier: {result.frontier_entity_count}")
    print(f"Rows avoided: {result.percent_rows_avoided}%")
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
    events_path = Path(args.events) if args.events else project_dir / "seeds" / "change_events.csv"
    events = load_change_events_csv(events_path)

    dry_run = _use_dry_run(args)
    warehouse: Warehouse
    if dry_run:
        warehouse = FakeWarehouse(
            {
                "full_entity_count": [(150_000, 3)],
                "order_id in (1)": [(36901,)],
                "order_id in (5)": [(781,)],
                "difference_count": [(0,)],
            }
        )
        print("Using in-memory warehouse (--dry-run). No Snowflake session.")
        proof = recorded_proof()
    else:
        snowflake_config = load_snowflake_config(
            project_dir,
            profiles_path=Path(args.profiles).expanduser() if args.profiles else None,
            target=args.target,
        )
        print("Snowflake: " + json.dumps(describe_connection(snowflake_config)))
        warehouse = open_warehouse(snowflake_config)
        try:
            proof = measure_mutation_proof(config, manifest=manifest, warehouse=warehouse)
        except ConfigError:
            warehouse.close()
            raise

    events = apply_resolved_delete(
        events,
        order_id=proof.deleted_order_id,
        customer_id=proof.deleted_order_customer_id,
    )

    try:
        result = run_frontier(
            config,
            manifest=manifest,
            events=events,
            warehouse=warehouse,
        )
        validations = collect_validation_results(
            config=config,
            manifest=manifest,
            run_results=run_results,
            events=events,
            result=result,
            warehouse=None if dry_run else warehouse,
        )
        validations.extend(proof_validation_results(proof))
    finally:
        warehouse.close()

    output = _emit_run(
        args,
        config=config,
        manifest=manifest,
        result=result,
        validations=validations,
        extra_metrics={
            "fullRowsRecomputed": proof.full_rows_recomputed,
            "frontierRowsRecomputed": proof.frontier_rows_recomputed,
            "missingFrontierEntities": proof.missing_frontier_entities,
            "extraFrontierEntities": proof.extra_frontier_entities,
            "mismatchedFinalRows": proof.mismatched_final_rows,
            "testDurationMs": proof.test_duration_ms,
        },
    )
    print(f"Full rows recomputed: {proof.full_rows_recomputed}")
    print(f"Frontier rows recomputed: {proof.frontier_rows_recomputed}")
    print(f"Rows avoided: {proof.rows_avoided}")
    print(f"Missing frontier entities: {proof.missing_frontier_entities}")
    print(f"Extra frontier entities: {proof.extra_frontier_entities}")
    print(f"Mismatched final rows: {proof.mismatched_final_rows}")
    print(f"Test duration: {proof.test_duration_ms} ms")
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


def cmd_upload(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    config = load_frontier_config(_config_path(args, project_dir))
    run_file = Path(args.run_file) if args.run_file else _target_dir(project_dir) / RUN_FILE_NAME
    if not run_file.is_file():
        raise ConfigError(
            f"Missing run file {run_file}. Run `frontier prove`, `frontier run`, or `frontier record-failure` first.",
        )
    payload = json.loads(run_file.read_text())
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
        help="Use the recorded TPCH counts without a live warehouse",
    )


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
    inspect.set_defaults(func=cmd_inspect)

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
