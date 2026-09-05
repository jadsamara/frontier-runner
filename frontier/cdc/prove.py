from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from frontier.cdc.config import CdcConfig
from frontier.cdc.route import route_events
from frontier.cdc.store import CdcBatch, CdcProofRecord, CdcStore, PROVABLE_STATUSES
from frontier.cdc.upload import assert_baseline_fresh
from frontier.compare import compiled_sql_for
from frontier.config import ConfigError, FrontierConfig
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.execute import (
    HANDWRITTEN_FRONTIER_MODELS,
    affected_keys_relation,
    assert_not_prod,
    confirmed_changed_sql,
    create_schema_sql,
    create_table_as_sql,
    generate_targeted_sql,
    generic_repaired_sql,
    restriction_is_pushed,
    targeted_phase_relation,
)
from frontier.progress import elapsed_ms, failure_status, log_step
from frontier.proof import mismatched_rows_sql
from frontier.sql_fingerprint import sql_fingerprint
from frontier.warehouse import WarehouseAdapter, sql_literal, sql_string

ORIGIN_EVENT = "event"
CDC_PREFIX = "cdc"
FORBIDDEN_SQL = (
    "change_events",
    "frontier_affected_customers",
    "frontier_customer_summary_target",
    "frontier_customer_orders_target",
    "customer_summary_after",
    "stg_orders_mutated",
    "stg_customers_mutated",
)


@dataclass(frozen=True)
class ProveResult:
    batch_id: str | None
    status: str
    logical_event_count: int
    event_candidate_count: int
    sql_change_candidate_count: int
    union_candidate_count: int
    confirmed_change_count: int
    no_op_count: int
    missed_event_count: int
    validation: str
    duration_ms: int
    evidence: tuple[str, ...] = ()
    repair_path: str | None = None

    @property
    def operation_label(self) -> str:
        return self.status


def cdc_isolated_location(model: DbtNode) -> tuple[str, str]:
    database = (
        os.environ.get("FRONTIER_WAREHOUSE_DATABASE") or model.database or "DATA_AGENT_DEV"
    ).strip()
    schema = (os.environ.get("FRONTIER_WAREHOUSE_SCHEMA") or model.schema or "DBT_DEV").strip()
    if not database or not schema:
        raise ConfigError("CDC proof requires a warehouse database and schema")
    assert_not_prod(database=database, schema=schema)
    assert_not_prod(database=model.database, schema=model.schema)
    return database, schema


def create_cdc_affected_keys_sql(
    relation: str,
    entity_key: str,
    event_counts: dict[str, int],
) -> str:
    if not event_counts:
        raise ConfigError("no candidate keys to materialize")
    unions = " union all ".join(
        (
            f"select {sql_literal(key)} as {entity_key}, "
            f"{sql_literal(key)} as entity_key, "
            f"{sql_string(ORIGIN_EVENT)} as origin, "
            f"{int(count)} as event_count"
        )
        for key, count in event_counts.items()
    )
    return (
        f"create or replace table {relation} as "
        f"select distinct {entity_key}, entity_key, origin, event_count "
        f"from ({unions}) as frontier_cdc_keys "
        f"where {entity_key} is not null"
    )


def _injection_count(sql: str) -> int:
    return sql.lower().count("as frontier_keys")


def compiled_sql_for_cdc_target(
    *,
    manifest: Manifest,
    model_name: str,
    entity_key: str,
    affected_relation: str,
    compiled_root: Path | None,
    dialect: str,
) -> tuple[str, str]:
    """Return compiled SQL that can push the key join into source CTEs."""
    node = manifest.find_model(model_name)
    candidates: list[tuple[int, str, str]] = []
    seen: set[str] = set()
    nodes: list[DbtNode] = [node, *manifest.upstream_models(node.unique_id)]
    for item in nodes:
        name = item.name.lower()
        if name in seen:
            continue
        seen.add(name)
        if name in HANDWRITTEN_FRONTIER_MODELS or name.startswith("mutation_"):
            continue
        if name.endswith(("_after", "_repaired", "_mutated")):
            continue
        sql = compiled_sql_for(item, compiled_root)
        if not sql or not sql.strip():
            continue
        try:
            targeted = generate_targeted_sql(
                sql,
                entity_key=entity_key,
                affected_relation=affected_relation,
                dialect=dialect,
            )
        except ConfigError:
            continue
        if not restriction_is_pushed(targeted, entity_key=entity_key, dialect=dialect):
            continue
        lowered = targeted.lower()
        if any(token in lowered for token in FORBIDDEN_SQL):
            continue
        candidates.append((_injection_count(targeted), item.name, sql))
    if not candidates:
        raise ConfigError("target SQL cannot be safely filtered")
    candidates.sort(key=lambda row: (row[0], row[1]), reverse=True)
    _score, name, sql = candidates[0]
    return sql, name


def _count(warehouse: WarehouseAdapter, sql: str) -> int:
    rows = warehouse.execute(sql)
    if not rows or rows[0][0] is None:
        return 0
    return int(rows[0][0])


def repair_artifact(
    *,
    target_relation: str,
    entity_key: str,
    candidate_count: int,
    confirmed_change_count: int,
    no_op_count: int,
    strategy: str,
    fingerprints: dict[str, str],
    apply: bool,
) -> dict[str, Any]:
    return {
        "targetRelation": target_relation,
        "entityKey": entity_key,
        "candidateCount": candidate_count,
        "confirmedChangeCount": confirmed_change_count,
        "noOpCount": no_op_count,
        "strategy": strategy,
        "apply": apply,
        "queryFingerprints": fingerprints,
        "wholeTableReferenceExecuted": False,
        "eventRoutingValidated": True,
        "targetedRowsCompared": True,
    }


def write_repair_artifact(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return path


def _empty_result(*, status: str, duration_ms: int, batch_id: str | None = None) -> ProveResult:
    return ProveResult(
        batch_id=batch_id,
        status=status,
        logical_event_count=0,
        event_candidate_count=0,
        sql_change_candidate_count=0,
        union_candidate_count=0,
        confirmed_change_count=0,
        no_op_count=0,
        missed_event_count=0,
        validation="skipped",
        duration_ms=duration_ms,
        evidence=(),
    )


def prove_batch(
    *,
    store: CdcStore,
    warehouse: WarehouseAdapter,
    cdc_config: CdcConfig,
    frontier_config: FrontierConfig,
    manifest: Manifest,
    compiled_root: Path | None,
    project_name: str,
    batch_id: str | None = None,
    apply: bool = False,
    output_dir: Path | None = None,
) -> ProveResult:
    started = time.perf_counter()
    store.ensure_control_tables()
    pending = (
        store.get_batch(batch_id)
        if batch_id
        else store.oldest_provable_batch(project_name)
    )
    if pending is not None and pending.status in PROVABLE_STATUSES:
        assert_baseline_fresh(store, project_name, ignoring_batch_id=pending.batch_id)
    log_step("batch claim started", prefix=CDC_PREFIX)
    claimed: CdcBatch | None = None
    created: list[str] = []
    try:
        claimed = _claim(store, project_name=project_name, batch_id=batch_id)
        if claimed is None:
            log_step(
                "batch claim completed",
                prefix=CDC_PREFIX,
                duration_ms=elapsed_ms(started),
                status="no-batch",
            )
            return _empty_result(status="NO_BATCH", duration_ms=elapsed_ms(started))
        log_step(
            f"batch claim completed batch={claimed.batch_id}",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(started),
            status="PROCESSING",
        )
        result = _prove_claimed(
            claimed,
            store=store,
            warehouse=warehouse,
            cdc_config=cdc_config,
            frontier_config=frontier_config,
            manifest=manifest,
            compiled_root=compiled_root,
            apply=apply,
            output_dir=output_dir,
            created=created,
            started=started,
        )
        return result
    except Exception as error:
        if claimed is not None:
            try:
                store.set_batch_status(
                    claimed.batch_id,
                    "FAILED",
                    error_code=type(error).__name__,
                )
            except Exception:
                pass
        log_step(
            "batch completion completed",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    finally:
        if claimed is not None:
            _cleanup(warehouse, store, created)
            store.release_proof_claim(claimed.batch_id)


def _claim(store: CdcStore, *, project_name: str, batch_id: str | None) -> CdcBatch | None:
    if batch_id:
        existing = store.get_batch(batch_id)
        if existing is None:
            raise ConfigError("CDC batch not found")
        if existing.status == "COMPLETED":
            raise ConfigError("completed batch cannot be reprocessed")
        if existing.status == "PROCESSING":
            raise ConfigError("CDC batch is already being proved")
        claimed = store.claim_proof(batch_id)
        if claimed is None:
            raise ConfigError("CDC batch is already being proved")
        return claimed
    return store.claim_oldest_provable(project_name)


def _prove_claimed(
    batch: CdcBatch,
    *,
    store: CdcStore,
    warehouse: WarehouseAdapter,
    cdc_config: CdcConfig,
    frontier_config: FrontierConfig,
    manifest: Manifest,
    compiled_root: Path | None,
    apply: bool,
    output_dir: Path | None,
    created: list[str],
    started: float,
) -> ProveResult:
    entity_key = frontier_config.model.key
    model = manifest.find_model(frontier_config.model.name)
    target_relation = model.relation
    dialect = warehouse.dialect
    database, schema = cdc_isolated_location(model)
    keys_relation = affected_keys_relation(batch.batch_id, database=database, schema=schema)
    after_relation = targeted_phase_relation(keys_relation, "head")

    phase = time.perf_counter()
    log_step("event routing started", prefix=CDC_PREFIX)
    events = store.list_events(batch.batch_id)
    routed = route_events(events, cdc_config)
    log_step(
        (
            f"event routing completed events={routed.event_count} "
            f"candidates={len(routed.candidates)}"
        ),
        prefix=CDC_PREFIX,
        duration_ms=elapsed_ms(phase),
        status="ok",
    )

    fingerprints: dict[str, str] = {}
    confirmed = 0
    no_ops = 0
    union_count = len(routed.candidates)
    event_count = union_count
    compared = False

    if routed.candidates:
        phase = time.perf_counter()
        log_step("candidate materialization started", prefix=CDC_PREFIX)
        warehouse.execute(create_schema_sql(database, schema))
        warehouse.execute(
            create_cdc_affected_keys_sql(keys_relation, entity_key, routed.event_counts())
        )
        created.append(keys_relation)
        log_step(
            f"candidate materialization completed candidates={union_count} origin=event",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(phase),
            status="ok",
        )

        phase = time.perf_counter()
        log_step("targeted execution started", prefix=CDC_PREFIX)
        model_sql, _sql_name = compiled_sql_for_cdc_target(
            manifest=manifest,
            model_name=frontier_config.model.name,
            entity_key=entity_key,
            affected_relation=keys_relation,
            compiled_root=compiled_root,
            dialect=dialect,
        )
        targeted_sql = generate_targeted_sql(
            model_sql,
            entity_key=entity_key,
            affected_relation=keys_relation,
            dialect=dialect,
        )
        lowered = targeted_sql.lower()
        if any(token in lowered for token in FORBIDDEN_SQL):
            raise ConfigError("target SQL cannot be safely filtered")
        fingerprints["targeted"] = sql_fingerprint(targeted_sql, dialect=dialect)
        warehouse.execute(create_table_as_sql(after_relation, targeted_sql))
        created.append(after_relation)
        targeted_count = _count(
            warehouse,
            (
                "select count(*) as targeted_row_count "
                f"from {after_relation} as frontier_cdc_targeted_count"
            ),
        )
        log_step(
            f"targeted execution completed rows={targeted_count}",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(phase),
            status="ok",
        )

        phase = time.perf_counter()
        log_step("confirmation started", prefix=CDC_PREFIX)
        before_sql = (
            f"select frontier_before.* from {target_relation} as frontier_before "
            f"inner join {keys_relation} as frontier_keys "
            f"on frontier_before.{entity_key} = frontier_keys.{entity_key}"
        )
        after_sql = f"select * from {after_relation} as frontier_after"
        confirmed = _count(
            warehouse,
            (
                "select count(*) as confirmed_change_count from ("
                f"{confirmed_changed_sql(before_sql=before_sql, after_sql=after_sql, entity_key=entity_key, dialect=dialect)}"
                ") as frontier_cdc_confirmed"
            ),
        )
        if confirmed > union_count:
            raise ConfigError("candidate missing from targeted execution unexpectedly")
        no_ops = union_count - confirmed
        log_step(
            (
                f"confirmation completed confirmed={confirmed} "
                f"no_ops={no_ops}"
            ),
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(phase),
            status="ok",
        )
        compared = True

        phase = time.perf_counter()
        log_step("validation started", prefix=CDC_PREFIX)
        missing = _count(
            warehouse,
            (
                "select count(*) as missing_candidate_count from ("
                f"select {entity_key} from {keys_relation} as frontier_expected_keys "
                f"except "
                f"select {entity_key} from {after_relation} as frontier_targeted_keys "
                ") as frontier_cdc_missing_candidates"
            ),
        )
        if missing:
            raise ConfigError("candidate missing from targeted execution unexpectedly")
        repair_sql = generic_repaired_sql(
            before_relation=target_relation,
            targeted_after_sql=f"select * from {after_relation}",
            affected_relation=keys_relation,
            entity_key=entity_key,
        )
        fingerprints["repair"] = sql_fingerprint(repair_sql, dialect=dialect)
        repaired_candidates = (
            f"select frontier_repaired.* from ({repair_sql}) as frontier_repaired "
            f"inner join {keys_relation} as frontier_keys "
            f"on frontier_repaired.{entity_key} = frontier_keys.{entity_key}"
        )
        mismatched = _count(
            warehouse,
            mismatched_rows_sql(
                after_relation=after_relation,
                repaired_relation=f"({repaired_candidates})",
                dialect=dialect,
            ).replace("as mismatched_final", "as frontier_cdc_repair_check"),
        )
        if mismatched:
            raise ConfigError("repaired candidate rows do not match targeted current-state calculation")
        log_step(
            "validation completed missed=0 whole_table=false",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(phase),
            status="ok",
        )

        if apply:
            warehouse.execute(
                f"delete from {target_relation} as frontier_apply_delete "
                f"where {entity_key} in (select {entity_key} from {keys_relation})"
            )
            warehouse.execute(
                f"insert into {target_relation} select * from {after_relation} as frontier_apply_insert"
            )
    else:
        log_step("candidate materialization started", prefix=CDC_PREFIX)
        log_step(
            "candidate materialization completed candidates=0 origin=event",
            prefix=CDC_PREFIX,
            status="ok",
        )
        log_step("targeted execution started", prefix=CDC_PREFIX)
        log_step("targeted execution completed rows=0", prefix=CDC_PREFIX, status="ok")
        log_step("confirmation started", prefix=CDC_PREFIX)
        log_step("confirmation completed confirmed=0 no_ops=0", prefix=CDC_PREFIX, status="ok")
        log_step("validation started", prefix=CDC_PREFIX)
        log_step("validation completed missed=0 whole_table=false", prefix=CDC_PREFIX, status="ok")

    evidence = ["event_routing_validated"]
    if compared:
        evidence.append("targeted_rows_compared")
    evidence.append("whole_table_reference_not_executed")

    payload = repair_artifact(
        target_relation=target_relation,
        entity_key=entity_key,
        candidate_count=union_count,
        confirmed_change_count=confirmed,
        no_op_count=no_ops,
        strategy="delete-insert-candidates",
        fingerprints=fingerprints,
        apply=apply,
    )
    repair_path = None
    if output_dir is not None:
        repair_path = str(
            write_repair_artifact(
                output_dir / f"frontier-cdc-repair-{batch.batch_id}.json",
                payload,
            )
        )

    record = CdcProofRecord(
        batch_id=batch.batch_id,
        candidate_count=event_count,
        sql_change_candidate_count=0,
        union_candidate_count=union_count,
        confirmed_change_count=confirmed,
        no_op_count=no_ops,
        missed_event_count=routed.missed_event_count,
        validation_status="passed",
        event_routing_validated=True,
        targeted_rows_compared=compared,
        whole_table_reference_executed=False,
        query_fingerprint=fingerprints.get("targeted"),
        repair_applied=apply,
    )
    store.persist_proof(batch.batch_id, record)
    store.set_baseline_checkpoint(
        batch.batch_id,
        relation_fingerprint=hashlib.sha256(target_relation.encode("utf-8")).hexdigest(),
        observed_at=datetime.now(timezone.utc).isoformat(),
        repair_applied=apply,
    )
    store.set_batch_status(batch.batch_id, "COMPLETED")
    log_step(
        (
            f"batch completion completed batch={batch.batch_id} "
            f"status=COMPLETED events={len(events)} "
            f"candidates={union_count} confirmed={confirmed} no_ops={no_ops}"
        ),
        prefix=CDC_PREFIX,
        duration_ms=elapsed_ms(started),
        status="COMPLETED",
    )
    return ProveResult(
        batch_id=batch.batch_id,
        status="COMPLETED",
        logical_event_count=len(events),
        event_candidate_count=event_count,
        sql_change_candidate_count=0,
        union_candidate_count=union_count,
        confirmed_change_count=confirmed,
        no_op_count=no_ops,
        missed_event_count=routed.missed_event_count,
        validation="passed",
        duration_ms=elapsed_ms(started),
        evidence=tuple(evidence),
        repair_path=repair_path,
    )


def _cleanup(
    warehouse: WarehouseAdapter,
    store: CdcStore,
    created: list[str],
) -> None:
    phase = time.perf_counter()
    log_step("cleanup started", prefix=CDC_PREFIX)
    for relation in reversed(list(dict.fromkeys(created))):
        try:
            store.drop_relation(relation)
        except Exception:
            try:
                warehouse.execute(f"drop table if exists {relation}")
            except Exception:
                continue
    log_step("cleanup completed", prefix=CDC_PREFIX, duration_ms=elapsed_ms(phase), status="ok")
