from __future__ import annotations

import hashlib
import re
import time
from collections import defaultdict
from typing import Any

from frontier.api import build_ingest_payload, upload_run
from frontier.cdc.config import CdcConfig
from frontier.cdc.normalize import LogicalChangeEvent, operation_counts
from frontier.cdc.store import CdcBatch, CdcProofRecord, CdcStore
from frontier.config import ConfigError, FrontierConfig
from frontier.dbt_artifacts import Manifest
from frontier.github import github_source
from frontier.progress import elapsed_ms, failure_status, log_step
from frontier.warehouse import WarehouseAdapter

UPLOADED = "UPLOADED"
UPLOAD_FAILED = "FAILED"
CDC_PREFIX = "cdc"
BASELINE_STALE = (
    "BASELINE_STALE: previous CDC assessment was not applied or incorporated into the "
    "mart baseline"
)
FORBIDDEN_UPLOAD_KEY = re.compile(
    r"(entityvalue|entityid|metadata\$|row_id|warehouserow|rawevent|"
    r"primarykeybefore|primarykeyafter|targetkeybefore|targetkeyafter|"
    r"orderkey|custkey|o_orderkey|c_custkey)",
    re.IGNORECASE,
)


def cdc_external_run_id(project_name: str, batch_fingerprint: str) -> str:
    return f"{project_name}-cdc-{batch_fingerprint}"


def relation_fingerprint(
    source_model: str,
    stream_relation: str,
    inserts: int,
    updates: int,
    deletes: int,
) -> str:
    material = (
        f"{source_model}|{stream_relation}|{int(inserts)}|{int(updates)}|{int(deletes)}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        text = value.isoformat()
    else:
        text = str(value).strip()
    if not text:
        return None
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    if re.search(r"[zZ]$|[+-]\d{2}:\d{2}$", text):
        return text
    return f"{text}Z"


def assert_no_entity_fields(payload: Any, *, path: tuple[str, ...] = ()) -> None:
    if isinstance(payload, dict):
        for key, child in payload.items():
            compact = key.replace("-", "").replace("_", "")
            if FORBIDDEN_UPLOAD_KEY.search(compact):
                raise ConfigError(
                    "Refusing to upload raw entity IDs or unsupported event payload fields",
                )
            assert_no_entity_fields(child, path=path + (key,))
        return
    if isinstance(payload, list):
        for item in payload:
            assert_no_entity_fields(item, path=path)


def assert_baseline_fresh(
    store: CdcStore,
    project_name: str,
    *,
    ignoring_batch_id: str | None = None,
) -> None:
    stale = [
        batch
        for batch in store.list_batches(project_name)
        if batch.status == "COMPLETED"
        and not batch.repair_applied
        and batch.batch_id != ignoring_batch_id
    ]
    if stale:
        raise ConfigError(BASELINE_STALE)


def select_upload_batch(
    store: CdcStore,
    project_name: str,
    *,
    batch_id: str | None = None,
) -> CdcBatch:
    if batch_id:
        batch = store.get_batch(batch_id)
        if batch is None:
            raise ConfigError(f"CDC batch not found: {batch_id}")
        return batch
    pending = [
        batch
        for batch in store.list_batches(project_name)
        if batch.status == "COMPLETED"
        and (batch.upload_status or "").upper() != UPLOADED
    ]
    if not pending:
        raise ConfigError("no completed CDC batch waiting for upload")
    return pending[0]


def source_relations_payload(events: list[LogicalChangeEvent]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[LogicalChangeEvent]] = defaultdict(list)
    for event in events:
        grouped[(event.source_model, event.source_relation)].append(event)
    relations: list[dict[str, Any]] = []
    for (source_model, stream_relation), model_events in sorted(grouped.items()):
        counts = operation_counts(model_events)
        relations.append(
            {
                "sourceModel": source_model,
                "relationFingerprint": relation_fingerprint(
                    source_model,
                    stream_relation,
                    counts["inserts"],
                    counts["updates"],
                    counts["deletes"],
                ),
                "insertCount": counts["inserts"],
                "updateCount": counts["updates"],
                "deleteCount": counts["deletes"],
            }
        )
    return relations


def unresolved_event_count(events: list[LogicalChangeEvent]) -> int:
    return sum(
        1
        for event in events
        if not event.target_key_before and not event.target_key_after
    )


def mart_row_count(warehouse: WarehouseAdapter, relation: str) -> int:
    rows = warehouse.execute(
        f"select count(*) as frontier_cdc_full_count from {relation}"
    )
    if not rows:
        raise ConfigError("could not count mart rows for CDC upload")
    count = int(rows[0][0])
    if count <= 0:
        raise ConfigError("mart row count must be positive")
    return count


def _upload_error_code(error: Exception) -> str:
    text = str(error)
    match = re.search(r"HTTP (\d{3})", text)
    if match:
        return f"HTTP_{match.group(1)}"
    return type(error).__name__[:64]


def build_cdc_ingest_payload(
    *,
    batch: CdcBatch,
    proof: CdcProofRecord | None,
    events: list[LogicalChangeEvent],
    config: FrontierConfig,
    cdc_config: CdcConfig,
    manifest: Manifest,
    full_entity_count: int,
) -> dict[str, Any]:
    if not batch.payload_fingerprint:
        raise ConfigError("CDC batch fingerprint is required for upload")
    fingerprint = batch.payload_fingerprint
    if batch.status == "COMPLETED":
        if proof is None:
            raise ConfigError("completed CDC batch is missing proof evidence")
        if proof.validation_status != "passed":
            raise ConfigError("completed CDC batch did not pass targeted validation")
        status = "passed"
        checkpoint = "completed"
        targeted = "PASSED" if proof.targeted_rows_compared else "FAILED"
        if targeted != "PASSED":
            raise ConfigError(
                "cannot upload a passed CDC assessment without targeted before/after comparison",
            )
        evidence_level = "empirically_validated"
        impact = "EXECUTED"
    elif batch.status == "FAILED":
        if not batch.error_code:
            raise ConfigError(
                "failed CDC assessment requires an explicit error code",
            )
        status = "failed"
        checkpoint = "failed"
        targeted = "FAILED"
        evidence_level = "none"
        impact = "FAILED"
        proof = proof or CdcProofRecord(
            batch_id=batch.batch_id,
            candidate_count=0,
            sql_change_candidate_count=0,
            union_candidate_count=0,
            confirmed_change_count=0,
            no_op_count=0,
            missed_event_count=0,
            validation_status="failed",
            event_routing_validated=False,
            targeted_rows_compared=False,
            whole_table_reference_executed=False,
        )
    else:
        raise ConfigError(
            "cannot represent a CAPTURED or PROCESSING batch as a successful completed assessment",
        )

    if status == "passed" and (proof.missed_event_count or unresolved_event_count(events)):
        raise ConfigError("cannot upload a passed CDC assessment with unresolved or missed events")

    union_count = proof.union_candidate_count
    frontier_count = union_count
    percent = round((1 - frontier_count / full_entity_count) * 100, 3)
    model = manifest.find_model(config.model.name)
    if not model.database or not model.schema:
        raise ConfigError("Manifest model is missing database/schema")
    git = github_source()
    payload = build_ingest_payload(
        external_run_id=cdc_external_run_id(batch.project_name, fingerprint),
        project=config.project,
        environment=config.environment,
        database=str(model.database),
        schema=str(model.schema),
        model_unique_id=model.unique_id,
        model_name=config.model.name,
        entity_type=config.model.entity,
        entity_key=config.model.key,
        grain=config.model.grain,
        metrics={
            "fullEntityCount": full_entity_count,
            "frontierEntityCount": frontier_count,
            "percentRowsAvoided": percent,
            "eventCandidateCount": proof.candidate_count,
            "sqlChangeCandidateCount": proof.sql_change_candidate_count,
            "unionCandidateCount": union_count,
            "confirmedEntityCount": proof.confirmed_change_count,
            "candidateNoOpCount": proof.no_op_count,
            "missedEntityCount": proof.missed_event_count,
            "candidateFrontierCount": union_count,
            "confirmedFrontierCount": proof.confirmed_change_count,
            "missingFrontierEntities": proof.missed_event_count,
        },
        change_events=[],
        affected_entities=[],
        validation_results=(
            [
                {
                    "testName": "cdc_batch",
                    "status": "failed",
                    "message": batch.error_code,
                }
            ]
            if status == "failed"
            else []
        ),
        evidence_level=evidence_level,
        status=status,
        git=git,
        entity_ids_hashed=False,
        warehouse_type="snowflake",
        run_mode="live",
        candidate_set_origin="event",
        assessment_type="cdc",
        cdc={
            "provider": cdc_config.provider,
            "batchFingerprint": fingerprint,
            "sourceRelations": source_relations_payload(events),
            "rawRecordCount": batch.raw_record_count,
            "logicalEventCount": batch.logical_event_count,
            "unresolvedEventCount": unresolved_event_count(events),
            **(
                {"capturedAt": captured}
                if (captured := iso_datetime(batch.captured_at))
                else {}
            ),
            **(
                {"completedAt": completed}
                if (completed := iso_datetime(batch.completed_at))
                else {}
            ),
            "checkpointStatus": checkpoint,
            "impactExecution": impact,
            "targetedValidation": targeted,
            "fullReferenceValidation": (
                "PASSED" if proof.whole_table_reference_executed else "NOT_RUN"
            ),
            "repairApplied": bool(proof.repair_applied or batch.repair_applied),
            **(
                {
                    "baselineRelationFingerprint": batch.baseline_relation_fingerprint,
                }
                if batch.baseline_relation_fingerprint
                else {}
            ),
            **(
                {"baselineObservedAt": observed}
                if (observed := iso_datetime(batch.baseline_observed_at))
                else {}
            ),
        },
    )
    assert_no_entity_fields(payload.get("cdc"))
    assert_no_entity_fields(payload.get("changeEvents"))
    assert_no_entity_fields(payload.get("affectedEntities"))
    return payload


def upload_cdc_batch(
    *,
    store: CdcStore,
    warehouse: WarehouseAdapter,
    cdc_config: CdcConfig,
    frontier_config: FrontierConfig,
    manifest: Manifest,
    project_name: str,
    api_url: str,
    api_key: str,
    batch_id: str | None = None,
) -> dict[str, Any]:
    store.ensure_control_tables()
    batch = select_upload_batch(store, project_name, batch_id=batch_id)
    proof = store.get_proof(batch.batch_id)
    events = store.list_events(batch.batch_id)
    model = manifest.find_model(frontier_config.model.name)
    full_entity_count = mart_row_count(warehouse, model.relation)
    payload = build_cdc_ingest_payload(
        batch=batch,
        proof=proof,
        events=events,
        config=frontier_config,
        cdc_config=cdc_config,
        manifest=manifest,
        full_entity_count=full_entity_count,
    )
    external_run_id = str(payload["externalRunId"])
    started = time.perf_counter()
    log_step("upload started", prefix=CDC_PREFIX)
    try:
        response = upload_run(payload, api_url=api_url, api_key=api_key)
    except Exception as error:
        store.record_upload(
            batch.batch_id,
            upload_status=UPLOAD_FAILED,
            external_run_id=external_run_id,
            error_code=_upload_error_code(error),
        )
        log_step(
            "upload completed",
            prefix=CDC_PREFIX,
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    saas_run_id = response.get("id")
    store.record_upload(
        batch.batch_id,
        upload_status=UPLOADED,
        external_run_id=external_run_id,
        saas_run_id=str(saas_run_id) if saas_run_id else None,
    )
    log_step(
        "upload completed",
        prefix=CDC_PREFIX,
        duration_ms=elapsed_ms(started),
        status="ok",
    )
    http_status = response.pop("_httpStatus", None)
    return {
        "httpStatus": http_status,
        "created": response.get("created"),
        "id": response.get("id"),
        "externalRunId": response.get("externalRunId") or external_run_id,
        "assessmentType": "cdc",
        "batchId": batch.batch_id,
        "uploadStatus": UPLOADED,
        **response,
    }
