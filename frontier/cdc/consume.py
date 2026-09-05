from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from frontier import __version__
from frontier.cdc.config import CdcConfig, CdcSourceConfig
from frontier.cdc.normalize import batch_fingerprint, normalize_stream_records, operation_counts
from frontier.cdc.store import CdcBatch, CdcStore
from frontier.config import ConfigError, load_frontier_config
from frontier.progress import elapsed_ms, failure_status, log_step


@dataclass(frozen=True)
class ConsumeResult:
    stream_name: str
    batch_id: str | None
    raw_record_count: int
    logical_event_count: int
    operation_counts: dict[str, int]
    status: str
    duration_ms: int


def new_batch_id() -> str:
    return f"cdc-{uuid.uuid4().hex}"


def consume_source(
    source: CdcSourceConfig,
    *,
    store: CdcStore,
    config: CdcConfig,
    project_name: str,
) -> ConsumeResult:
    started = time.perf_counter()
    log_step(f"capture started stream={source.stream_name}", prefix="cdc")
    try:
        result = _consume_source(
            source,
            store=store,
            config=config,
            project_name=project_name,
            started=started,
        )
    except Exception as error:
        log_step(
            f"capture completed stream={source.stream_name}",
            prefix="cdc",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    counts = result.operation_counts
    log_step(
        (
            f"capture completed stream={source.stream_name} "
            f"batch={result.batch_id or '-'} "
            f"raw={result.raw_record_count} logical={result.logical_event_count} "
            f"inserts={counts['inserts']} updates={counts['updates']} "
            f"deletes={counts['deletes']}"
        ),
        prefix="cdc",
        duration_ms=result.duration_ms,
        status=result.status,
    )
    return result


def _consume_source(
    source: CdcSourceConfig,
    *,
    store: CdcStore,
    config: CdcConfig,
    project_name: str,
    started: float,
) -> ConsumeResult:
    empty_counts = {"inserts": 0, "updates": 0, "deletes": 0}
    if not store.stream_has_data(source.stream_relation):
        existing = store.latest_captured_batch(source.stream_relation)
        if existing is not None:
            return ConsumeResult(
                stream_name=source.stream_name,
                batch_id=existing.batch_id,
                raw_record_count=existing.raw_record_count,
                logical_event_count=existing.logical_event_count,
                operation_counts=empty_counts,
                status="REUSED",
                duration_ms=elapsed_ms(started),
            )
        return ConsumeResult(
            stream_name=source.stream_name,
            batch_id=None,
            raw_record_count=0,
            logical_event_count=0,
            operation_counts=empty_counts,
            status="EMPTY",
            duration_ms=elapsed_ms(started),
        )

    batch_id = new_batch_id()
    if not store.acquire_claim(source.stream_relation, batch_id):
        raise ConfigError("CDC stream is already claimed by another consumer")
    claimed = True
    in_txn = False
    try:
        store.begin()
        in_txn = True
        batch = CdcBatch(
            batch_id=batch_id,
            project_name=project_name,
            stream_relation=source.stream_relation,
            status="CAPTURING",
            runner_version=__version__,
        )
        store.insert_batch(batch)
        raw_relation, raw_count = store.capture_stream(source, batch_id)
        records = store.read_raw(raw_relation, source)
        events = normalize_stream_records(
            records,
            source=source,
            provider=config.provider,
        )
        fingerprints = tuple(event.payload_fingerprint for event in events)
        if store.existing_fingerprints(fingerprints):
            raise ConfigError("duplicate CDC event fingerprint")
        store.insert_events(batch_id, events)
        batch.status = "CAPTURED"
        batch.raw_record_count = raw_count
        batch.logical_event_count = len(events)
        batch.payload_fingerprint = batch_fingerprint(events)
        store.update_batch(batch)
        store.commit()
        in_txn = False
        return ConsumeResult(
            stream_name=source.stream_name,
            batch_id=batch_id,
            raw_record_count=raw_count,
            logical_event_count=len(events),
            operation_counts=operation_counts(events),
            status="CAPTURED",
            duration_ms=elapsed_ms(started),
        )
    except Exception:
        if in_txn:
            store.rollback()
        raise
    finally:
        if claimed:
            store.release_claim(source.stream_relation, batch_id)


def consume_all(config: CdcConfig, *, store: CdcStore, project_name: str) -> list[ConsumeResult]:
    store.ensure_control_tables()
    return [
        consume_source(source, store=store, config=config, project_name=project_name)
        for source in config.sources
    ]


def project_name_for(project_dir) -> str:
    from pathlib import Path

    config_path = Path(project_dir) / "frontier.yml"
    if config_path.is_file():
        try:
            return load_frontier_config(config_path).project
        except ConfigError:
            pass
    return Path(project_dir).name
