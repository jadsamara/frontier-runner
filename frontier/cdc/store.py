from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from frontier.cdc.config import CdcConfig, CdcSourceConfig
from frontier.cdc.normalize import LogicalChangeEvent, StreamRecord, records_from_rows
from frontier.config import ConfigError
from frontier.execute import isolated_table_name
from frontier.warehouse import WarehouseAdapter, sql_string, split_relation_parts

BATCHES_TABLE = "FRONTIER_CDC_BATCHES"
EVENTS_TABLE = "FRONTIER_CDC_EVENTS"
CLAIMS_TABLE = "FRONTIER_CDC_CLAIMS"
PROOFS_TABLE = "FRONTIER_CDC_PROOFS"
RAW_SUFFIX = "CDC_RAW"
PROOF_CLAIM_PREFIX = "proof:"
PROVABLE_STATUSES = frozenset({"CAPTURED", "FAILED"})


@dataclass
class CdcBatch:
    batch_id: str
    project_name: str
    stream_relation: str
    status: str
    raw_record_count: int = 0
    logical_event_count: int = 0
    error_code: str | None = None
    payload_fingerprint: str | None = None
    runner_version: str | None = None
    started_at: str | None = None
    captured_at: str | None = None
    completed_at: str | None = None
    upload_status: str | None = None
    uploaded_at: str | None = None
    external_run_id: str | None = None
    saas_run_id: str | None = None
    upload_attempt_count: int = 0
    last_upload_error_code: str | None = None
    repair_applied: bool = False
    baseline_relation_fingerprint: str | None = None
    baseline_observed_at: str | None = None


@dataclass(frozen=True)
class CdcProofRecord:
    batch_id: str
    candidate_count: int
    sql_change_candidate_count: int
    union_candidate_count: int
    confirmed_change_count: int
    no_op_count: int
    missed_event_count: int
    validation_status: str
    event_routing_validated: bool
    targeted_rows_compared: bool
    whole_table_reference_executed: bool
    query_fingerprint: str | None = None
    repair_applied: bool = False


class CdcStore(Protocol):
    def ensure_control_tables(self) -> None: ...

    def stream_has_data(self, stream_relation: str) -> bool: ...

    def acquire_claim(self, stream_relation: str, batch_id: str) -> bool: ...

    def release_claim(self, stream_relation: str, batch_id: str) -> None: ...

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def insert_batch(self, batch: CdcBatch) -> None: ...

    def update_batch(self, batch: CdcBatch) -> None: ...

    def latest_captured_batch(self, stream_relation: str) -> CdcBatch | None: ...

    def capture_stream(self, source: CdcSourceConfig, batch_id: str) -> tuple[str, int]: ...

    def read_raw(self, raw_relation: str, source: CdcSourceConfig) -> list[StreamRecord]: ...

    def existing_fingerprints(self, fingerprints: tuple[str, ...]) -> set[str]: ...

    def insert_events(self, batch_id: str, events: list[LogicalChangeEvent]) -> None: ...

    def get_batch(self, batch_id: str) -> CdcBatch | None: ...

    def oldest_provable_batch(self, project_name: str) -> CdcBatch | None: ...

    def claim_proof(self, batch_id: str) -> CdcBatch | None: ...

    def claim_oldest_provable(self, project_name: str) -> CdcBatch | None: ...

    def release_proof_claim(self, batch_id: str) -> None: ...

    def set_batch_status(
        self,
        batch_id: str,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None: ...

    def list_events(self, batch_id: str) -> list[LogicalChangeEvent]: ...

    def persist_proof(self, batch_id: str, proof: CdcProofRecord) -> None: ...

    def get_proof(self, batch_id: str) -> CdcProofRecord | None: ...

    def drop_relation(self, relation: str) -> None: ...

    def list_batches(self, project_name: str) -> list[CdcBatch]: ...

    def record_upload(
        self,
        batch_id: str,
        *,
        upload_status: str,
        external_run_id: str | None = None,
        saas_run_id: str | None = None,
        error_code: str | None = None,
    ) -> None: ...

    def set_baseline_checkpoint(
        self,
        batch_id: str,
        *,
        relation_fingerprint: str,
        observed_at: str,
        repair_applied: bool,
    ) -> None: ...


def qualify(database: str, schema: str, table: str) -> str:
    return f"{database}.{schema}.{table}"


def raw_table_name(batch_id: str) -> str:
    return isolated_table_name(batch_id, RAW_SUFFIX)


class FakeCdcStore:
    """In-memory CDC store for unit tests. Never opens a vendor client."""

    def __init__(self, streams: dict[str, list[dict[str, Any]]] | None = None):
        self.streams = {key.upper(): list(value) for key, value in (streams or {}).items()}
        self.raw_tables: dict[str, list[dict[str, Any]]] = {}
        self.claims: dict[str, str] = {}
        self.batches: list[CdcBatch] = []
        self.events: list[tuple[str, LogicalChangeEvent]] = []
        self._txn: dict[str, Any] | None = None
        self.proofs: dict[str, CdcProofRecord] = {}
        self.proof_claims: set[str] = set()
        self.dropped: list[str] = []
        self.fail_capture = False
        self.block_claims = False
        self.block_proof_claims = False

    def ensure_control_tables(self) -> None:
        return None

    def stream_has_data(self, stream_relation: str) -> bool:
        return bool(self.streams.get(stream_relation.upper()))

    def acquire_claim(self, stream_relation: str, batch_id: str) -> bool:
        if self.block_claims:
            return False
        key = stream_relation.upper()
        if key in self.claims:
            return False
        self.claims[key] = batch_id
        return True

    def release_claim(self, stream_relation: str, batch_id: str) -> None:
        key = stream_relation.upper()
        if self.claims.get(key) == batch_id:
            del self.claims[key]

    def begin(self) -> None:
        self._txn = {
            "streams": copy.deepcopy(self.streams),
            "raw_tables": copy.deepcopy(self.raw_tables),
            "batches": copy.deepcopy(self.batches),
            "events": copy.deepcopy(self.events),
        }

    def commit(self) -> None:
        self._txn = None

    def rollback(self) -> None:
        if self._txn is None:
            return
        self.streams = self._txn["streams"]
        self.raw_tables = self._txn["raw_tables"]
        self.batches = self._txn["batches"]
        self.events = self._txn["events"]
        self._txn = None

    def insert_batch(self, batch: CdcBatch) -> None:
        copied = copy.copy(batch)
        if not copied.started_at:
            copied.started_at = datetime.now(timezone.utc).isoformat()
        self.batches.append(copied)

    def update_batch(self, batch: CdcBatch) -> None:
        for index, existing in enumerate(self.batches):
            if existing.batch_id == batch.batch_id:
                self.batches[index] = batch
                return
        raise ConfigError("CDC batch not found")

    def latest_captured_batch(self, stream_relation: str) -> CdcBatch | None:
        matches = [
            batch
            for batch in self.batches
            if batch.stream_relation.upper() == stream_relation.upper()
            and batch.status == "CAPTURED"
        ]
        return matches[-1] if matches else None

    def capture_stream(self, source: CdcSourceConfig, batch_id: str) -> tuple[str, int]:
        if self.fail_capture:
            raise ConfigError("CDC capture DML failed")
        key = source.stream_relation.upper()
        rows = list(self.streams.get(key) or [])
        database, schema, _table = split_relation_parts(source.stream_relation)
        raw_relation = qualify(database or "", schema or "", raw_table_name(batch_id))
        self.raw_tables[raw_relation.upper()] = rows
        self.streams[key] = []
        if self.fail_capture:
            raise ConfigError("CDC capture DML failed")
        return raw_relation, len(rows)

    def read_raw(self, raw_relation: str, source: CdcSourceConfig) -> list[StreamRecord]:
        del source
        rows = self.raw_tables.get(raw_relation.upper()) or []
        return records_from_rows(rows)

    def existing_fingerprints(self, fingerprints: tuple[str, ...]) -> set[str]:
        wanted = set(fingerprints)
        return {
            event.payload_fingerprint
            for _batch_id, event in self.events
            if event.payload_fingerprint in wanted
        }

    def insert_events(self, batch_id: str, events: list[LogicalChangeEvent]) -> None:
        for event in events:
            self.events.append((batch_id, event))

    def get_batch(self, batch_id: str) -> CdcBatch | None:
        for batch in self.batches:
            if batch.batch_id == batch_id:
                return batch
        return None

    def oldest_provable_batch(self, project_name: str) -> CdcBatch | None:
        for batch in self.batches:
            if batch.project_name == project_name and batch.status in PROVABLE_STATUSES:
                return batch
        return None

    def claim_proof(self, batch_id: str) -> CdcBatch | None:
        if self.block_proof_claims:
            return None
        batch = self.get_batch(batch_id)
        if batch is None or batch.status not in PROVABLE_STATUSES:
            return None
        if batch_id in self.proof_claims:
            return None
        self.proof_claims.add(batch_id)
        batch.status = "PROCESSING"
        return batch

    def claim_oldest_provable(self, project_name: str) -> CdcBatch | None:
        candidate = self.oldest_provable_batch(project_name)
        if candidate is None:
            return None
        return self.claim_proof(candidate.batch_id)

    def release_proof_claim(self, batch_id: str) -> None:
        self.proof_claims.discard(batch_id)

    def set_batch_status(
        self,
        batch_id: str,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None:
        batch = self.get_batch(batch_id)
        if batch is None:
            raise ConfigError("CDC batch not found")
        batch.status = status
        batch.error_code = error_code
        if status == "CAPTURED" and not batch.captured_at:
            batch.captured_at = datetime.now(timezone.utc).isoformat()
        if status == "COMPLETED":
            batch.completed_at = datetime.now(timezone.utc).isoformat()

    def list_events(self, batch_id: str) -> list[LogicalChangeEvent]:
        return [event for stored_id, event in self.events if stored_id == batch_id]

    def persist_proof(self, batch_id: str, proof: CdcProofRecord) -> None:
        self.proofs[batch_id] = proof
        batch = self.get_batch(batch_id)
        if batch is not None:
            batch.repair_applied = proof.repair_applied

    def get_proof(self, batch_id: str) -> CdcProofRecord | None:
        return self.proofs.get(batch_id)

    def drop_relation(self, relation: str) -> None:
        self.dropped.append(relation)
        self.raw_tables.pop(relation.upper(), None)

    def list_batches(self, project_name: str) -> list[CdcBatch]:
        matches = [batch for batch in self.batches if batch.project_name == project_name]
        return list(reversed(matches))

    def record_upload(
        self,
        batch_id: str,
        *,
        upload_status: str,
        external_run_id: str | None = None,
        saas_run_id: str | None = None,
        error_code: str | None = None,
    ) -> None:
        batch = self.get_batch(batch_id)
        if batch is None:
            raise ConfigError("CDC batch not found")
        batch.upload_attempt_count += 1
        batch.upload_status = upload_status
        if external_run_id:
            batch.external_run_id = external_run_id
        if saas_run_id:
            batch.saas_run_id = saas_run_id
        if upload_status == "UPLOADED":
            batch.uploaded_at = datetime.now(timezone.utc).isoformat()
            batch.last_upload_error_code = None
        else:
            batch.last_upload_error_code = error_code

    def set_baseline_checkpoint(
        self,
        batch_id: str,
        *,
        relation_fingerprint: str,
        observed_at: str,
        repair_applied: bool,
    ) -> None:
        batch = self.get_batch(batch_id)
        if batch is None:
            raise ConfigError("CDC batch not found")
        batch.baseline_relation_fingerprint = relation_fingerprint
        batch.baseline_observed_at = observed_at
        batch.repair_applied = repair_applied


def _q(database: str, schema: str, table: str) -> str:
    return f"{database}.{schema}.{table}"


BATCH_COLUMNS = (
    "batch_id, project_name, stream_relation, status, "
    "raw_record_count, logical_event_count, error_code, "
    "payload_fingerprint, runner_version, started_at, captured_at, completed_at, "
    "upload_status, uploaded_at, external_run_id, saas_run_id, "
    "upload_attempt_count, last_upload_error_code, repair_applied, "
    "baseline_relation_fingerprint, baseline_observed_at"
)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"true", "1"}


def _cell(row: tuple[Any, ...], index: int) -> Any:
    return row[index] if len(row) > index else None


class SnowflakeCdcStore:
    """Durable Snowflake control tables and transactional stream capture."""

    def __init__(self, warehouse: WarehouseAdapter, config: CdcConfig):
        self.warehouse = warehouse
        self.database = config.control_database
        self.schema = config.control_schema
        self.batches = _q(self.database, self.schema, BATCHES_TABLE)
        self.events = _q(self.database, self.schema, EVENTS_TABLE)
        self.claims = _q(self.database, self.schema, CLAIMS_TABLE)
        self.proofs = _q(self.database, self.schema, PROOFS_TABLE)

    def ensure_control_tables(self) -> None:
        self.warehouse.execute(
            f"create schema if not exists {self.database}.{self.schema}"
        )
        self.warehouse.execute(
            f"create table if not exists {self.batches} ("
            "batch_id varchar not null, "
            "project_name varchar not null, "
            "stream_relation varchar not null, "
            "status varchar not null, "
            "started_at timestamp_ntz not null, "
            "captured_at timestamp_ntz, "
            "completed_at timestamp_ntz, "
            "raw_record_count number, "
            "logical_event_count number, "
            "error_code varchar, "
            "payload_fingerprint varchar, "
            "runner_version varchar"
            ")"
        )
        self.warehouse.execute(
            f"create table if not exists {self.events} ("
            "batch_id varchar not null, "
            "event_id varchar not null, "
            "source_model varchar not null, "
            "source_relation varchar not null, "
            "provider varchar not null, "
            "operation varchar not null, "
            "provider_row_id varchar, "
            "is_update boolean, "
            "primary_key_before varchar, "
            "primary_key_after varchar, "
            "target_key_before varchar, "
            "target_key_after varchar, "
            "event_time timestamp_ntz, "
            "ingestion_time timestamp_ntz not null, "
            "payload_fingerprint varchar not null, "
            "processing_status varchar not null"
            ")"
        )
        self.warehouse.execute(
            f"create table if not exists {self.claims} ("
            "stream_relation varchar not null, "
            "batch_id varchar not null, "
            "claimed_at timestamp_ntz not null"
            ")"
        )
        self.warehouse.execute(
            f"create table if not exists {self.proofs} ("
            "batch_id varchar not null, "
            "candidate_count number, "
            "sql_change_candidate_count number, "
            "union_candidate_count number, "
            "confirmed_change_count number, "
            "no_op_count number, "
            "missed_event_count number, "
            "validation_status varchar, "
            "event_routing_validated boolean, "
            "targeted_rows_compared boolean, "
            "whole_table_reference_executed boolean, "
            "query_fingerprint varchar, "
            "recorded_at timestamp_ntz not null"
            ")"
        )
        for statement in (
            f"alter table {self.batches} add column if not exists upload_status varchar",
            f"alter table {self.batches} add column if not exists uploaded_at timestamp_ntz",
            f"alter table {self.batches} add column if not exists external_run_id varchar",
            f"alter table {self.batches} add column if not exists saas_run_id varchar",
            f"alter table {self.batches} add column if not exists upload_attempt_count number",
            f"alter table {self.batches} add column if not exists last_upload_error_code varchar",
            f"alter table {self.batches} add column if not exists repair_applied boolean",
            f"alter table {self.batches} add column if not exists baseline_relation_fingerprint varchar",
            f"alter table {self.batches} add column if not exists baseline_observed_at timestamp_ntz",
            f"alter table {self.proofs} add column if not exists repair_applied boolean",
        ):
            self.warehouse.execute(statement)

    def stream_has_data(self, stream_relation: str) -> bool:
        rows = self.warehouse.execute(
            f"select system$stream_has_data({sql_string(stream_relation)})"
        )
        if not rows:
            return False
        value = rows[0][0]
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"true", "1"}

    def acquire_claim(self, stream_relation: str, batch_id: str) -> bool:
        self.warehouse.execute(
            f"insert into {self.claims} (stream_relation, batch_id, claimed_at) "
            f"select {sql_string(stream_relation)}, {sql_string(batch_id)}, current_timestamp() "
            f"where not exists ("
            f"select 1 from {self.claims} "
            f"where stream_relation = {sql_string(stream_relation)}"
            ")"
        )
        rows = self.warehouse.execute(
            f"select count(*) from {self.claims} "
            f"where stream_relation = {sql_string(stream_relation)} "
            f"and batch_id = {sql_string(batch_id)}"
        )
        return bool(rows and int(rows[0][0] or 0) > 0)

    def release_claim(self, stream_relation: str, batch_id: str) -> None:
        self.warehouse.execute(
            f"delete from {self.claims} "
            f"where stream_relation = {sql_string(stream_relation)} "
            f"and batch_id = {sql_string(batch_id)}"
        )

    def begin(self) -> None:
        connection = getattr(self.warehouse, "_connection", None)
        if connection is not None and hasattr(connection, "autocommit"):
            connection.autocommit(False)
        self.warehouse.execute("begin")

    def commit(self) -> None:
        connection = getattr(self.warehouse, "_connection", None)
        if connection is not None and hasattr(connection, "commit"):
            connection.commit()
        else:
            self.warehouse.execute("commit")

    def rollback(self) -> None:
        connection = getattr(self.warehouse, "_connection", None)
        if connection is not None and hasattr(connection, "rollback"):
            connection.rollback()
        else:
            self.warehouse.execute("rollback")

    def insert_batch(self, batch: CdcBatch) -> None:
        self.warehouse.execute(
            f"insert into {self.batches} ("
            "batch_id, project_name, stream_relation, status, started_at, "
            "raw_record_count, logical_event_count, error_code, "
            "payload_fingerprint, runner_version"
            ") values ("
            f"{sql_string(batch.batch_id)}, "
            f"{sql_string(batch.project_name)}, "
            f"{sql_string(batch.stream_relation)}, "
            f"{sql_string(batch.status)}, "
            "current_timestamp(), "
            f"{int(batch.raw_record_count)}, "
            f"{int(batch.logical_event_count)}, "
            f"{sql_string(batch.error_code) if batch.error_code else 'null'}, "
            f"{sql_string(batch.payload_fingerprint) if batch.payload_fingerprint else 'null'}, "
            f"{sql_string(batch.runner_version) if batch.runner_version else 'null'}"
            ")"
        )

    def update_batch(self, batch: CdcBatch) -> None:
        captured = "current_timestamp()" if batch.status == "CAPTURED" else "captured_at"
        completed = "current_timestamp()" if batch.status == "COMPLETED" else "completed_at"
        self.warehouse.execute(
            f"update {self.batches} set "
            f"status = {sql_string(batch.status)}, "
            f"raw_record_count = {int(batch.raw_record_count)}, "
            f"logical_event_count = {int(batch.logical_event_count)}, "
            f"error_code = {sql_string(batch.error_code) if batch.error_code else 'null'}, "
            f"payload_fingerprint = {sql_string(batch.payload_fingerprint) if batch.payload_fingerprint else 'null'}, "
            f"captured_at = {captured}, "
            f"completed_at = {completed} "
            f"where batch_id = {sql_string(batch.batch_id)}"
        )

    def latest_captured_batch(self, stream_relation: str) -> CdcBatch | None:
        rows = self.warehouse.execute(
            f"select {BATCH_COLUMNS} from {self.batches} "
            f"where stream_relation = {sql_string(stream_relation)} "
            "and status = 'CAPTURED' "
            "order by started_at desc limit 1"
        )
        if not rows:
            return None
        return self._batch_from_row(rows[0])

    def capture_stream(self, source: CdcSourceConfig, batch_id: str) -> tuple[str, int]:
        raw_relation = _q(self.database, self.schema, raw_table_name(batch_id))
        self.warehouse.execute(
            f"create table {raw_relation} as select * from {source.stream_relation}"
        )
        rows = self.warehouse.execute(f"select count(*) from {raw_relation}")
        count = int(rows[0][0] or 0) if rows else 0
        return raw_relation, count

    def read_raw(self, raw_relation: str, source: CdcSourceConfig) -> list[StreamRecord]:
        rows = self.warehouse.execute(
            "select metadata$action, metadata$isupdate, metadata$row_id, "
            f"{source.primary_key}, {source.target_key} "
            f"from {raw_relation}"
        )
        return records_from_rows(rows)

    def existing_fingerprints(self, fingerprints: tuple[str, ...]) -> set[str]:
        if not fingerprints:
            return set()
        values = ", ".join(sql_string(item) for item in fingerprints)
        rows = self.warehouse.execute(
            f"select payload_fingerprint from {self.events} "
            f"where payload_fingerprint in ({values})"
        )
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def insert_events(self, batch_id: str, events: list[LogicalChangeEvent]) -> None:
        for event in events:
            self.warehouse.execute(
                f"insert into {self.events} ("
                "batch_id, event_id, source_model, source_relation, provider, "
                "operation, provider_row_id, is_update, primary_key_before, "
                "primary_key_after, target_key_before, target_key_after, "
                "event_time, ingestion_time, payload_fingerprint, processing_status"
                ") values ("
                f"{sql_string(batch_id)}, "
                f"{sql_string(event.event_id)}, "
                f"{sql_string(event.source_model)}, "
                f"{sql_string(event.source_relation)}, "
                f"{sql_string(event.provider)}, "
                f"{sql_string(event.operation)}, "
                f"{sql_string(event.provider_row_id)}, "
                f"{'true' if event.is_update else 'false'}, "
                f"{sql_string(event.primary_key_before) if event.primary_key_before is not None else 'null'}, "
                f"{sql_string(event.primary_key_after) if event.primary_key_after is not None else 'null'}, "
                f"{sql_string(event.target_key_before) if event.target_key_before is not None else 'null'}, "
                f"{sql_string(event.target_key_after) if event.target_key_after is not None else 'null'}, "
                "null, current_timestamp(), "
                f"{sql_string(event.payload_fingerprint)}, "
                "'CAPTURED'"
                ")"
            )

    def _batch_from_row(self, row: tuple[Any, ...]) -> CdcBatch:
        return CdcBatch(
            batch_id=str(row[0]),
            project_name=str(row[1]),
            stream_relation=str(row[2]),
            status=str(row[3]),
            raw_record_count=int(row[4] or 0),
            logical_event_count=int(row[5] or 0),
            error_code=str(row[6]) if row[6] is not None else None,
            payload_fingerprint=str(row[7]) if row[7] is not None else None,
            runner_version=str(row[8]) if row[8] is not None else None,
            started_at=str(_cell(row, 9)) if _cell(row, 9) is not None else None,
            captured_at=str(_cell(row, 10)) if _cell(row, 10) is not None else None,
            completed_at=str(_cell(row, 11)) if _cell(row, 11) is not None else None,
            upload_status=str(_cell(row, 12)) if _cell(row, 12) is not None else None,
            uploaded_at=str(_cell(row, 13)) if _cell(row, 13) is not None else None,
            external_run_id=str(_cell(row, 14)) if _cell(row, 14) is not None else None,
            saas_run_id=str(_cell(row, 15)) if _cell(row, 15) is not None else None,
            upload_attempt_count=int(_cell(row, 16) or 0),
            last_upload_error_code=(
                str(_cell(row, 17)) if _cell(row, 17) is not None else None
            ),
            repair_applied=_as_bool(_cell(row, 18)),
            baseline_relation_fingerprint=(
                str(_cell(row, 19)) if _cell(row, 19) is not None else None
            ),
            baseline_observed_at=(
                str(_cell(row, 20)) if _cell(row, 20) is not None else None
            ),
        )

    def get_batch(self, batch_id: str) -> CdcBatch | None:
        rows = self.warehouse.execute(
            f"select {BATCH_COLUMNS} from {self.batches} "
            f"where batch_id = {sql_string(batch_id)}"
        )
        if not rows:
            return None
        return self._batch_from_row(rows[0])

    def oldest_provable_batch(self, project_name: str) -> CdcBatch | None:
        rows = self.warehouse.execute(
            f"select {BATCH_COLUMNS} from {self.batches} "
            f"where project_name = {sql_string(project_name)} "
            "and status in ('CAPTURED', 'FAILED') "
            "order by started_at asc, batch_id asc limit 1"
        )
        if not rows:
            return None
        return self._batch_from_row(rows[0])

    def claim_proof(self, batch_id: str) -> CdcBatch | None:
        claim_key = f"{PROOF_CLAIM_PREFIX}{batch_id}"
        if not self.acquire_claim(claim_key, batch_id):
            return None
        self.warehouse.execute(
            f"update {self.batches} set status = 'PROCESSING' "
            f"where batch_id = {sql_string(batch_id)} "
            "and status in ('CAPTURED', 'FAILED')"
        )
        batch = self.get_batch(batch_id)
        if batch is None or batch.status != "PROCESSING":
            self.release_claim(claim_key, batch_id)
            return None
        return batch

    def claim_oldest_provable(self, project_name: str) -> CdcBatch | None:
        for _ in range(8):
            candidate = self.oldest_provable_batch(project_name)
            if candidate is None:
                return None
            claimed = self.claim_proof(candidate.batch_id)
            if claimed is not None:
                return claimed
        return None

    def release_proof_claim(self, batch_id: str) -> None:
        self.release_claim(f"{PROOF_CLAIM_PREFIX}{batch_id}", batch_id)

    def set_batch_status(
        self,
        batch_id: str,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None:
        completed = "current_timestamp()" if status == "COMPLETED" else "completed_at"
        self.warehouse.execute(
            f"update {self.batches} set "
            f"status = {sql_string(status)}, "
            f"error_code = {sql_string(error_code) if error_code else 'null'}, "
            f"completed_at = {completed} "
            f"where batch_id = {sql_string(batch_id)}"
        )
        processing = "COMPLETED" if status == "COMPLETED" else status
        self.warehouse.execute(
            f"update {self.events} set processing_status = {sql_string(processing)} "
            f"where batch_id = {sql_string(batch_id)}"
        )

    def list_events(self, batch_id: str) -> list[LogicalChangeEvent]:
        rows = self.warehouse.execute(
            "select event_id, source_model, source_relation, provider, operation, "
            "provider_row_id, is_update, primary_key_before, primary_key_after, "
            "target_key_before, target_key_after, payload_fingerprint "
            f"from {self.events} "
            f"where batch_id = {sql_string(batch_id)} "
            "order by event_id"
        )
        events: list[LogicalChangeEvent] = []
        for row in rows:
            is_update = row[6]
            if not isinstance(is_update, bool):
                is_update = str(is_update).strip().lower() in {"true", "1"}
            events.append(
                LogicalChangeEvent(
                    event_id=str(row[0]),
                    source_model=str(row[1]),
                    source_relation=str(row[2]),
                    provider=str(row[3]),
                    operation=str(row[4]),
                    provider_row_id=str(row[5] or ""),
                    is_update=bool(is_update),
                    primary_key_before=str(row[7]) if row[7] is not None else None,
                    primary_key_after=str(row[8]) if row[8] is not None else None,
                    target_key_before=str(row[9]) if row[9] is not None else None,
                    target_key_after=str(row[10]) if row[10] is not None else None,
                    payload_fingerprint=str(row[11]),
                )
            )
        return events

    def persist_proof(self, batch_id: str, proof: CdcProofRecord) -> None:
        self.warehouse.execute(
            f"delete from {self.proofs} where batch_id = {sql_string(batch_id)}"
        )
        self.warehouse.execute(
            f"insert into {self.proofs} ("
            "batch_id, candidate_count, sql_change_candidate_count, "
            "union_candidate_count, confirmed_change_count, no_op_count, "
            "missed_event_count, validation_status, event_routing_validated, "
            "targeted_rows_compared, whole_table_reference_executed, "
            "query_fingerprint, recorded_at, repair_applied"
            ") values ("
            f"{sql_string(batch_id)}, "
            f"{int(proof.candidate_count)}, "
            f"{int(proof.sql_change_candidate_count)}, "
            f"{int(proof.union_candidate_count)}, "
            f"{int(proof.confirmed_change_count)}, "
            f"{int(proof.no_op_count)}, "
            f"{int(proof.missed_event_count)}, "
            f"{sql_string(proof.validation_status)}, "
            f"{'true' if proof.event_routing_validated else 'false'}, "
            f"{'true' if proof.targeted_rows_compared else 'false'}, "
            f"{'true' if proof.whole_table_reference_executed else 'false'}, "
            f"{sql_string(proof.query_fingerprint) if proof.query_fingerprint else 'null'}, "
            "current_timestamp(), "
            f"{'true' if proof.repair_applied else 'false'}"
            ")"
        )

    def get_proof(self, batch_id: str) -> CdcProofRecord | None:
        rows = self.warehouse.execute(
            "select batch_id, candidate_count, sql_change_candidate_count, "
            "union_candidate_count, confirmed_change_count, no_op_count, "
            "missed_event_count, validation_status, event_routing_validated, "
            "targeted_rows_compared, whole_table_reference_executed, query_fingerprint, "
            "repair_applied "
            f"from {self.proofs} "
            f"where batch_id = {sql_string(batch_id)}"
        )
        if not rows:
            return None
        row = rows[0]
        return CdcProofRecord(
            batch_id=str(row[0]),
            candidate_count=int(row[1] or 0),
            sql_change_candidate_count=int(row[2] or 0),
            union_candidate_count=int(row[3] or 0),
            confirmed_change_count=int(row[4] or 0),
            no_op_count=int(row[5] or 0),
            missed_event_count=int(row[6] or 0),
            validation_status=str(row[7] or ""),
            event_routing_validated=_as_bool(row[8]),
            targeted_rows_compared=_as_bool(row[9]),
            whole_table_reference_executed=_as_bool(row[10]),
            query_fingerprint=str(row[11]) if row[11] is not None else None,
            repair_applied=_as_bool(_cell(row, 12)),
        )

    def drop_relation(self, relation: str) -> None:
        self.warehouse.execute(f"drop table if exists {relation}")

    def list_batches(self, project_name: str) -> list[CdcBatch]:
        rows = self.warehouse.execute(
            f"select {BATCH_COLUMNS} from {self.batches} "
            f"where project_name = {sql_string(project_name)} "
            "order by started_at desc, batch_id desc"
        )
        return [self._batch_from_row(row) for row in rows]

    def record_upload(
        self,
        batch_id: str,
        *,
        upload_status: str,
        external_run_id: str | None = None,
        saas_run_id: str | None = None,
        error_code: str | None = None,
    ) -> None:
        uploaded_at = "current_timestamp()" if upload_status == "UPLOADED" else "uploaded_at"
        error_sql = (
            "null"
            if upload_status == "UPLOADED"
            else (sql_string(error_code) if error_code else "last_upload_error_code")
        )
        self.warehouse.execute(
            f"update {self.batches} set "
            f"upload_status = {sql_string(upload_status)}, "
            f"uploaded_at = {uploaded_at}, "
            f"external_run_id = coalesce({sql_string(external_run_id) if external_run_id else 'null'}, external_run_id), "
            f"saas_run_id = coalesce({sql_string(saas_run_id) if saas_run_id else 'null'}, saas_run_id), "
            "upload_attempt_count = coalesce(upload_attempt_count, 0) + 1, "
            f"last_upload_error_code = {error_sql} "
            f"where batch_id = {sql_string(batch_id)}"
        )

    def set_baseline_checkpoint(
        self,
        batch_id: str,
        *,
        relation_fingerprint: str,
        observed_at: str,
        repair_applied: bool,
    ) -> None:
        self.warehouse.execute(
            f"update {self.batches} set "
            f"baseline_relation_fingerprint = {sql_string(relation_fingerprint)}, "
            f"baseline_observed_at = {sql_string(observed_at)}, "
            f"repair_applied = {'true' if repair_applied else 'false'} "
            f"where batch_id = {sql_string(batch_id)}"
        )

