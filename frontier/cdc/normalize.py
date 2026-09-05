from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from typing import Any, Mapping

from frontier.cdc.config import CdcSourceConfig
from frontier.config import ConfigError

METADATA_ACTION = "METADATA$ACTION"
METADATA_ISUPDATE = "METADATA$ISUPDATE"
METADATA_ROW_ID = "METADATA$ROW_ID"


@dataclass(frozen=True)
class StreamRecord:
    action: str
    is_update: bool
    row_id: str
    primary_key: str | None
    target_key: str | None


@dataclass(frozen=True)
class LogicalChangeEvent:
    event_id: str
    source_model: str
    source_relation: str
    provider: str
    operation: str
    provider_row_id: str
    is_update: bool
    primary_key_before: str | None
    primary_key_after: str | None
    target_key_before: str | None
    target_key_after: str | None
    payload_fingerprint: str


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "t", "yes"}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _fingerprint(*parts: str) -> str:
    payload = "|".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def records_from_rows(
    rows: list[Mapping[str, Any]] | list[tuple[Any, ...]],
) -> list[StreamRecord]:
    records: list[StreamRecord] = []
    for row in rows:
        if isinstance(row, Mapping):
            action = str(row.get("action") or row.get(METADATA_ACTION) or "").upper()
            is_update = _truthy(row.get("is_update", row.get(METADATA_ISUPDATE)))
            row_id = str(row.get("row_id") or row.get(METADATA_ROW_ID) or "").strip()
            primary_key = _text(row.get("primary_key"))
            target_key = _text(row.get("target_key"))
        else:
            action = str(row[0] or "").upper()
            is_update = _truthy(row[1])
            row_id = str(row[2] or "").strip()
            primary_key = _text(row[3] if len(row) > 3 else None)
            target_key = _text(row[4] if len(row) > 4 else None)
        if not row_id:
            raise ConfigError("Snowflake stream row is missing METADATA$ROW_ID")
        if action not in {"INSERT", "DELETE"}:
            raise ConfigError("unsupported Snowflake stream action")
        records.append(
            StreamRecord(
                action=action,
                is_update=is_update,
                row_id=row_id,
                primary_key=primary_key,
                target_key=target_key,
            )
        )
    return records


def normalize_stream_records(
    records: list[StreamRecord],
    *,
    source: CdcSourceConfig,
    provider: str,
) -> list[LogicalChangeEvent]:
    grouped: dict[str, list[StreamRecord]] = {}
    for record in records:
        grouped.setdefault(record.row_id, []).append(record)

    events: list[LogicalChangeEvent] = []
    for row_id, group in grouped.items():
        inserts = [item for item in group if item.action == "INSERT"]
        deletes = [item for item in group if item.action == "DELETE"]
        is_update = any(item.is_update for item in group)
        if is_update:
            if len(inserts) != 1 or len(deletes) != 1 or len(group) != 2:
                raise ConfigError("unpaired or duplicate Snowflake update record")
            event = _logical_event(
                source=source,
                provider=provider,
                operation="UPDATE",
                row_id=row_id,
                is_update=True,
                before=deletes[0],
                after=inserts[0],
            )
        elif len(inserts) == 1 and not deletes:
            if inserts[0].is_update:
                raise ConfigError("unpaired or duplicate Snowflake update record")
            event = _logical_event(
                source=source,
                provider=provider,
                operation="INSERT",
                row_id=row_id,
                is_update=False,
                before=None,
                after=inserts[0],
            )
        elif len(deletes) == 1 and not inserts:
            if deletes[0].is_update:
                raise ConfigError("unpaired or duplicate Snowflake update record")
            event = _logical_event(
                source=source,
                provider=provider,
                operation="DELETE",
                row_id=row_id,
                is_update=False,
                before=deletes[0],
                after=None,
            )
        else:
            raise ConfigError("unsupported Snowflake stream shape")
        _validate_before_image(event, source)
        events.append(event)

    fingerprints = [event.payload_fingerprint for event in events]
    if len(fingerprints) != len(set(fingerprints)):
        raise ConfigError("duplicate CDC event fingerprint")
    return events


def _logical_event(
    *,
    source: CdcSourceConfig,
    provider: str,
    operation: str,
    row_id: str,
    is_update: bool,
    before: StreamRecord | None,
    after: StreamRecord | None,
) -> LogicalChangeEvent:
    pk_before = before.primary_key if before else None
    pk_after = after.primary_key if after else None
    tk_before = before.target_key if before else None
    tk_after = after.target_key if after else None
    fingerprint = _fingerprint(
        source.stream_relation,
        row_id,
        operation,
        pk_before or "",
        pk_after or "",
        tk_before or "",
        tk_after or "",
    )
    return LogicalChangeEvent(
        event_id=fingerprint[:32],
        source_model=source.source_model,
        source_relation=source.base_relation,
        provider=provider,
        operation=operation,
        provider_row_id=row_id,
        is_update=is_update,
        primary_key_before=pk_before,
        primary_key_after=pk_after,
        target_key_before=tk_before,
        target_key_after=tk_after,
        payload_fingerprint=fingerprint,
    )


def _validate_before_image(event: LogicalChangeEvent, source: CdcSourceConfig) -> None:
    required = set(source.require_before_image_for)
    if event.operation == "DELETE" and "DELETE" in required and not event.target_key_before:
        raise ConfigError("delete is missing the required before target key")
    key_changed = (
        event.operation == "UPDATE"
        and event.target_key_before != event.target_key_after
    )
    if key_changed and "KEY_CHANGE" in required:
        if not event.target_key_before or not event.target_key_after:
            raise ConfigError("key change is missing a required before or after target key")


def operation_counts(events: list[LogicalChangeEvent]) -> dict[str, int]:
    counts = Counter(event.operation.lower() + "s" for event in events)
    return {
        "inserts": counts.get("inserts", 0),
        "updates": counts.get("updates", 0),
        "deletes": counts.get("deletes", 0),
    }


def batch_fingerprint(events: list[LogicalChangeEvent]) -> str:
    joined = ",".join(sorted(event.payload_fingerprint for event in events))
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
