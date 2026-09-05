from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from frontier.cdc.events import CanonicalChangeEvent, make_event
from frontier.config import ConfigError

METADATA_ACTION = "METADATA$ACTION"
METADATA_ISUPDATE = "METADATA$ISUPDATE"
METADATA_ROW_ID = "METADATA$ROW_ID"


@dataclass(frozen=True)
class StreamChange:
    operation: str
    row_id: str
    before: dict[str, Any]
    after: dict[str, Any]
    is_update: bool


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"true", "1", "t", "yes"}


def _row_data(row: Mapping[str, Any]) -> dict[str, Any]:
    skip = {METADATA_ACTION, METADATA_ISUPDATE, METADATA_ROW_ID, "metadata$action", "metadata$isupdate", "metadata$row_id"}
    data: dict[str, Any] = {}
    for key, value in row.items():
        if key.upper() in {METADATA_ACTION, METADATA_ISUPDATE, METADATA_ROW_ID}:
            continue
        if key.lower() in skip:
            continue
        data[key] = value
    return data


def _meta(row: Mapping[str, Any], name: str) -> Any:
    if name in row:
        return row[name]
    for key, value in row.items():
        if key.upper() == name:
            return value
    return None


def pair_stream_rows(rows: list[Mapping[str, Any]]) -> list[StreamChange]:
    """Pair Snowflake stream INSERT/DELETE rows into insert, delete, and update."""
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        row_id = str(_meta(row, METADATA_ROW_ID) or "")
        if not row_id:
            raise ConfigError("Snowflake stream row is missing METADATA$ROW_ID")
        grouped.setdefault(row_id, []).append(row)

    changes: list[StreamChange] = []
    for row_id, group in grouped.items():
        inserts = [item for item in group if str(_meta(item, METADATA_ACTION) or "").upper() == "INSERT"]
        deletes = [item for item in group if str(_meta(item, METADATA_ACTION) or "").upper() == "DELETE"]
        if not inserts and not deletes:
            raise ConfigError(f"unsupported Snowflake stream shape for row {row_id}")
        is_update = any(_truthy(_meta(item, METADATA_ISUPDATE)) for item in group)
        if is_update:
            if len(inserts) != 1 or len(deletes) != 1:
                raise ConfigError(f"update before/after pairing failed for row {row_id}")
            changes.append(
                StreamChange(
                    operation="UPDATE",
                    row_id=row_id,
                    before=_row_data(deletes[0]),
                    after=_row_data(inserts[0]),
                    is_update=True,
                )
            )
            continue
        if len(inserts) == 1 and not deletes:
            changes.append(
                StreamChange(
                    operation="INSERT",
                    row_id=row_id,
                    before={},
                    after=_row_data(inserts[0]),
                    is_update=False,
                )
            )
            continue
        if len(deletes) == 1 and not inserts:
            changes.append(
                StreamChange(
                    operation="DELETE",
                    row_id=row_id,
                    before=_row_data(deletes[0]),
                    after={},
                    is_update=False,
                )
            )
            continue
        raise ConfigError(f"unsupported Snowflake stream shape for row {row_id}")
    return changes


def _changed_columns(before: Mapping[str, Any], after: Mapping[str, Any]) -> tuple[str, ...]:
    names = sorted(set(before) | set(after))
    return tuple(
        name
        for name in names
        if str(before.get(name)) != str(after.get(name))
    )


def stream_changes_to_canonical(
    changes: list[StreamChange],
    *,
    project_id: str,
    source_relation: str,
    key_columns: tuple[str, ...],
    entity_key: str | None = None,
) -> list[CanonicalChangeEvent]:
    events: list[CanonicalChangeEvent] = []
    for change in changes:
        before_keys = {column: change.before[column] for column in key_columns if column in change.before}
        after_keys = {column: change.after[column] for column in key_columns if column in change.after}
        before_entity = ()
        after_entity = ()
        if entity_key:
            if entity_key in change.before and change.before[entity_key] is not None:
                before_entity = (str(change.before[entity_key]),)
            if entity_key in change.after and change.after[entity_key] is not None:
                after_entity = (str(change.after[entity_key]),)
        events.append(
            make_event(
                project_id=project_id,
                source_relation=source_relation,
                operation=change.operation,
                provider="snowflake_streams",
                source_offset=change.row_id,
                event_time=None,
                changed_columns=_changed_columns(change.before, change.after),
                before_keys=before_keys,
                after_keys=after_keys,
                before_entity_values=before_entity,
                after_entity_values=after_entity,
                provider_metadata={"row_id": change.row_id, "is_update": str(change.is_update).lower()},
                event_id=change.row_id,
            )
        )
    return events
