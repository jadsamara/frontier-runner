from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from frontier.config import ConfigError, FrontierConfig
from frontier.dbt_artifacts import Manifest
from frontier.hashing import entity_type_from_key, hmac_entity_id
from frontier.snowflake import Warehouse

REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
TEMPLATE_PATTERN = re.compile(r"\{\{.*?\}\}")


@dataclass(frozen=True)
class ChangeEvent:
    event_id: str
    source_model: str
    operation: str
    entity_key: str
    entity_value: str
    prior_entity_value: str | None = None


@dataclass(frozen=True)
class AffectedEntity:
    entity_type: str
    entity_key: str
    entity_value: str
    reason: str


@dataclass
class FrontierResult:
    full_entity_count: int
    frontier_entity_count: int
    percent_rows_avoided: float
    change_events: list[ChangeEvent]
    affected_entities: list[AffectedEntity]
    frontier_sql: str
    metrics_sql: str


def percent_rows_avoided(full_entity_count: int, frontier_entity_count: int) -> float:
    if full_entity_count <= 0:
        raise ConfigError("full entity count must be positive")
    if frontier_entity_count < 0 or frontier_entity_count > full_entity_count:
        raise ConfigError("frontier count must be between 0 and full entity count")
    return round((1 - frontier_entity_count / full_entity_count) * 100, 3)


def load_change_events_csv(path: Path) -> list[ChangeEvent]:
    if not path.is_file():
        raise ConfigError(f"Missing change events file: {path}")
    events: list[ChangeEvent] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            prior = (
                row.get("prior_customer_id")
                or row.get("prior_entity_value")
                or row.get("priorEntityValue")
                or ""
            ).strip()
            events.append(
                ChangeEvent(
                    event_id=str(row.get("event_id") or row.get("eventId") or "").strip(),
                    source_model=str(row.get("source_model") or row.get("sourceModel") or "").strip(),
                    operation=str(row.get("operation") or "").strip(),
                    entity_key=str(row.get("entity_key") or row.get("entityKey") or "").strip(),
                    entity_value=str(row.get("entity_value") or row.get("entityValue") or "").strip(),
                    prior_entity_value=prior or None,
                )
            )
    if not events:
        raise ConfigError(f"No change events in {path}")
    for event in events:
        if not all([event.event_id, event.source_model, event.operation, event.entity_key, event.entity_value]):
            raise ConfigError(f"Incomplete change event: {event}")
    return events


def _sql_literal(value: str) -> str:
    stripped = value.strip()
    if re.fullmatch(r"-?\d+", stripped):
        return stripped
    escaped = stripped.replace("'", "''")
    return f"'{escaped}'"


def _changed_values_sql(values: list[str]) -> str:
    if not values:
        return "null"
    return ", ".join(_sql_literal(value) for value in values)


def compile_route_sql(
    template: str,
    *,
    manifest: Manifest,
    changed_values: list[str],
) -> str:
    def replace_ref(match: re.Match[str]) -> str:
        node = manifest.find_by_name(match.group(1), resource_types=("model", "seed", "source"))
        return node.relation

    sql = REF_PATTERN.sub(replace_ref, template)
    sql = sql.replace("{{ changed_values }}", _changed_values_sql(changed_values))
    leftover = TEMPLATE_PATTERN.findall(sql)
    if leftover:
        raise ConfigError(f"Unresolved SQL template fragments: {leftover}")
    parsed = sqlglot.parse_one(sql, dialect="snowflake")
    if parsed is None:
        raise ConfigError("Route query could not be parsed")
    if not isinstance(parsed, exp.Select) and parsed.find(exp.Select) is None:
        raise ConfigError("Route queries may only be SELECT statements")
    if parsed.find(exp.Insert) or parsed.find(exp.Update) or parsed.find(exp.Delete) or parsed.find(exp.Merge):
        raise ConfigError("Route queries may only be SELECT statements")
    return sql.strip()


def _direct_reason(config: FrontierConfig) -> str:
    return f"Direct {config.model.entity} key"


def _routed_reason(change_key: str, value: str, entity: str) -> str:
    label = change_key.removesuffix("_id").replace("_", " ").title()
    return f"{label} {value} belongs to {entity}"


def _before_image_reason() -> str:
    return "Before-image supplied customer"


def resolve_affected_entities(
    config: FrontierConfig,
    events: list[ChangeEvent],
    *,
    manifest: Manifest,
    warehouse: Warehouse | None,
) -> tuple[list[AffectedEntity], str]:
    affected: list[AffectedEntity] = []
    sql_parts: list[str] = []
    events_by_source: dict[str, list[ChangeEvent]] = {}
    for event in events:
        events_by_source.setdefault(event.source_model, []).append(event)

    for source_name, source_events in events_by_source.items():
        relation = config.relation(source_name)
        if relation.route.kind == "direct":
            for event in source_events:
                affected.append(
                    AffectedEntity(
                        entity_type=config.model.entity,
                        entity_key=config.model.key,
                        entity_value=event.entity_value,
                        reason=_direct_reason(config),
                    )
                )
            continue

        assert relation.route.query is not None
        if warehouse is None:
            raise ConfigError("A warehouse is required to execute routed frontier queries")
        for event in source_events:
            sql = compile_route_sql(
                relation.route.query,
                manifest=manifest,
                changed_values=[event.entity_value],
            )
            sql_parts.append(sql)
            for row in warehouse.fetch_all(sql):
                if not row or row[0] is None:
                    continue
                affected.append(
                    AffectedEntity(
                        entity_type=config.model.entity,
                        entity_key=config.model.key,
                        entity_value=str(row[0]),
                        reason=_routed_reason(
                            relation.change_key,
                            event.entity_value,
                            config.model.entity,
                        ),
                    )
                )
            if event.prior_entity_value:
                affected.append(
                    AffectedEntity(
                        entity_type=config.model.entity,
                        entity_key=config.model.key,
                        entity_value=event.prior_entity_value,
                        reason=_before_image_reason(),
                    )
                )

    # Before-image on direct events (and any remaining events not handled above)
    for event in events:
        relation = config.relation(event.source_model)
        if relation.route.kind == "direct" and event.prior_entity_value:
            affected.append(
                AffectedEntity(
                    entity_type=config.model.entity,
                    entity_key=config.model.key,
                    entity_value=event.prior_entity_value,
                    reason=_before_image_reason(),
                )
            )

    unique: dict[str, AffectedEntity] = {}
    for entity in affected:
        unique.setdefault(entity.entity_value, entity)
    ordered = sorted(unique.values(), key=lambda item: entity_sort_key(item.entity_value))
    return ordered, "\n;\n".join(sql_parts)


def entity_sort_key(value: str) -> tuple[int, str | int]:
    if re.fullmatch(r"-?\d+", value):
        return (0, int(value))
    return (1, value)


def current_frontier_metrics_sql(manifest: Manifest, model_name: str) -> str:
    model = manifest.find_model(model_name)
    try:
        frontier = manifest.find_model("frontier_affected_customers")
        frontier_rel = frontier.relation
    except ConfigError:
        frontier_rel = None

    full = model.relation
    if frontier_rel:
        sql = (
            "select\n"
            f"    (select count(*) from {full}) as full_entity_count,\n"
            f"    (select count(*) from {frontier_rel}) as frontier_entity_count"
        )
    else:
        sql = f"select count(*) as full_entity_count from {full}"
    sqlglot.parse_one(sql, dialect="snowflake")
    return sql


def run_frontier(
    config: FrontierConfig,
    *,
    manifest: Manifest,
    events: list[ChangeEvent],
    warehouse: Warehouse,
) -> FrontierResult:
    affected, frontier_sql = resolve_affected_entities(
        config,
        events,
        manifest=manifest,
        warehouse=warehouse,
    )
    metrics_sql = current_frontier_metrics_sql(manifest, config.model.name)
    metric_rows = warehouse.fetch_all(metrics_sql)
    if not metric_rows:
        raise ConfigError("Frontier metrics query returned no rows")
    row = metric_rows[0]
    full_entity_count = int(row[0])
    if len(row) > 1 and row[1] is not None:
        frontier_entity_count = int(row[1])
    else:
        frontier_entity_count = len(affected)
    percent = percent_rows_avoided(full_entity_count, frontier_entity_count)
    return FrontierResult(
        full_entity_count=full_entity_count,
        frontier_entity_count=frontier_entity_count,
        percent_rows_avoided=percent,
        change_events=events,
        affected_entities=affected,
        frontier_sql=frontier_sql,
        metrics_sql=metrics_sql,
    )


def hash_entity_id(
    value: str,
    *,
    secret: str,
    project: str,
    entity_type: str,
    entity_key: str,
) -> str:
    return hmac_entity_id(
        secret,
        project=project,
        entity_type=entity_type,
        entity_key=entity_key,
        value=value,
    )


def _pseudonymize(
    value: str,
    *,
    secret: str | None,
    include_entity_ids: bool,
    project: str,
    entity_type: str,
    entity_key: str,
) -> str:
    if include_entity_ids:
        return value
    if not secret:
        raise ConfigError(
            "FRONTIER_ENTITY_HASH_KEY is required unless --include-entity-ids is set"
        )
    return hmac_entity_id(
        secret,
        project=project,
        entity_type=entity_type,
        entity_key=entity_key,
        value=value,
    )


def _upload_reason(reason: str, *, include_entity_ids: bool) -> str:
    if include_entity_ids:
        return reason
    if reason.startswith("Order ") and " belongs to " in reason:
        return "Order belongs to customer"
    return reason


def frontier_result_to_dict(
    result: FrontierResult,
    *,
    config: FrontierConfig,
    include_entity_ids: bool,
    hash_key: str | None,
) -> dict[str, Any]:
    if not include_entity_ids and not hash_key:
        raise ConfigError(
            "FRONTIER_ENTITY_HASH_KEY is required unless --include-entity-ids is set"
        )

    def transform(*, value: str, entity_type: str, entity_key: str) -> str:
        return _pseudonymize(
            value,
            secret=hash_key,
            include_entity_ids=include_entity_ids,
            project=config.project,
            entity_type=entity_type,
            entity_key=entity_key,
        )

    change_events = []
    for event in result.change_events:
        event_type = entity_type_from_key(event.entity_key, config.model)
        payload_event = {
            "eventId": event.event_id,
            "sourceModel": event.source_model,
            "operation": event.operation,
            "entityKey": event.entity_key,
            "entityValue": transform(
                value=event.entity_value,
                entity_type=event_type,
                entity_key=event.entity_key,
            ),
        }
        if event.prior_entity_value:
            payload_event["priorEntityValue"] = transform(
                value=event.prior_entity_value,
                entity_type=config.model.entity,
                entity_key=config.model.key,
            )
        change_events.append(payload_event)

    return {
        "metrics": {
            "fullEntityCount": result.full_entity_count,
            "frontierEntityCount": result.frontier_entity_count,
            "percentRowsAvoided": result.percent_rows_avoided,
        },
        "changeEvents": change_events,
        "affectedEntities": [
            {
                "entityType": entity.entity_type,
                "entityKey": entity.entity_key,
                "entityValue": transform(
                    value=entity.entity_value,
                    entity_type=entity.entity_type,
                    entity_key=entity.entity_key,
                ),
                "reason": _upload_reason(entity.reason, include_entity_ids=include_entity_ids),
            }
            for entity in result.affected_entities
        ],
        "frontierSql": result.frontier_sql,
        "metricsSql": result.metrics_sql,
    }
