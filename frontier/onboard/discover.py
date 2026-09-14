from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from frontier.dbt_artifacts import DbtNode, Manifest

_SKIP_PREFIXES = ("stg_", "int_", "base_", "frontier_", "mutation_")
_SKIP_SUFFIXES = ("_after", "_mutated", "_repaired", "_target")
_SKIP_TAGS = {"frontier_demo", "frontier_mutation"}
_ID_COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*_id$")


@dataclass(frozen=True)
class SourceSuggestion:
    name: str
    change_key: str
    join_route: str
    mutation_policy: str
    deletes_require_before_image: bool
    temporal_mode: str
    event_time_column: str | None
    maximum_lateness: str | None
    confidence: str
    origin: str
    route_status: str = "UNRESOLVED"
    route_path: tuple[Any, ...] = ()
    evidence: tuple[str, ...] = ()
    sql_change_blocker: bool = False
    cdc_blocker: bool = True


@dataclass(frozen=True)
class ModelSuggestion:
    model: str
    entity: str
    entity_key: str
    grain: str
    sources: tuple[SourceSuggestion, ...]
    confidence: str
    reasons: tuple[str, ...]
    target_unique_id: str = ""
    discarded_overrides: tuple[SourceSuggestion, ...] = ()

    def to_document(self) -> dict[str, Any]:
        return self.to_semantic_document()

    def to_semantic_document(self) -> dict[str, Any]:
        sources = []
        for source in self.sources:
            item: dict[str, Any] = {
                "name": source.name,
                "changeKey": source.change_key if source.change_key else "unresolved",
                "joinRoute": source.join_route,
                "mutationPolicy": source.mutation_policy,
                "deletesRequireBeforeImage": source.deletes_require_before_image,
                "temporalMode": source.temporal_mode,
                "eventTimeColumn": source.event_time_column,
                "maximumLateness": source.maximum_lateness,
                "confidence": source.confidence,
                "origin": source.origin,
                "routeStatus": source.route_status,
                "evidence": list(source.evidence),
                "sqlChangeBlocker": source.sql_change_blocker,
                "cdcBlocker": source.cdc_blocker,
            }
            path = [
                {"model": hop.model, "column": hop.column}
                for hop in source.route_path
            ]
            if path:
                item["routePath"] = path
            sources.append(item)
        return {
            "model": self.model,
            "entity": self.entity,
            "entityKey": self.entity_key,
            "grain": self.grain,
            "generationKind": "generated",
            "sources": sources,
        }


def _is_candidate_mart(node: DbtNode) -> bool:
    name = node.name.lower()
    if node.resource_type != "model":
        return False
    if any(tag in _SKIP_TAGS for tag in node.tags):
        return False
    if any(name.startswith(prefix) for prefix in _SKIP_PREFIXES):
        return False
    if any(name.endswith(suffix) for suffix in _SKIP_SUFFIXES):
        return False
    path = (node.original_file_path or "").replace("\\", "/")
    if "/staging/" in path or "/intermediate/" in path:
        return False
    return True


def _columns(node: DbtNode) -> tuple[str, ...]:
    return getattr(node, "columns", ()) or ()


def _unique_columns(manifest: Manifest, node: DbtNode) -> set[str]:
    found: set[str] = set()
    for test in manifest.tests_for(node.unique_id):
        name = test.name.lower()
        if "unique" not in name:
            continue
        for column in _columns(node):
            if column.lower() in name:
                found.add(column)
    return found


def _entity_from_key(key: str, model_name: str) -> str:
    if key.endswith("_id") and len(key) > 3:
        return key[: -len("_id")]
    if model_name.endswith("_summary"):
        return model_name[: -len("_summary")]
    return model_name


def _suggest_key(manifest: Manifest, node: DbtNode) -> tuple[str, str]:
    columns = list(_columns(node))
    unique = _unique_columns(manifest, node)
    for column in columns:
        if column in unique and _ID_COLUMN.match(column):
            return column, "high"
    for column in columns:
        if _ID_COLUMN.match(column):
            return column, "medium"
    if columns:
        return columns[0], "low"
    return "id", "low"


def _source_models(manifest: Manifest, mart: DbtNode) -> list[DbtNode]:
    upstream = manifest.upstream_models(mart.unique_id)
    staging = [
        node
        for node in upstream
        if node.name.startswith("stg_") or "/staging/" in (node.original_file_path or "")
    ]
    return staging or [node for node in upstream if node.resource_type == "model"][:4]


def suggest_models(manifest: Manifest) -> list[ModelSuggestion]:
    from frontier.onboard.routes import suggest_generated_models

    return suggest_generated_models(manifest)
