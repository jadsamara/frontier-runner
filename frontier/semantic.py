from __future__ import annotations

import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urljoin

from frontier.config import (
    ConfigError,
    FrontierConfig,
    ModelConfig,
    RelationConfig,
    Route,
    derive_proof_config,
)
from frontier.dbt_artifacts import Manifest
from frontier.progress import elapsed_ms, log_step

PIN_FILE_NAME = "frontier-manifest.json"
JOIN_ROUTE_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\s*->\s*[A-Za-z_][A-Za-z0-9_]*$",
)
SOURCES = ("saas_active", "pinned_file", "local_override")


class ManifestError(ConfigError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


@dataclass(frozen=True)
class SemanticSource:
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


@dataclass(frozen=True)
class PinnedSemanticManifest:
    id: str
    version: int
    fingerprint: str
    project: str
    status: str
    activated_at: str | None
    target_model_unique_id: str
    target_model: str
    target_entity_type: str
    entity_key: str
    grain: str
    sources: tuple[SemanticSource, ...]
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "version": self.version,
            "fingerprint": self.fingerprint,
            "project": self.project,
            "status": self.status,
            "activatedAt": self.activated_at,
            "source": self.source,
            "targetModelUniqueId": self.target_model_unique_id,
            "targetModel": self.target_model,
            "targetEntityType": self.target_entity_type,
            "entityKey": self.entity_key,
            "grain": self.grain,
            "sources": [
                {
                    "name": item.name,
                    "changeKey": item.change_key,
                    "joinRoute": item.join_route,
                    "mutationPolicy": item.mutation_policy,
                    "deletesRequireBeforeImage": item.deletes_require_before_image,
                    "temporalMode": item.temporal_mode,
                    "eventTimeColumn": item.event_time_column,
                    "maximumLateness": item.maximum_lateness,
                    "confidence": item.confidence,
                    "origin": item.origin,
                }
                for item in self.sources
            ],
        }


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def fingerprint_document(document: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(document)).encode("utf-8")).hexdigest()


def fingerprint_prefix(fingerprint: str) -> str:
    hex_digest = fingerprint.strip().lower()
    if len(hex_digest) < 12:
        return "—"
    return f"{hex_digest[:12]}…"


def default_pin_path(project_dir: Path) -> Path:
    return project_dir / "target" / PIN_FILE_NAME


def api_credentials_configured() -> bool:
    for name in ("FRONTIER_API_KEY", "FRONTIER_DEMO_API_KEY"):
        value = (os.environ.get(name) or "").strip()
        if value:
            return True
    return False


def allow_local_manifest(args: Any) -> bool:
    if bool(getattr(args, "allow_local_manifest", False)):
        return True
    if (os.environ.get("GITHUB_ACTIONS") or "").strip():
        return False
    return (os.environ.get("FRONTIER_ALLOW_LOCAL_MANIFEST") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _source_from_mapping(raw: Mapping[str, Any]) -> SemanticSource:
    event_time = raw.get("eventTimeColumn")
    lateness = raw.get("maximumLateness")
    return SemanticSource(
        name=str(raw.get("name") or "").strip(),
        change_key=str(raw.get("changeKey") or "").strip(),
        join_route=str(raw.get("joinRoute") or "").strip(),
        mutation_policy=str(raw.get("mutationPolicy") or "").strip(),
        deletes_require_before_image=bool(raw.get("deletesRequireBeforeImage")),
        temporal_mode=str(raw.get("temporalMode") or "none").strip() or "none",
        event_time_column=None if event_time in (None, "") else str(event_time).strip(),
        maximum_lateness=None if lateness in (None, "") else str(lateness).strip(),
        confidence=str(raw.get("confidence") or "").strip(),
        origin=str(raw.get("origin") or "").strip(),
    )


def document_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    if "document" in payload and isinstance(payload["document"], Mapping):
        document = dict(payload["document"])
    else:
        document = {
            "model": payload.get("targetModel") or payload.get("model"),
            "entity": payload.get("targetEntityType") or payload.get("entity"),
            "entityKey": payload.get("entityKey"),
            "grain": payload.get("grain"),
            "sources": payload.get("sources"),
        }
    return document


def pinned_from_payload(payload: Mapping[str, Any], *, source: str) -> PinnedSemanticManifest:
    document = document_from_payload(payload)
    sources_raw = document.get("sources")
    if not isinstance(sources_raw, list) or not sources_raw:
        raise ManifestError("MANIFEST_REQUIRED", "Semantic manifest sources are missing")
    sources = tuple(_source_from_mapping(item) for item in sources_raw if isinstance(item, Mapping))
    fingerprint = str(payload.get("fingerprint") or "").strip() or fingerprint_document(document)
    recomputed = fingerprint_document(document)
    if fingerprint != recomputed:
        raise ManifestError(
            "MANIFEST_FINGERPRINT_MISMATCH",
            "Pinned semantic manifest fingerprint does not match the document",
        )
    project = str(payload.get("project") or "").strip()
    model = str(document.get("model") or "").strip()
    unique_id = str(payload.get("targetModelUniqueId") or "").strip()
    if not unique_id:
        unique_id = f"model.{project}.{model}" if project and model else model
    try:
        version = int(payload.get("version"))
    except (TypeError, ValueError) as error:
        raise ManifestError("MANIFEST_VERSION_MISMATCH", "Semantic manifest version is invalid") from error
    return PinnedSemanticManifest(
        id=str(payload.get("id") or "").strip(),
        version=version,
        fingerprint=fingerprint,
        project=project,
        status=str(payload.get("status") or "active").strip() or "active",
        activated_at=str(payload.get("activatedAt") or "").strip() or None,
        target_model_unique_id=unique_id,
        target_model=model,
        target_entity_type=str(document.get("entity") or "").strip(),
        entity_key=str(document.get("entityKey") or "").strip(),
        grain=str(document.get("grain") or "").strip(),
        sources=sources,
        source=source,
    )


def join_route_to_route(source_name: str, source: SemanticSource, entity_key: str) -> Route:
    route = source.join_route.strip()
    if route == "direct":
        return Route(kind="direct")
    if JOIN_ROUTE_PATTERN.fullmatch(route):
        query = (
            f"select {entity_key}\n"
            f"from {{{{ ref('{source_name}') }}}}\n"
            f"where {source.change_key} in ({{{{ changed_values }}}})"
        )
        return Route(kind="query", query=query)
    if "{{ changed_values }}" in route:
        return Route(kind="query", query=route)
    raise ManifestError(
        "MANIFEST_LOCAL_REMOTE_CONFLICT",
        f"Join route for '{source_name}' is not a supported mapping",
    )


def apply_pinned_manifest(config: FrontierConfig, pinned: PinnedSemanticManifest) -> FrontierConfig:
    if pinned.project and pinned.project != config.project:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "frontier.yml project does not match the semantic manifest project",
        )
    model = ModelConfig(
        name=pinned.target_model,
        entity=pinned.target_entity_type,
        key=pinned.entity_key,
        grain=pinned.grain,
    )
    relations = {
        item.name: RelationConfig(
            name=item.name,
            change_key=item.change_key,
            route=join_route_to_route(item.name, item, pinned.entity_key),
        )
        for item in pinned.sources
    }
    local_names = set(config.relations)
    remote_names = set(relations)
    if local_names and local_names != remote_names:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "Local relation names do not match the semantic manifest sources",
        )
    if config.model.name != pinned.target_model:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "Local model name does not match the semantic manifest",
        )
    if config.model.entity != pinned.target_entity_type:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "Local entity type does not match the semantic manifest",
        )
    if config.model.key != pinned.entity_key:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "Local entity key does not match the semantic manifest",
        )
    if config.model.grain != pinned.grain:
        raise ManifestError(
            "MANIFEST_LOCAL_REMOTE_CONFLICT",
            "Local grain does not match the semantic manifest",
        )
    for name, relation in config.relations.items():
        remote = relations[name]
        if relation.change_key != remote.change_key:
            raise ManifestError(
                "MANIFEST_LOCAL_REMOTE_CONFLICT",
                f"Local change key for '{name}' does not match the semantic manifest",
            )
        if relation.route.kind == "direct" and remote.route.kind != "direct":
            raise ManifestError(
                "MANIFEST_LOCAL_REMOTE_CONFLICT",
                f"Local route for '{name}' does not match the semantic manifest",
            )
    proof = derive_proof_config(model)
    return replace(config, model=model, relations=relations, proof=proof, pinned=pinned)


def validate_pinned_document(pinned: PinnedSemanticManifest) -> None:
    if pinned.status == "draft":
        raise ManifestError("MANIFEST_IS_DRAFT", "Draft semantic manifests cannot run autonomously")
    if pinned.status not in {"active", "retired"}:
        raise ManifestError("MANIFEST_NOT_ACTIVE", "Semantic manifest is not active")
    names: set[str] = set()
    for source in pinned.sources:
        if source.name in names:
            raise ManifestError(
                "MANIFEST_LOCAL_REMOTE_CONFLICT",
                f"Duplicate source '{source.name}'",
            )
        names.add(source.name)
        if source.confidence == "low":
            raise ManifestError(
                "MANIFEST_LOW_CONFIDENCE",
                f"Low-confidence mapping for '{source.name}' cannot run autonomously",
            )
        if source.origin == "inferred":
            raise ManifestError(
                "MANIFEST_ROUTE_UNCONFIRMED",
                f"Inferred mapping for '{source.name}' must be confirmed before autonomous runs",
            )
        if source.temporal_mode == "event_time" and not (source.event_time_column or "").strip():
            raise ManifestError(
                "MANIFEST_TEMPORAL_INCOMPLETE",
                f"Event-time column is required for '{source.name}'",
            )
        route = source.join_route.strip()
        if route != "direct" and not JOIN_ROUTE_PATTERN.fullmatch(route) and "{{ changed_values }}" not in route:
            raise ManifestError(
                "MANIFEST_LOCAL_REMOTE_CONFLICT",
                f"Join route for '{source.name}' is invalid",
            )


def _sql_has_identifier(sql: str, name: str) -> bool:
    return re.search(rf"(?i)(^|[^A-Za-z0-9_]){re.escape(name)}([^A-Za-z0-9_]|$)", sql) is not None


def validate_pinned_against_dbt(pinned: PinnedSemanticManifest, manifest: Manifest) -> None:
    try:
        model = manifest.find_model(pinned.target_model)
    except ConfigError as error:
        raise ManifestError(
            "MANIFEST_TARGET_MODEL_MISSING",
            f"Target model '{pinned.target_model}' is not in the dbt manifest",
        ) from error
    sql = model.compiled_code or ""
    if sql and not _sql_has_identifier(sql, pinned.entity_key):
        raise ManifestError(
            "MANIFEST_ENTITY_KEY_ABSENT",
            "Entity key is absent from compiled target SQL",
        )
    lowered = sql.lower()
    if "group by" in lowered and not _sql_has_identifier(
        lowered.split("group by", 1)[1],
        pinned.entity_key,
    ):
        raise ManifestError(
            "MANIFEST_GRAIN_INCOMPATIBLE",
            "Grain is incompatible with the compiled target model",
        )
    missing = [
        source.name
        for source in pinned.sources
        if all(
            node.name != source.name
            for node in manifest.nodes.values()
            if node.resource_type == "model"
        )
    ]
    if missing:
        raise ManifestError(
            "MANIFEST_TARGET_MODEL_MISSING",
            f"Configured relations not in the dbt manifest: {', '.join(missing)}",
        )


def pin_manifest(path: Path, pinned: PinnedSemanticManifest) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(pinned.to_dict(), indent=2) + "\n")
    return path


def load_pinned_file(path: Path) -> PinnedSemanticManifest:
    if not path.is_file():
        raise ManifestError("MANIFEST_REQUIRED", f"Missing pinned semantic manifest {path}")
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as error:
        raise ManifestError("MANIFEST_REQUIRED", "Pinned semantic manifest is not valid JSON") from error
    if not isinstance(payload, Mapping):
        raise ManifestError("MANIFEST_REQUIRED", "Pinned semantic manifest must be an object")
    return pinned_from_payload(payload, source="pinned_file")


def fetch_active_manifest(
    *,
    api_url: str,
    api_key: str,
    project: str,
    timeout_seconds: int = 30,
) -> PinnedSemanticManifest:
    url = urljoin(api_url.rstrip("/") + "/", f"api/v1/projects/{quote(project, safe='')}/manifests/active")
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8")
            payload = json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        code = "MANIFEST_SAAS_UNAVAILABLE"
        if error.code in {401, 403}:
            code = "MANIFEST_API_KEY_REVOKED"
        elif error.code == 404:
            try:
                body = json.loads(detail) if detail else {}
            except json.JSONDecodeError:
                body = {}
            code = str(body.get("code") or "MANIFEST_NOT_FOUND")
            if code not in {
                "MANIFEST_NOT_ACTIVE",
                "MANIFEST_NOT_FOUND",
                "MANIFEST_IS_DRAFT",
            }:
                code = "MANIFEST_NOT_FOUND"
        raise ManifestError(code, f"Active manifest fetch failed with HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise ManifestError("MANIFEST_SAAS_UNAVAILABLE", "Frontier SaaS is unavailable") from error
    except json.JSONDecodeError as error:
        raise ManifestError("MANIFEST_SAAS_UNAVAILABLE", "Active manifest response was not JSON") from error
    if not isinstance(payload, Mapping):
        raise ManifestError("MANIFEST_SAAS_UNAVAILABLE", "Active manifest response was not an object")
    return pinned_from_payload(payload, source="saas_active")


def print_semantic_manifest(pinned: PinnedSemanticManifest) -> None:
    print("Semantic manifest:", flush=True)
    print(f"- source: {pinned.source}", flush=True)
    print(f"- version: {pinned.version}", flush=True)
    print(f"- fingerprint: {fingerprint_prefix(pinned.fingerprint)}", flush=True)
    print(f"- target model: {pinned.target_model}", flush=True)
    print(f"- entity: {pinned.target_entity_type}", flush=True)
    print(f"- grain: {pinned.grain}", flush=True)
    print(f"- route count: {len(pinned.sources)}", flush=True)


def local_override_from_config(config: FrontierConfig) -> PinnedSemanticManifest:
    document = {
        "model": config.model.name,
        "entity": config.model.entity,
        "entityKey": config.model.key,
        "grain": config.model.grain,
        "sources": [
            {
                "name": relation.name,
                "changeKey": relation.change_key,
                "joinRoute": (
                    "direct"
                    if relation.route.kind == "direct"
                    else (relation.route.query or "direct")
                ),
                "mutationPolicy": "targeted_repair",
                "deletesRequireBeforeImage": False,
                "temporalMode": "none",
                "eventTimeColumn": None,
                "maximumLateness": None,
                "confidence": "high",
                "origin": "confirmed",
            }
            for relation in config.relations.values()
        ],
    }
    return PinnedSemanticManifest(
        id="00000000-0000-4000-8000-000000000000",
        version=1,
        fingerprint=fingerprint_document(document),
        project=config.project,
        status="active",
        activated_at=None,
        target_model_unique_id=f"model.{config.project}.{config.model.name}",
        target_model=config.model.name,
        target_entity_type=config.model.entity,
        entity_key=config.model.key,
        grain=config.model.grain,
        sources=tuple(_source_from_mapping(item) for item in document["sources"]),
        source="local_override",
    )


def resolve_semantic_manifest(
    args: Any,
    *,
    project_dir: Path,
    config: FrontierConfig,
    dbt_manifest: Manifest | None,
    api_url: str,
    api_key: str | None,
) -> tuple[FrontierConfig, PinnedSemanticManifest]:
    explicit = getattr(args, "manifest_file", None)
    pin_path = Path(explicit).expanduser().resolve() if explicit else default_pin_path(project_dir)
    started = time.perf_counter()
    log_step("manifest fetch started", prefix="manifest")
    try:
        if explicit:
            pinned = load_pinned_file(pin_path)
        elif api_key:
            pinned = fetch_active_manifest(api_url=api_url, api_key=api_key, project=config.project)
        elif allow_local_manifest(args):
            pinned = local_override_from_config(config)
        else:
            raise ManifestError(
                "MANIFEST_REQUIRED",
                "Active SaaS manifest or --manifest-file is required (pass --allow-local-manifest for local development)",
            )
    except ManifestError:
        log_step(
            "manifest fetch completed",
            prefix="manifest",
            duration_ms=elapsed_ms(started),
            status="failed",
        )
        raise
    log_step(
        "manifest fetch completed",
        prefix="manifest",
        duration_ms=elapsed_ms(started),
        status="ok",
    )

    started = time.perf_counter()
    log_step("manifest validation started", prefix="manifest")
    try:
        validate_pinned_document(pinned)
        if dbt_manifest is not None:
            validate_pinned_against_dbt(pinned, dbt_manifest)
        applied = apply_pinned_manifest(config, pinned)
    except ManifestError:
        log_step(
            "manifest validation completed",
            prefix="manifest",
            duration_ms=elapsed_ms(started),
            status="failed",
        )
        raise
    log_step(
        "manifest validation completed",
        prefix="manifest",
        duration_ms=elapsed_ms(started),
        status="ok",
    )

    started = time.perf_counter()
    log_step("manifest pinning started", prefix="manifest")
    try:
        if pinned.source != "local_override":
            pin_manifest(default_pin_path(project_dir) if not explicit else pin_path, pinned)
        elif explicit:
            pin_manifest(pin_path, pinned)
    except OSError as error:
        log_step(
            "manifest pinning completed",
            prefix="manifest",
            duration_ms=elapsed_ms(started),
            status="failed",
        )
        raise ManifestError("MANIFEST_REQUIRED", "Could not pin the semantic manifest locally") from error
    log_step(
        "manifest pinning completed",
        prefix="manifest",
        duration_ms=elapsed_ms(started),
        status="ok",
    )
    print_semantic_manifest(applied.pinned if isinstance(applied.pinned, PinnedSemanticManifest) else pinned)
    return applied, pinned
