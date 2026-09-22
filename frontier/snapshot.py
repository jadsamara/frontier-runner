"""Warehouse source-snapshot capture, binding, and verification.

The filter-v1 theorem is relative to one fixed database instance. Live
assessments are certified only when every source read is bound to one
logical snapshot. This module is the adapter-facing contract for that
premise. The static compiler must consume snapshot evidence produced
here; it must never synthesize it.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.config import ConfigError
from frontier.sql_fingerprint import render_executable_sql
from frontier.warehouse import split_relation_parts, sql_string

RULE_SET_VERSION = "filter-v1"

MODE_TIME_TRAVEL = "snowflake_time_travel"
MODE_CLONE = "snowflake_clone"
MODE_EXTERNAL = "externally_attested"
MODE_NONE = "none"

ASSURANCE_ADAPTER = "ADAPTER_VERIFIED"
ASSURANCE_EXTERNAL = "EXTERNALLY_ATTESTED"
ASSURANCE_NONE = "NONE"

SOURCE_SNAPSHOT_NOT_PINNED = "SOURCE_SNAPSHOT_NOT_PINNED"
SOURCE_RELATION_UNSUPPORTED = "SOURCE_RELATION_UNSUPPORTED"
SOURCE_TIME_TRAVEL_UNAVAILABLE = "SOURCE_TIME_TRAVEL_UNAVAILABLE"
SOURCE_SNAPSHOT_BINDING_FAILED = "SOURCE_SNAPSHOT_BINDING_FAILED"
SOURCE_SNAPSHOT_VERIFICATION_FAILED = "SOURCE_SNAPSHOT_VERIFICATION_FAILED"

PERMANENT_TABLE = "permanent_table"
TRANSIENT_TABLE = "transient_table"
TEMPORARY_TABLE = "temporary_table"
VIEW = "view"
MATERIALIZED_VIEW = "materialized_view"
EXTERNAL_TABLE = "external_table"
DYNAMIC_TABLE = "dynamic_table"
UNSUPPORTED = "unsupported"
UNKNOWN = "unknown"

TIME_TRAVEL_TYPES = frozenset({PERMANENT_TABLE, TRANSIENT_TABLE})
UNSUPPORTED_TYPES = frozenset(
    {
        MATERIALIZED_VIEW,
        EXTERNAL_TABLE,
        DYNAMIC_TABLE,
        TEMPORARY_TABLE,
        UNSUPPORTED,
        UNKNOWN,
    }
)

_ISOLATED_MARKERS = ("AFFECTED_KEYS", "TARGET_BASE", "TARGET_HEAD")
_SECRET_KEY = re.compile(
    r"(password|secret|token|credential|api[_-]?key|private[_-]?key|entity[_-]?id)",
    re.IGNORECASE,
)
_ADAPTER_EVIDENCE_PREFIX = "frontier-adapter-snapshot:"


class SnapshotError(ConfigError):
    """Fail-closed snapshot binding or verification error."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class RelationBinding:
    name: str
    relation_type: str
    supported: bool
    leaf_names: tuple[str, ...] = ()
    view_sql: str | None = None
    retention_days: int | None = None

    def to_safe_dict(self) -> dict[str, Any]:
        return {
            "relationType": self.relation_type,
            "supported": self.supported,
            "leafCount": len(self.leaf_names),
            "retentionDays": self.retention_days,
        }


@dataclass
class SourceSnapshot:
    identifier: str
    mode: str
    assurance: str
    captured_at: str
    relation_bindings: tuple[RelationBinding, ...] = ()
    attestation_source: str | None = None
    adapter_evidence: str | None = None
    failure_code: str | None = None
    failure_reason: str | None = None
    phases: dict[str, str] = field(default_factory=dict)

    def record_phase(self, phase: str, identifier: str | None = None) -> None:
        self.phases[phase] = identifier or self.identifier

    def binding_for(self, name: str) -> RelationBinding | None:
        key = _relation_key(name)
        for binding in self.relation_bindings:
            if _relation_key(binding.name) == key:
                return binding
        return None

    @property
    def relations_checked(self) -> int:
        return len(self.relation_bindings)

    @property
    def relations_bound(self) -> int:
        return sum(1 for binding in self.relation_bindings if binding.supported)

    @property
    def consistent(self) -> bool:
        if not self.phases:
            return self.assurance == ASSURANCE_ADAPTER and self.relations_bound == self.relations_checked
        return len(set(self.phases.values())) == 1 and next(iter(self.phases.values())) == self.identifier

    def allows_sql_certified(self) -> bool:
        return (
            self.assurance == ASSURANCE_ADAPTER
            and self.mode == MODE_TIME_TRAVEL
            and bool(self.adapter_evidence)
            and self.adapter_evidence.startswith(_ADAPTER_EVIDENCE_PREFIX)
            and self.relations_checked > 0
            and self.relations_bound == self.relations_checked
            and self.consistent
            and self.failure_code is None
            and self.certification_phases_bound()
        )

    def certification_phases_bound(self) -> bool:
        """Discovery, targeted old/new, and confirmation must share this snapshot."""
        required = ("discovery", "targeted_base", "targeted_head", "confirmation")
        return all(self.phases.get(phase) == self.identifier for phase in required)

    def to_upload_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "mode": self.mode,
            "identifier": self.identifier,
            "assurance": self.assurance,
            "capturedAt": self.captured_at,
            "relationsChecked": self.relations_checked,
            "relationsBound": self.relations_bound,
            "consistentAcrossDiscoveryTargetedConfirmationAndReference": self.consistent,
            "attestationSource": self.attestation_source,
        }
        if self.failure_code:
            payload["failureCode"] = self.failure_code
        if self.failure_reason:
            payload["failureReason"] = _safe_reason(self.failure_reason)
        assert_snapshot_payload_is_safe(payload)
        return payload


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def adapter_evidence_token(identifier: str) -> str:
    return f"{_ADAPTER_EVIDENCE_PREFIX}{identifier}"


def unpinned_snapshot(
    *,
    failure_code: str = SOURCE_SNAPSHOT_NOT_PINNED,
    failure_reason: str = "source snapshot was not pinned",
    identifier: str = "none",
    captured_at: str | None = None,
    bindings: tuple[RelationBinding, ...] = (),
    attestation_source: str | None = None,
    mode: str = MODE_NONE,
    assurance: str = ASSURANCE_NONE,
) -> SourceSnapshot:
    return SourceSnapshot(
        identifier=identifier,
        mode=mode,
        assurance=assurance,
        captured_at=captured_at or utc_now_iso(),
        relation_bindings=bindings,
        attestation_source=attestation_source,
        adapter_evidence=None,
        failure_code=failure_code,
        failure_reason=failure_reason,
    )


def is_isolated_relation(name: str) -> bool:
    token = _unquoted(name).upper()
    if "FRONTIER_" not in token:
        return False
    return any(marker in token for marker in _ISOLATED_MARKERS)


def collect_source_relations(
    *sqls: str,
    dialect: str = "snowflake",
) -> tuple[str, ...]:
    found: dict[str, str] = {}
    for sql in sqls:
        text = (sql or "").strip()
        if not text:
            continue
        try:
            statements = [item for item in sqlglot.parse(text, dialect=dialect) if item is not None]
        except SqlglotError:
            continue
        for root in statements:
            for table in _source_table_nodes(root):
                name = _table_name(table)
                if not name or is_isolated_relation(name):
                    continue
                found.setdefault(_relation_key(name), name)
    return tuple(found.values())


def bind_sql_to_snapshot(sql: str, snapshot: SourceSnapshot, *, dialect: str = "snowflake") -> str:
    text = (sql or "").strip().rstrip(";")
    if not text:
        return sql
    if snapshot.assurance != ASSURANCE_ADAPTER:
        return sql
    try:
        statements = [item for item in sqlglot.parse(text, dialect=dialect) if item is not None]
    except SqlglotError as error:
        raise SnapshotError(SOURCE_SNAPSHOT_BINDING_FAILED, f"cannot parse SQL for snapshot binding: {error}") from error
    if not statements:
        return sql
    bound: list[str] = []
    for root in statements:
        _bind_expression(root, snapshot, dialect=dialect)
        bound.append(render_executable_sql(root, dialect=dialect, pretty=True))
    return ";\n".join(bound)


def verify_snapshot_binding(sql: str, snapshot: SourceSnapshot, *, dialect: str = "snowflake") -> bool:
    text = (sql or "").strip()
    if not text:
        return snapshot.assurance != ASSURANCE_ADAPTER
    if snapshot.assurance != ASSURANCE_ADAPTER:
        return False
    try:
        statements = [item for item in sqlglot.parse(text, dialect=dialect) if item is not None]
    except SqlglotError:
        return False
    for root in statements:
        for table in _source_table_nodes(root):
            name = _table_name(table)
            if not name or is_isolated_relation(name):
                continue
            binding = snapshot.binding_for(name)
            if binding is None:
                return False
            if binding.relation_type == VIEW:
                return False
            if binding.relation_type not in TIME_TRAVEL_TYPES:
                return False
            if not _table_matches_snapshot(table, snapshot.identifier):
                return False
    return True


def capture_from_catalog(
    relations: Iterable[str],
    *,
    catalog: dict[str, dict[str, Any]],
    identifier: str,
    captured_at: str,
    dialect: str = "snowflake",
    attestation_source: str | None = None,
) -> SourceSnapshot:
    """Build a snapshot from an in-memory relation catalog (tests / fake warehouse)."""
    del dialect
    if attestation_source:
        bindings = _catalog_bindings(relations, catalog)
        return unpinned_snapshot(
            failure_code=None if all(item.supported for item in bindings) else SOURCE_RELATION_UNSUPPORTED,
            failure_reason="externally attested snapshot; not adapter-verified",
            identifier=identifier,
            captured_at=captured_at,
            bindings=bindings,
            attestation_source=attestation_source,
            mode=MODE_EXTERNAL,
            assurance=ASSURANCE_EXTERNAL,
        )
    bindings, failure_code, failure_reason = _resolve_catalog_bindings(relations, catalog)
    if failure_code:
        return unpinned_snapshot(
            failure_code=failure_code,
            failure_reason=failure_reason or "source snapshot was not pinned",
            identifier=identifier,
            captured_at=captured_at,
            bindings=bindings,
        )
    return SourceSnapshot(
        identifier=identifier,
        mode=MODE_TIME_TRAVEL,
        assurance=ASSURANCE_ADAPTER,
        captured_at=captured_at,
        relation_bindings=bindings,
        adapter_evidence=adapter_evidence_token(identifier),
    )


def assert_snapshot_payload_is_safe(payload: dict[str, Any], *, path: tuple[str, ...] = ()) -> None:
    for key, value in payload.items():
        next_path = path + (key,)
        if _SECRET_KEY.search(str(key)):
            raise ConfigError("snapshot evidence must not include secret field " + ".".join(next_path))
        if isinstance(value, dict):
            assert_snapshot_payload_is_safe(value, path=next_path)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    assert_snapshot_payload_is_safe(item, path=next_path)
                else:
                    _reject_sensitive_text(str(item), next_path)
        else:
            _reject_sensitive_text(str(value), next_path)


def _reject_sensitive_text(text: str, path: tuple[str, ...]) -> None:
    lowered = text.lower()
    if "password=" in lowered or "secret=" in lowered or "frn_" in lowered and "entity" in ".".join(path).lower():
        raise ConfigError("snapshot evidence must not include secrets or entity IDs")


def _safe_reason(reason: str) -> str:
    text = " ".join((reason or "").split())
    return text[:512]


def _relation_key(name: str) -> str:
    catalog, schema, table = split_relation_parts(name)
    parts = [part.lower() for part in (catalog, schema, table) if part]
    return ".".join(parts)


def _unquoted(name: str) -> str:
    return name.replace('"', "").replace("`", "")


def _table_name(table: exp.Table) -> str:
    copied = table.copy()
    copied.set("alias", None)
    copied.set("when", None)
    return copied.sql(dialect="snowflake", comments=False).strip()


def _cte_names(root: exp.Expression) -> set[str]:
    names: set[str] = set()
    for with_ in root.find_all(exp.With):
        for cte in with_.expressions or []:
            alias = str(cte.alias or "")
            if alias:
                names.add(alias.lower())
    return names


def _source_table_nodes(root: exp.Expression) -> list[exp.Table]:
    cte_names = _cte_names(root)
    tables: list[exp.Table] = []
    for table in root.find_all(exp.Table):
        if isinstance(table.parent, exp.Create):
            continue
        this = table.this
        if not isinstance(this, exp.Identifier):
            continue
        name = str(table.name or "").lower()
        schema = str(table.db or "").lower()
        catalog = str(table.catalog or "").lower()
        if schema == "information_schema" or catalog == "information_schema":
            continue
        if not catalog and not schema and name in cte_names:
            continue
        tables.append(table)
    return tables


def _bind_expression(root: exp.Expression, snapshot: SourceSnapshot, *, dialect: str) -> None:
    for table in list(_source_table_nodes(root)):
        name = _table_name(table)
        if is_isolated_relation(name):
            continue
        binding = snapshot.binding_for(name)
        if binding is None:
            raise SnapshotError(SOURCE_SNAPSHOT_BINDING_FAILED, "assessment SQL referenced an unbound relation")
        if not binding.supported:
            raise SnapshotError(
                binding.relation_type in UNSUPPORTED_TYPES and SOURCE_RELATION_UNSUPPORTED or SOURCE_SNAPSHOT_BINDING_FAILED,
                "assessment SQL referenced an unsupported or unbound relation",
            )
        if binding.relation_type == VIEW:
            if not binding.view_sql:
                raise SnapshotError(SOURCE_RELATION_UNSUPPORTED, "view has no snapshot-capable definition")
            try:
                view_root = sqlglot.parse_one(binding.view_sql, dialect=dialect)
            except SqlglotError as error:
                raise SnapshotError(SOURCE_SNAPSHOT_BINDING_FAILED, f"cannot parse view definition: {error}") from error
            if view_root is None:
                raise SnapshotError(SOURCE_SNAPSHOT_BINDING_FAILED, "view definition is empty")
            _bind_expression(view_root, snapshot, dialect=dialect)
            alias = table.alias or table.name or "frontier_snapshot_view"
            subquery = exp.Subquery(this=view_root, alias=exp.to_identifier(str(alias)))
            table.replace(subquery)
            continue
        if binding.relation_type not in TIME_TRAVEL_TYPES:
            raise SnapshotError(SOURCE_RELATION_UNSUPPORTED, "relation type cannot use Time Travel")
        if _table_matches_snapshot(table, snapshot.identifier):
            continue
        table.set("when", _historical_data(snapshot.identifier))


def _historical_data(identifier: str) -> exp.HistoricalData:
    return exp.HistoricalData(
        this="AT",
        kind="TIMESTAMP",
        expression=exp.Anonymous(
            this="TO_TIMESTAMP_TZ",
            expressions=[exp.Literal.string(identifier)],
        ),
    )


def _table_matches_snapshot(table: exp.Table, identifier: str) -> bool:
    when = table.args.get("when")
    if not isinstance(when, exp.HistoricalData):
        return False
    kind = str(when.args.get("kind") or "").upper()
    if kind != "TIMESTAMP":
        return False
    rendered = when.sql(dialect="snowflake", comments=False)
    return identifier.lower() in rendered.lower()


def _catalog_entry(catalog: dict[str, dict[str, Any]], name: str) -> dict[str, Any] | None:
    key = _relation_key(name)
    for raw, entry in catalog.items():
        if _relation_key(raw) == key:
            return entry
    return None


def _kind_from_entry(entry: dict[str, Any]) -> str:
    raw = str(entry.get("kind") or entry.get("table_type") or PERMANENT_TABLE).strip().lower()
    mapping = {
        "base table": PERMANENT_TABLE,
        "table": PERMANENT_TABLE,
        "permanent": PERMANENT_TABLE,
        PERMANENT_TABLE: PERMANENT_TABLE,
        "transient": TRANSIENT_TABLE,
        TRANSIENT_TABLE: TRANSIENT_TABLE,
        "temporary": TEMPORARY_TABLE,
        "temp": TEMPORARY_TABLE,
        TEMPORARY_TABLE: TEMPORARY_TABLE,
        "view": VIEW,
        VIEW: VIEW,
        "materialized view": MATERIALIZED_VIEW,
        MATERIALIZED_VIEW: MATERIALIZED_VIEW,
        "external table": EXTERNAL_TABLE,
        EXTERNAL_TABLE: EXTERNAL_TABLE,
        "dynamic table": DYNAMIC_TABLE,
        DYNAMIC_TABLE: DYNAMIC_TABLE,
    }
    return mapping.get(raw, UNKNOWN)


def _catalog_bindings(relations: Iterable[str], catalog: dict[str, dict[str, Any]]) -> tuple[RelationBinding, ...]:
    bindings, _code, _reason = _resolve_catalog_bindings(relations, catalog, fail_closed=False)
    return bindings


def _resolve_catalog_bindings(
    relations: Iterable[str],
    catalog: dict[str, dict[str, Any]],
    *,
    fail_closed: bool = True,
) -> tuple[tuple[RelationBinding, ...], str | None, str | None]:
    resolved: dict[str, RelationBinding] = {}
    pending = [str(item).strip() for item in relations if str(item).strip()]
    seen: set[str] = set()
    failure_code: str | None = None
    failure_reason: str | None = None

    while pending:
        name = pending.pop(0)
        key = _relation_key(name)
        if key in seen:
            continue
        seen.add(key)
        if is_isolated_relation(name):
            continue
        entry = _catalog_entry(catalog, name)
        if entry is None:
            if catalog:
                failure_code = SOURCE_SNAPSHOT_NOT_PINNED
                failure_reason = "a source relation was not inventoried"
                resolved[key] = RelationBinding(name=name, relation_type=UNKNOWN, supported=False)
                if fail_closed:
                    continue
            else:
                resolved[key] = RelationBinding(
                    name=name,
                    relation_type=PERMANENT_TABLE,
                    supported=True,
                    leaf_names=(name,),
                    retention_days=1,
                )
                continue
        kind = _kind_from_entry(entry)
        retention = entry.get("retention_days")
        retention_days = int(retention) if retention is not None else 1
        if kind == VIEW:
            view_sql = str(entry.get("view_sql") or entry.get("definition") or "").strip()
            if not view_sql:
                failure_code = SOURCE_RELATION_UNSUPPORTED
                failure_reason = "a view could not be expanded to snapshot-capable leaves"
                resolved[key] = RelationBinding(name=name, relation_type=VIEW, supported=False)
                continue
            leaves = collect_source_relations(view_sql, dialect="snowflake")
            pending.extend(leaves)
            resolved[key] = RelationBinding(
                name=name,
                relation_type=VIEW,
                supported=True,
                leaf_names=leaves,
                view_sql=view_sql,
            )
            continue
        if kind in TIME_TRAVEL_TYPES:
            if retention_days <= 0:
                failure_code = SOURCE_TIME_TRAVEL_UNAVAILABLE
                failure_reason = "Time Travel retention is unavailable on a source table"
                resolved[key] = RelationBinding(
                    name=name,
                    relation_type=kind,
                    supported=False,
                    retention_days=retention_days,
                )
                continue
            resolved[key] = RelationBinding(
                name=name,
                relation_type=kind,
                supported=True,
                leaf_names=(name,),
                retention_days=retention_days,
            )
            continue
        failure_code = SOURCE_RELATION_UNSUPPORTED
        failure_reason = "a source relation type cannot be bound with Time Travel"
        resolved[key] = RelationBinding(name=name, relation_type=kind, supported=False)

    if fail_closed and any(not item.supported for item in resolved.values()) and failure_code is None:
        failure_code = SOURCE_SNAPSHOT_NOT_PINNED
        failure_reason = "one or more source relations were not bound"
    return tuple(resolved.values()), failure_code, failure_reason


def snapshot_from_json(payload: dict[str, Any] | None) -> SourceSnapshot | None:
    if not payload:
        return None
    return SourceSnapshot(
        identifier=str(payload.get("identifier") or "none"),
        mode=str(payload.get("mode") or MODE_NONE),
        assurance=str(payload.get("assurance") or ASSURANCE_NONE),
        captured_at=str(payload.get("capturedAt") or utc_now_iso()),
        attestation_source=payload.get("attestationSource"),
        failure_code=payload.get("failureCode"),
        failure_reason=payload.get("failureReason"),
    )


def dumps_safe(payload: dict[str, Any]) -> str:
    assert_snapshot_payload_is_safe(payload)
    return json.dumps(payload, sort_keys=True)
