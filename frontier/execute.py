from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.config import ConfigError
from frontier.progress import elapsed_ms, failure_status, log_step
from frontier.sql_fingerprint import sql_fingerprint
from frontier.warehouse import WarehouseAdapter, sql_literal, split_relation_parts

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_TOKEN = re.compile(r"[^A-Za-z0-9]+")
_PROD_NAMES = frozenset({"DBT_PROD", "PROD", "PRODUCTION"})
_DEFAULT_SCHEMA = "DBT_CI"
_TABLE_PREFIX = "FRONTIER_"
_AFFECTED_SUFFIX = "AFFECTED_KEYS"
_SNOWFLAKE_NAME_LIMIT = 255
_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SAFE_PROJECT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")

ORIGIN_EVENT = "event"
ORIGIN_SQL_CHANGE = "sql_change"
SQL_CHANGE_REASON = "SQL change candidate"

DEMO_IMPACT_TAGS = frozenset({"frontier_demo", "frontier_mutation"})
_DISCOVERY_FORBIDDEN = re.compile(
    r"frontier_affected_customers|affected_keys|frontier_keys",
    re.IGNORECASE,
)

HANDWRITTEN_FRONTIER_MODELS = (
    "frontier_affected_customers",
    "frontier_customer_orders_target",
    "frontier_customer_orders_target_after",
    "frontier_customer_summary_target",
    "frontier_customer_summary_target_after",
    "customer_summary_repaired",
)

_HANDWRITTEN_FRONTIER_MODEL_NAMES = frozenset(
    name.lower() for name in HANDWRITTEN_FRONTIER_MODELS
)


def is_sql_change_impact_model(
    name: str,
    *,
    tags: Iterable[str] = (),
    target_name: str | None = None,
) -> bool:
    """Production models whose impact SQL may run against the warehouse.

    Models tagged frontier_demo / frontier_mutation, mutation overlays, and
    handwritten Frontier demo models join frontier_affected_customers. Using
    them as the live candidate query counts the seed table instead of
    STG_ORDERS. The configured target is always eligible.
    """
    lowered = (name or "").strip().lower()
    if not lowered:
        return False
    if target_name and lowered == target_name.strip().lower():
        return True
    tagset = {str(tag).strip().lower() for tag in tags if str(tag).strip()}
    if tagset & DEMO_IMPACT_TAGS:
        return False
    if lowered in _HANDWRITTEN_FRONTIER_MODEL_NAMES:
        return False
    if lowered.startswith("frontier_") or lowered.startswith("mutation_"):
        return False
    if lowered.endswith(("_after", "_repaired", "_mutated")):
        return False
    if lowered in {"change_events_resolved", "mutation_deleted_order"}:
        return False
    return True


def discovery_sql_is_unrestricted(sql: str, *, affected_relation: str | None = None) -> bool:
    """True when candidate discovery does not join demo or isolated key tables."""
    text = (sql or "").strip()
    if not text:
        return False
    if _DISCOVERY_FORBIDDEN.search(text):
        return False
    relation = (affected_relation or "").strip()
    if relation and relation.lower() in text.lower():
        return False
    return True


def _normalize_ident(value: str | None) -> str:
    return (value or "").strip().strip('"').strip("`").upper()


def is_prod_identifier(value: str | None) -> bool:
    token = _normalize_ident(value)
    if not token:
        return False
    if token in _PROD_NAMES:
        return True
    return token.endswith("_PROD")


def assert_not_prod(*, database: str | None = None, schema: str | None = None, relation: str | None = None) -> None:
    """Refuse to read or write production warehouse locations."""
    parts: list[str] = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    if relation:
        catalog, rel_schema, table = split_relation_parts(relation)
        parts.extend([part for part in (catalog, rel_schema, table) if part])
    for part in parts:
        if is_prod_identifier(part):
            raise ConfigError("Frontier will not execute in DBT_PROD")


def run_relation_token(run_id: str) -> str:
    """Snowflake-safe token unique per assessment / PR commit."""
    raw = (run_id or "").strip()
    if not raw:
        raise ConfigError("run id is required for isolated frontier relations")
    token = _UNSAFE_TOKEN.sub("_", raw).strip("_").upper()
    if not token:
        raise ConfigError("run id did not yield a warehouse identifier")
    if not token[0].isalpha():
        token = f"R_{token}"
    budget = _SNOWFLAKE_NAME_LIMIT - len(_TABLE_PREFIX) - len(_AFFECTED_SUFFIX) - 1
    return token[:budget]


def isolated_table_name(run_id: str, suffix: str = _AFFECTED_SUFFIX) -> str:
    token = run_relation_token(run_id)
    suffix_token = _UNSAFE_TOKEN.sub("_", suffix).strip("_").upper() or _AFFECTED_SUFFIX
    return f"{_TABLE_PREFIX}{token}_{suffix_token}"


def isolated_location(
    *,
    model_database: str | None,
    model_schema: str | None,
    dialect: str = "snowflake",
) -> tuple[str, str]:
    database = (
        os.environ.get("FRONTIER_WAREHOUSE_DATABASE") or model_database or ""
    ).strip()
    schema = (
        os.environ.get("FRONTIER_WAREHOUSE_SCHEMA") or model_schema or _DEFAULT_SCHEMA
    ).strip()
    if not database:
        raise ConfigError("Frontier isolated execution requires a warehouse database")
    if not _safe_location_part(database, dialect=dialect, role="project"):
        raise ConfigError("warehouse database is not a safe identifier")
    if not _safe_location_part(schema, dialect=dialect, role="dataset"):
        raise ConfigError("warehouse schema is not a safe identifier")
    assert_not_prod(database=database, schema=schema)
    assert_not_prod(database=model_database, schema=model_schema)
    return database, schema


def _safe_location_part(value: str, *, dialect: str, role: str) -> bool:
    if dialect == "bigquery" and role == "project":
        return bool(_SAFE_PROJECT.fullmatch(value))
    return bool(_SAFE_IDENT.fullmatch(value))


def qualify_relation(
    database: str,
    schema: str,
    table: str,
    *,
    dialect: str = "snowflake",
) -> str:
    if dialect == "bigquery":
        if not _SAFE_PROJECT.fullmatch(database):
            raise ConfigError(f"unsafe warehouse identifier: {database}")
        if not _SAFE_IDENT.fullmatch(schema):
            raise ConfigError(f"unsafe warehouse identifier: {schema}")
        if not _SAFE_IDENT.fullmatch(table):
            raise ConfigError(f"unsafe warehouse identifier: {table}")
        return f"`{database}`.`{schema}`.`{table}`"
    for part in (database, schema, table):
        if not _SAFE_IDENT.fullmatch(part):
            raise ConfigError(f"unsafe warehouse identifier: {part}")
    return f"{database}.{schema}.{table}"


def affected_keys_relation(
    run_id: str,
    *,
    database: str,
    schema: str,
    dialect: str = "snowflake",
) -> str:
    return qualify_relation(
        database,
        schema,
        isolated_table_name(run_id, _AFFECTED_SUFFIX),
        dialect=dialect,
    )


def targeted_phase_relation(keys_relation: str, phase: str) -> str:
    """Warehouse table for targeted base or head output (not used in discovery)."""
    suffix = "TARGET_BASE" if phase == "base" else "TARGET_HEAD"
    relation = (keys_relation or "").strip()
    quote = ""
    body = relation
    if body.startswith("`") and body.endswith("`"):
        quote = "`"
        body = body[:-1]
    elif body.startswith('"') and body.endswith('"'):
        quote = '"'
        body = body[:-1]
    if body.upper().endswith("AFFECTED_KEYS"):
        return f"{body[: -len('AFFECTED_KEYS')]}{suffix}{quote}"
    return f"{relation}_{suffix}"


def create_schema_sql(database: str, schema: str, *, dialect: str = "snowflake") -> str:
    if dialect == "bigquery":
        return f"create schema if not exists `{database}.{schema}`"
    if dialect == "redshift":
        return f"create schema if not exists {schema}"
    return f"create schema if not exists {database}.{schema}"


def keys_select_sql(entity_key: str, values: Iterable[str], *, origin: str = ORIGIN_EVENT) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    literals = [sql_literal(str(value)) for value in values if str(value).strip()]
    if not literals:
        raise ConfigError("no candidate keys to materialize")
    unions = " union all ".join(
        f"select {item} as {entity_key}, {sql_literal(origin)} as origin" for item in literals
    )
    return (
        f"select distinct {entity_key} as {entity_key}, origin "
        f"from ({unions}) as frontier_keys "
        f"where {entity_key} is not null"
    )


def sql_change_keys_select_sql(entity_key: str, impact_sql: str) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    query = (impact_sql or "").strip().rstrip(";")
    if not query:
        raise ConfigError("SQL impact query is empty")
    return (
        f"select {entity_key} as {entity_key}, {sql_literal(ORIGIN_SQL_CHANGE)} as origin "
        f"from ({query}) as frontier_sql_change_keys "
        f"where {entity_key} is not null"
    )


def affected_keys_select_sql(
    entity_key: str,
    values: Iterable[str] = (),
    *,
    sql_change_queries: Iterable[str] = (),
) -> str:
    parts: list[str] = []
    event_values = [str(value).strip() for value in values if str(value).strip()]
    queries = [str(query).strip() for query in sql_change_queries if str(query).strip()]
    if event_values:
        parts.append(keys_select_sql(entity_key, event_values, origin=ORIGIN_EVENT))
    for query in queries:
        parts.append(sql_change_keys_select_sql(entity_key, query))
    if not parts:
        raise ConfigError("no candidate keys to materialize")
    inner = " union all ".join(f"({part})" for part in parts)
    return (
        f"select distinct {entity_key}, origin from ({inner}) as frontier_union_keys "
        f"where {entity_key} is not null"
    )


def create_affected_keys_sql(
    relation: str,
    entity_key: str,
    values: Iterable[str] = (),
    *,
    sql_change_queries: Iterable[str] = (),
    dialect: str = "snowflake",
) -> str:
    select_sql = affected_keys_select_sql(
        entity_key,
        values,
        sql_change_queries=sql_change_queries,
    )
    return create_table_as_sql(relation, select_sql, dialect=dialect)


def origin_count_sql(relation: str, entity_key: str) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    return (
        f"select "
        f"count(distinct case when origin = {sql_literal(ORIGIN_EVENT)} then {entity_key} end) "
        f"as event_candidate_count, "
        f"count(distinct case when origin = {sql_literal(ORIGIN_SQL_CHANGE)} then {entity_key} end) "
        f"as sql_change_candidate_count, "
        f"count(distinct {entity_key}) as union_candidate_count "
        f"from {relation} as frontier_origin_counts"
    )


def origin_keys_sql(relation: str, entity_key: str) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    return (
        f"select distinct {entity_key} as entity_value, origin as key_origin "
        f"from {relation} as frontier_origin_keys"
    )


def snapshot_execute(
    warehouse: WarehouseAdapter,
    sql: str,
    snapshot: Any | None = None,
    *,
    phase: str | None = None,
) -> list[tuple[Any, ...]]:
    """Execute SQL, binding source reads to the assessment snapshot when verified."""
    from frontier.snapshot import (
        ASSURANCE_ADAPTER,
        SOURCE_SNAPSHOT_VERIFICATION_FAILED,
        SnapshotError,
        bind_sql_to_snapshot,
        verify_snapshot_binding,
    )

    text = sql
    if snapshot is not None and snapshot.assurance == ASSURANCE_ADAPTER:
        bind = getattr(warehouse, "bind_query_to_snapshot", None)
        verify = getattr(warehouse, "verify_snapshot_binding", None)
        text = bind(sql, snapshot) if callable(bind) else bind_sql_to_snapshot(
            sql,
            snapshot,
            dialect=warehouse.dialect,
        )
        ok = (
            verify(text, snapshot)
            if callable(verify)
            else verify_snapshot_binding(text, snapshot, dialect=warehouse.dialect)
        )
        if not ok:
            raise SnapshotError(
                SOURCE_SNAPSHOT_VERIFICATION_FAILED,
                "bound SQL did not verify against the captured snapshot",
            )
    if snapshot is not None and phase:
        snapshot.record_phase(phase)
    return warehouse.execute(text)


def drop_relation_sql(relation: str) -> str:
    return f"drop table if exists {relation}"


def create_table_as_sql(relation: str, select_sql: str, *, dialect: str = "snowflake") -> str:
    query = (select_sql or "").strip().rstrip(";")
    if not query:
        raise ConfigError("empty SQL for isolated table")
    assert_not_prod(relation=relation)
    if dialect == "redshift":
        return f"create table {relation} as {query}"
    return f"create or replace table {relation} as {query}"


def _reject_mutating_sql(root: exp.Expression) -> None:
    if isinstance(root, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Command)):
        raise ConfigError("unsupported SQL")
    if root.find(exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Command):
        raise ConfigError("unsupported SQL")


def _render(node: exp.Expression, dialect: str = "snowflake") -> str:
    return node.sql(
        dialect=dialect,
        comments=False,
        pretty=True,
        normalize=True,
        normalize_functions="lower",
    ).strip()


def _from_table_alias(select: exp.Select) -> str:
    from_ = select.args.get("from_")
    if from_ is None:
        return ""
    this = from_.this
    if this is None:
        return ""
    alias = str(getattr(this, "alias", None) or "")
    if alias:
        return alias
    if isinstance(this, exp.Table):
        return str(this.name or "")
    return ""


def _select_can_bind_entity_key(select: exp.Select, entity_key: str) -> bool:
    from_ = select.args.get("from_")
    if from_ is None or from_.find(exp.Table) is None:
        return False
    target = entity_key.lower()
    if select.find(exp.Star):
        return True
    for expr in select.expressions or []:
        alias = str(expr.alias or "").lower()
        if alias == target:
            return True
    for column in select.find_all(exp.Column):
        if str(column.name or "").lower() == target:
            return True
    group = select.args.get("group")
    if group is not None:
        for column in group.find_all(exp.Column):
            if str(column.name or "").lower() == target:
                return True
    return False


def _entity_key_join_expression(select: exp.Select, entity_key: str) -> exp.Expression:
    """Join affected keys on the physical column that produces the entity key."""
    target = entity_key.lower()
    for expr in select.expressions or []:
        alias = str(expr.alias or "").lower()
        if alias != target:
            continue
        source = expr.this if isinstance(expr, exp.Alias) else expr
        return source.copy()
    alias = _from_table_alias(select)
    return exp.column(entity_key, table=alias) if alias else exp.column(entity_key)


def _is_keys_join(join: exp.Join) -> bool:
    alias = str(join.alias or "").lower()
    if alias == "frontier_keys":
        return True
    table = join.find(exp.Table)
    if table is None:
        return False
    table_alias = str(table.alias or "").lower()
    name = str(table.name or "").lower()
    return table_alias == "frontier_keys" or name.endswith("affected_keys")


def _qualify_entity_key_against_keys_join(select: exp.Select, entity_key: str) -> None:
    """Keep customer_id unambiguous after joining frontier_keys."""
    alias = _from_table_alias(select)
    if not alias:
        return
    target = entity_key.lower()
    qualified: list[exp.Expression] = []
    for expr in select.expressions or []:
        if isinstance(expr, exp.Star) or (
            isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star) and not expr.table
        ):
            qualified.append(exp.Column(this=exp.Star(), table=exp.to_identifier(alias)))
            continue
        for column in expr.find_all(exp.Column):
            if str(column.name or "").lower() != target:
                continue
            if column.table:
                continue
            column.set("table", exp.to_identifier(alias))
        qualified.append(expr)
    select.set("expressions", qualified)

    def qualify_root(root: exp.Expression | None) -> None:
        if root is None:
            return
        for column in root.find_all(exp.Column):
            if str(column.name or "").lower() != target:
                continue
            if column.table:
                continue
            column.set("table", exp.to_identifier(alias))

    qualify_root(select.args.get("where"))
    group = select.args.get("group")
    if group is not None:
        qualify_root(group)
    qualify_root(select.args.get("having"))


def _inject_keys_join(
    select: exp.Select,
    entity_key: str,
    affected_relation: str,
    *,
    dialect: str = "snowflake",
) -> bool:
    if any(_is_keys_join(join) for join in (select.args.get("joins") or [])):
        _qualify_entity_key_against_keys_join(select, entity_key)
        return True
    if not _select_can_bind_entity_key(select, entity_key):
        return False
    alias = _from_table_alias(select)
    key_col = _entity_key_join_expression(select, entity_key)
    keys_table = exp.alias_(
        exp.to_table(affected_relation, dialect=dialect),
        "frontier_keys",
        table=True,
    )
    join = exp.Join(
        this=keys_table,
        kind="INNER",
        on=exp.EQ(this=key_col, expression=exp.column(entity_key, table="frontier_keys")),
    )
    existing = list(select.args.get("joins") or [])
    select.set("joins", [join, *existing])
    _qualify_entity_key_against_keys_join(select, entity_key)
    return True


def restriction_is_pushed(sql: str, *, entity_key: str, dialect: str = "snowflake") -> bool:
    """True when the key join sits on a source select, not only an outer wrapper."""
    del entity_key
    text = (sql or "").strip()
    if not text:
        return False
    try:
        root = sqlglot.parse_one(text, dialect=dialect)
    except SqlglotError:
        return False
    if root is None:
        return False
    lowered = text.lower()
    wrapper_only = "as frontier_target" in lowered and "inner join" not in lowered
    if wrapper_only:
        return False
    return any(
        _is_keys_join(join)
        for select in root.find_all(exp.Select)
        for join in (select.args.get("joins") or [])
    )


def profile_shows_reduction(full: dict[str, Any], targeted: dict[str, Any]) -> bool:
    """True when the targeted profile scanned or produced strictly less than the full plan."""
    for key in (
        "bytes_scanned",
        "total_bytes_processed",
        "partitions_scanned",
        "rows_produced",
    ):
        full_value = full.get(key)
        targeted_value = targeted.get(key)
        if isinstance(full_value, (int, float)) and isinstance(targeted_value, (int, float)):
            if targeted_value < full_value:
                return True
    return False


def sql_change_impact_queries(
    comparison: dict[str, Any] | None,
    *,
    target_name: str | None = None,
) -> tuple[tuple[str, ...], bool]:
    """Return the canonical impact SQL to execute, and whether SQL change requires it.

    Demo/mutation models are ignored. Equivalent predicates from multiple
    macro consumers are collapsed to one query. Discovery SQL that joins
    frontier_affected_customers or an affected-keys table is rejected.
    """
    if not comparison:
        return (), False
    added = [
        row
        for row in (comparison.get("added") or [])
        if is_sql_change_impact_model(
            str(row.get("name") or ""),
            tags=tuple(row.get("tags") or ()),
            target_name=target_name,
        )
    ]
    removed = [
        row
        for row in (comparison.get("removed") or [])
        if is_sql_change_impact_model(
            str(row.get("name") or ""),
            tags=tuple(row.get("tags") or ()),
            target_name=target_name,
        )
    ]
    modified = list(comparison.get("modified") or [])
    eligible = [
        row
        for row in modified
        if is_sql_change_impact_model(
            str(row.get("name") or ""),
            tags=tuple(row.get("tags") or ()),
            target_name=target_name,
        )
    ]
    required = bool(added or removed or eligible)
    if not required:
        return (), False
    if added or removed:
        return (), True
    if comparison.get("fullRebuildRequired") or comparison.get("narrowFrontierSafe") is False:
        return (), True
    ranked: list[tuple[int, str, str]] = []
    for row in eligible:
        eligibility = row.get("staticEligibility") or {}
        if eligibility.get("eligible") is False or (
            eligibility.get("eligible") is True and eligibility.get("compiled") is False
        ):
            return (), True
        name = str(row.get("name") or "")
        if row.get("unsafe") or row.get("impactStatus") == "FULL_REBUILD_REQUIRED":
            return (), True
        sql = str(row.get("candidateSql") or "").strip()
        if not sql or not discovery_sql_is_unrestricted(sql):
            return (), True
        ranked.append((len(row.get("downstream") or []), name, sql))
    if not ranked:
        return (), True
    best_by_fp: dict[str, tuple[int, str, str]] = {}
    for downstream_n, name, sql in ranked:
        fingerprint = sql_fingerprint(sql)
        previous = best_by_fp.get(fingerprint)
        if previous is None or downstream_n > previous[0]:
            best_by_fp[fingerprint] = (downstream_n, name, sql)
    chosen = max(best_by_fp.values(), key=lambda item: (item[0], item[1]))
    return (chosen[2],), True


def generate_targeted_sql(
    model_sql: str,
    *,
    entity_key: str,
    affected_relation: str,
    dialect: str = "snowflake",
) -> str:
    """Push the affected-key join into source scans before joins and aggregates."""
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    text = (model_sql or "").strip().rstrip(";")
    if not text:
        raise ConfigError("empty model SQL")
    try:
        statements = [item for item in sqlglot.parse(text, dialect=dialect) if item is not None]
    except SqlglotError as error:
        raise ConfigError(f"cannot generate targeted SQL: {error}") from error
    if len(statements) != 1:
        raise ConfigError("targeted SQL requires a single SELECT")
    root = statements[0]
    _reject_mutating_sql(root)
    if isinstance(root, (exp.Union, exp.Except, exp.Intersect)):
        raise ConfigError("cannot push affected-key restriction")
    outer = root if isinstance(root, exp.Select) else None
    if outer is None:
        raise ConfigError("targeted SQL requires a SELECT")
    with_ = outer.args.get("with_")
    if with_ is not None and with_.args.get("recursive"):
        raise ConfigError("cannot push affected-key restriction")
    injected = 0
    if with_ is not None:
        for cte in with_.expressions or []:
            body = cte.this
            if isinstance(body, exp.Select) and _inject_keys_join(
                body,
                entity_key,
                affected_relation,
                dialect=dialect,
            ):
                injected += 1
    if _inject_keys_join(outer, entity_key, affected_relation, dialect=dialect):
        injected += 1
    if injected == 0:
        raise ConfigError("cannot push affected-key restriction")
    rendered = _render(outer, dialect)
    if not restriction_is_pushed(rendered, entity_key=entity_key, dialect=dialect):
        raise ConfigError("cannot push affected-key restriction")
    return rendered


def _except_keyword(dialect: str) -> str:
    if dialect == "bigquery":
        return "except distinct"
    return "except"


def confirmed_changed_sql(
    *,
    before_sql: str,
    after_sql: str,
    entity_key: str,
    dialect: str = "snowflake",
) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    except_op = _except_keyword(dialect)
    return (
        f"select distinct {entity_key} as {entity_key} from ("
        f" select * from ({before_sql}) as frontier_before"
        f" {except_op}"
        f" select * from ({after_sql}) as frontier_after"
        " union"
        f" select * from ({after_sql}) as frontier_after_2"
        f" {except_op}"
        f" select * from ({before_sql}) as frontier_before_2"
        ") as frontier_changed"
    )


def generic_repaired_sql(
    *,
    before_relation: str,
    targeted_after_sql: str,
    affected_relation: str,
    entity_key: str,
) -> str:
    if not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    return (
        f"select * from {before_relation} as frontier_keep "
        f"where frontier_keep.{entity_key} not in ("
        f"select {entity_key} from {affected_relation}) "
        "union all "
        f"select * from ({targeted_after_sql}) as frontier_repaired_target"
    )


@dataclass(frozen=True)
class IsolatedExecution:
    relation: str
    database: str
    schema: str
    run_id: str
    candidate_sql: str | None = None
    event_candidate_count: int = 0
    sql_change_candidate_count: int = 0
    union_candidate_count: int = 0
    origin_keys: tuple[tuple[str, str], ...] = ()
    query_id: str | None = None


@dataclass
class IsolatedRun:
    warehouse: WarehouseAdapter
    relation: str
    database: str
    schema: str
    run_id: str
    entity_key: str
    _created: list[str] = field(default_factory=list)
    _cleaned: bool = False
    last_targeted_query_id: str | None = None
    targeted_base_relation: str | None = None
    targeted_head_relation: str | None = None
    confirmed_count: int | None = None
    phase_timings: dict[str, int] = field(default_factory=dict)
    job_metrics: list[dict[str, Any]] = field(default_factory=list)
    snapshot: Any | None = None
    guard: Any = field(default=None, repr=False)
    mart_baseline: dict[str, Any] = field(default_factory=dict)
    repair_validation: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        from frontier.mutation import GuardedWarehouse, build_guard
        from frontier.repair import empty_mart_baseline, empty_repair_validation

        if not self.mart_baseline:
            self.mart_baseline = empty_mart_baseline()
        if not self.repair_validation:
            self.repair_validation = empty_repair_validation()
        if isinstance(self.warehouse, GuardedWarehouse):
            self.guard = getattr(self.warehouse, "_guard", self.guard)
            return
        dialect = getattr(self.warehouse, "dialect", "snowflake")
        self.guard = build_guard(
            run_id=self.run_id,
            database=self.database,
            schema=self.schema,
            keys_relation=self.relation,
            dialect=dialect,
        )
        self.warehouse = GuardedWarehouse(self.warehouse, self.guard)

    def _record_job(self, phase: str) -> dict[str, Any] | None:
        query_id = getattr(self.warehouse, "last_query_id", None)
        profile: dict[str, Any] = {}
        getter = getattr(self.warehouse, "get_query_profile", None)
        if callable(getter) and query_id:
            try:
                profile = dict(getter(str(query_id)) or {})
            except Exception:
                profile = {}
        if not query_id and not profile:
            return None
        record = {
            "phase": phase,
            "query_id": str(query_id) if query_id else profile.get("query_id") or profile.get("job_id"),
            "elapsed_ms": (
                profile["elapsed_ms"]
                if profile.get("elapsed_ms") is not None
                else profile["total_elapsed_ms"]
                if profile.get("total_elapsed_ms") is not None
                else self.phase_timings.get(phase)
            ),
            "bytes_scanned": profile.get("bytes_scanned"),
            "total_bytes_processed": profile.get("total_bytes_processed"),
            "total_bytes_billed": profile.get("total_bytes_billed"),
            "slot_millis": profile.get("slot_millis"),
            "location": profile.get("location"),
        }
        if "status" in profile:
            record["status"] = profile.get("status")
        if "queue_ms" in profile:
            record["queue_ms"] = profile.get("queue_ms")
        if "execution_ms" in profile:
            record["execution_ms"] = profile.get("execution_ms")
        if "metrics_available" in profile:
            record["metrics_available"] = profile.get("metrics_available")
        self.job_metrics.append(record)
        return record

    def __enter__(self) -> IsolatedRun:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()

    def materialize(
        self,
        values: Iterable[str] = (),
        *,
        sql_change_queries: Iterable[str] = (),
    ) -> IsolatedExecution:
        assert_not_prod(database=self.database, schema=self.schema, relation=self.relation)
        try:
            self.warehouse.execute(create_schema_sql(self.database, self.schema, dialect=self.warehouse.dialect))
        except Exception as error:
            detail = str(error)
            lowered = detail.lower()
            if (
                "42501" not in detail
                and "insufficient privileges" not in lowered
                and "access denied" not in lowered
                and "permission denied" not in lowered
                and "already exists" not in lowered
            ):
                raise
        sql = create_affected_keys_sql(
            self.relation,
            self.entity_key,
            values,
            sql_change_queries=sql_change_queries,
            dialect=self.warehouse.dialect,
        )
        log_step("candidate materialization started")
        started = time.perf_counter()
        try:
            self._replace_relation(self.relation, sql)
        except Exception as error:
            self.phase_timings["candidate materialization"] = elapsed_ms(started)
            log_step(
                "candidate materialization completed",
                duration_ms=self.phase_timings["candidate materialization"],
                status=failure_status(error),
            )
            raise
        self.phase_timings["candidate materialization"] = elapsed_ms(started)
        log_step(
            "candidate materialization completed",
            duration_ms=self.phase_timings["candidate materialization"],
            status="ok",
        )
        self._created.append(self.relation)
        query_id = getattr(self.warehouse, "last_query_id", None)
        self._record_job("candidate materialization")
        event_values = [str(value).strip() for value in values if str(value).strip()]
        origin_keys: tuple[tuple[str, str], ...] = ()
        if event_values:
            origin_keys = tuple(
                (
                    str(row[0]),
                    str(row[1]) if len(row) > 1 and row[1] is not None else ORIGIN_EVENT,
                )
                for row in snapshot_execute(
                    self.warehouse,
                    origin_keys_sql(self.relation, self.entity_key),
                    self.snapshot,
                    phase="candidate_keys",
                )
                if row and row[0] is not None
            )
        count_rows = snapshot_execute(
            self.warehouse,
            origin_count_sql(self.relation, self.entity_key),
            self.snapshot,
            phase="candidate_counts",
        )
        event_count = 0
        sql_count = 0
        union_count = len({value for value, _origin in origin_keys})
        if count_rows:
            row = count_rows[0]
            event_count = int(row[0] or 0)
            sql_count = int(row[1] or 0) if len(row) > 1 else 0
            union_count = int(row[2] or union_count) if len(row) > 2 else union_count
        return IsolatedExecution(
            relation=self.relation,
            database=self.database,
            schema=self.schema,
            run_id=self.run_id,
            candidate_sql=sql,
            event_candidate_count=event_count,
            sql_change_candidate_count=sql_count,
            union_candidate_count=union_count,
            origin_keys=origin_keys,
            query_id=str(query_id) if query_id else None,
        )

    def confirm(
        self,
        *,
        before_sql: str,
        after_sql: str,
        dialect: str | None = None,
    ) -> tuple[str, ...] | None:
        """Return confirmed keys, or None when analysis fails (never treat failure as empty)."""
        dialect_name = dialect or self.warehouse.dialect
        try:
            targeted_before = generate_targeted_sql(
                before_sql,
                entity_key=self.entity_key,
                affected_relation=self.relation,
                dialect=dialect_name,
            )
            targeted_after = generate_targeted_sql(
                after_sql,
                entity_key=self.entity_key,
                affected_relation=self.relation,
                dialect=dialect_name,
            )
        except Exception as error:
            log_step("targeted base execution started")
            log_step("targeted base execution completed", status=failure_status(error))
            if isinstance(error, (ConfigError, SqlglotError)):
                return None
            raise
        base_relation = targeted_phase_relation(self.relation, "base")
        head_relation = targeted_phase_relation(self.relation, "head")
        log_step("targeted base execution started")
        started = time.perf_counter()
        try:
            self._replace_relation(
                base_relation,
                create_table_as_sql(base_relation, targeted_before, dialect=dialect_name),
            )
        except Exception as error:
            duration = elapsed_ms(started)
            self.phase_timings["targeted base execution"] = duration
            log_step(
                "targeted base execution completed",
                duration_ms=duration,
                status=failure_status(error),
            )
            raise
        self._created.append(base_relation)
        self.targeted_base_relation = base_relation
        self.phase_timings["targeted base execution"] = elapsed_ms(started)
        self._record_job("targeted base execution")
        log_step(
            "targeted base execution completed",
            duration_ms=self.phase_timings["targeted base execution"],
            status="ok",
        )
        log_step("targeted head execution started")
        started = time.perf_counter()
        try:
            self._replace_relation(
                head_relation,
                create_table_as_sql(head_relation, targeted_after, dialect=dialect_name),
            )
        except Exception as error:
            duration = elapsed_ms(started)
            self.phase_timings["targeted head execution"] = duration
            log_step(
                "targeted head execution completed",
                duration_ms=duration,
                status=failure_status(error),
            )
            raise
        self._created.append(head_relation)
        self.targeted_head_relation = head_relation
        self.phase_timings["targeted head execution"] = elapsed_ms(started)
        self._record_job("targeted head execution")
        log_step(
            "targeted head execution completed",
            duration_ms=self.phase_timings["targeted head execution"],
            status="ok",
        )
        log_step("confirmation started")
        started = time.perf_counter()
        try:
            sql = confirmed_changed_sql(
                before_sql=f"select * from {base_relation}",
                after_sql=f"select * from {head_relation}",
                entity_key=self.entity_key,
                dialect=dialect_name,
            )
            count_sql = (
                "select count(*) as confirmed_frontier_count "
                f"from ({sql}) as frontier_confirmed"
            )
            rows = snapshot_execute(
                self.warehouse,
                count_sql,
                self.snapshot,
                phase="confirmation",
            )
        except Exception as error:
            duration = elapsed_ms(started)
            self.phase_timings["confirmation"] = duration
            log_step(
                "confirmation completed",
                duration_ms=duration,
                status=failure_status(error),
            )
            if isinstance(error, (ConfigError, SqlglotError)):
                return None
            raise
        self.confirmed_count = int(rows[0][0]) if rows and rows[0][0] is not None else 0
        self.phase_timings["confirmation"] = elapsed_ms(started)
        log_step(
            "confirmation completed",
            duration_ms=self.phase_timings["confirmation"],
            status="ok",
        )
        self.last_targeted_query_id = getattr(self.warehouse, "last_query_id", None)
        self._record_job("confirmation")
        return ()

    def _replace_relation(self, relation: str, create_sql: str) -> None:
        """Materialize an isolated table without colliding with a concurrent PR.

        Redshift has no CREATE OR REPLACE TABLE for this path, so drop first.
        Drop failures are ignored when the table does not exist yet.
        """
        if self.warehouse.dialect == "redshift":
            try:
                if self.guard is not None:
                    self.guard.register(relation)
                self.warehouse.execute(drop_relation_sql(relation))
            except Exception:
                pass
        phase = "candidate_materialization"
        lowered = create_sql.lower()
        if "target_base" in lowered:
            phase = "targeted_base"
        elif "target_head" in lowered:
            phase = "targeted_head"
        snapshot_execute(self.warehouse, create_sql, self.snapshot, phase=phase)
        if relation not in self._created:
            self._created.append(relation)

    def mart_copy_relation(self) -> str:
        return qualify_relation(
            self.database,
            self.schema,
            isolated_table_name(self.run_id, "MART_COPY"),
            dialect=self.warehouse.dialect,
        )

    def _note_query(self, ids: list[str]) -> None:
        query_id = getattr(self.warehouse, "last_query_id", None)
        if query_id:
            ids.append(str(query_id))

    def verify_mart_baseline(
        self,
        *,
        target_relation: str,
        old_sql: str,
        entity_key_index: int = 0,
    ) -> dict[str, Any]:
        from frontier.repair import (
            MART_BASELINE_FAILED,
            MART_BASELINE_MATCHED,
            MART_BASELINE_MISMATCHED,
            SCHEMA_MISALIGNED,
            SNAPSHOT_IDENTIFIER_MISMATCH,
            TARGET_SNAPSHOT_NOT_PINNED,
            WAREHOUSE_EXECUTION_FAILED,
            compare_complete_results,
            empty_mart_baseline,
            snapshot_identifiers_consistent,
            target_binding_is_usable,
        )
        from frontier.snapshot import SnapshotError

        payload = empty_mart_baseline()
        query_ids: list[str] = []
        identifier = getattr(self.snapshot, "identifier", None)
        payload["snapshotIdentifier"] = identifier
        try:
            from frontier.snapshot import include_target_in_snapshot

            ok, code, reason = include_target_in_snapshot(self.snapshot, target_relation, self.warehouse)
            if ok:
                ok, code, reason = target_binding_is_usable(self.snapshot, target_relation)
            if not ok:
                payload.update(
                    status=MART_BASELINE_FAILED,
                    reasonCode=code or TARGET_SNAPSHOT_NOT_PINNED,
                    reason=reason,
                    failurePhase="baseline verification",
                )
                self.mart_baseline = payload
                return payload
            if not snapshot_identifiers_consistent(self.snapshot):
                payload.update(
                    status=MART_BASELINE_FAILED,
                    reasonCode=SNAPSHOT_IDENTIFIER_MISMATCH,
                    reason="snapshot identifiers are not identical across required reads",
                    failurePhase="baseline verification",
                )
                self.mart_baseline = payload
                return payload
            log_step("baseline verification started")
            started = time.perf_counter()
            mart_sql = f"select * from {target_relation} as frontier_mart_baseline"
            old_wrapped = f"select * from ({(old_sql or '').strip().rstrip(';')}) as frontier_old_complete"
            mart_rows = snapshot_execute(
                self.warehouse, mart_sql, self.snapshot, phase="baseline verification"
            )
            self._note_query(query_ids)
            old_rows = snapshot_execute(
                self.warehouse, old_wrapped, self.snapshot, phase="baseline verification"
            )
            self._note_query(query_ids)
            compared = compare_complete_results(
                mart_rows,
                old_rows,
                entity_key_index=entity_key_index,
            )
            payload["warehouseQueryIds"] = query_ids
            payload["missingRows"] = compared.missing_rows if compared.reason_code is None else None
            payload["extraRows"] = compared.extra_rows if compared.reason_code is None else None
            payload["mismatchedRows"] = compared.mismatched_rows if compared.reason_code is None else None
            payload["duplicateEntityKeys"] = compared.duplicate_entity_keys or None
            if compared.reason_code:
                payload.update(
                    status=MART_BASELINE_FAILED,
                    reasonCode=compared.reason_code,
                    reason=compared.reason,
                    failurePhase="baseline verification",
                )
                if compared.reason_code == SCHEMA_MISALIGNED:
                    payload["missingRows"] = None
                    payload["extraRows"] = None
                    payload["mismatchedRows"] = None
            elif compared.ok:
                payload.update(status=MART_BASELINE_MATCHED, missingRows=0, extraRows=0, mismatchedRows=0, duplicateEntityKeys=0)
            else:
                payload.update(status=MART_BASELINE_MISMATCHED)
            self.phase_timings["baseline verification"] = elapsed_ms(started)
            self._record_job("baseline verification")
            log_step("baseline verification completed", duration_ms=self.phase_timings["baseline verification"], status="ok")
        except SnapshotError as error:
            payload.update(
                status=MART_BASELINE_FAILED,
                reasonCode=getattr(error, "code", TARGET_SNAPSHOT_NOT_PINNED),
                reason=str(error)[:512],
                failurePhase="baseline verification",
                warehouseQueryIds=query_ids,
            )
        except Exception as error:
            payload.update(
                status=MART_BASELINE_FAILED,
                reasonCode=WAREHOUSE_EXECUTION_FAILED,
                reason=str(error)[:512],
                failurePhase="baseline verification",
                warehouseQueryIds=query_ids,
            )
        self.mart_baseline = payload
        return payload

    def validate_disposable_repair(
        self,
        *,
        target_relation: str,
        new_sql: str,
        certified: bool,
        confirmed: bool,
        entity_key_index: int = 0,
    ) -> dict[str, Any]:
        from frontier.repair import (
            GRAIN_VIOLATED,
            MART_BASELINE_MATCHED,
            PREREQUISITES_NOT_MET,
            REPAIR_FAILED,
            REPAIR_SUCCEEDED,
            SCHEMA_MISALIGNED,
            SNAPSHOT_IDENTIFIER_MISMATCH,
            WAREHOUSE_EXECUTION_FAILED,
            compare_complete_results,
            empty_repair_validation,
            snapshot_identifiers_consistent,
        )

        payload = empty_repair_validation()
        payload["snapshotIdentifier"] = getattr(self.snapshot, "identifier", None)
        payload["candidateCount"] = self.confirmed_count
        if not certified or not confirmed:
            payload.update(
                reasonCode=PREREQUISITES_NOT_MET,
                reason="candidate certification and confirmation are required before disposable repair",
            )
            self.repair_validation = payload
            return payload
        if (self.mart_baseline or {}).get("status") != MART_BASELINE_MATCHED:
            payload.update(
                reasonCode=PREREQUISITES_NOT_MET,
                reason="mart baseline is not MATCHED",
            )
            self.repair_validation = payload
            return payload
        if not snapshot_identifiers_consistent(self.snapshot):
            payload.update(
                status=REPAIR_FAILED,
                reasonCode=SNAPSHOT_IDENTIFIER_MISMATCH,
                reason="snapshot identifiers are not identical across required reads",
                failurePhase="disposable repair",
            )
            self.repair_validation = payload
            return payload
        query_ids: list[str] = []
        copy = self.mart_copy_relation()
        try:
            log_step("disposable mart creation started")
            started = time.perf_counter()
            self._replace_relation(
                copy,
                create_table_as_sql(
                    copy,
                    f"select * from {target_relation}",
                    dialect=self.warehouse.dialect,
                ),
            )
            self.phase_timings["disposable mart creation"] = elapsed_ms(started)
            self._record_job("disposable mart creation")
            self._note_query(query_ids)
            log_step("disposable mart creation completed", duration_ms=self.phase_timings["disposable mart creation"], status="ok")

            delete_count_sql = (
                f"select count(*) as frontier_deleted_count from {copy} as frontier_deleted_count "
                f"inner join {self.relation} as frontier_keys "
                f"on frontier_deleted_count.{self.entity_key} = frontier_keys.{self.entity_key}"
            )
            deleted_rows = snapshot_execute(
                self.warehouse, delete_count_sql, self.snapshot, phase="candidate-key delete"
            )
            self._note_query(query_ids)
            deleted = int(deleted_rows[0][0]) if deleted_rows and deleted_rows[0][0] is not None else 0
            log_step("candidate-key delete started")
            started = time.perf_counter()
            self.warehouse.execute(
                f"delete from {copy} where {self.entity_key} in "
                f"(select {self.entity_key} from {self.relation})"
            )
            self.phase_timings["candidate-key delete"] = elapsed_ms(started)
            self._record_job("candidate-key delete")
            self._note_query(query_ids)
            log_step("candidate-key delete completed", duration_ms=self.phase_timings["candidate-key delete"], status="ok")

            head_rel = self.targeted_head_relation
            if not head_rel:
                targeted = generate_targeted_sql(
                    new_sql,
                    entity_key=self.entity_key,
                    affected_relation=self.relation,
                    dialect=self.warehouse.dialect,
                )
                head_rel = targeted_phase_relation(self.relation, "head")
                log_step("targeted head computation started")
                started = time.perf_counter()
                self._replace_relation(
                    head_rel,
                    create_table_as_sql(head_rel, targeted, dialect=self.warehouse.dialect),
                )
                self.targeted_head_relation = head_rel
                self.phase_timings["targeted head computation"] = elapsed_ms(started)
                self._record_job("targeted head computation")
                log_step("targeted head computation completed", duration_ms=self.phase_timings["targeted head computation"], status="ok")
            else:
                self._record_job("targeted head computation")
            self._note_query(query_ids)

            insert_count_sql = f"select count(*) as frontier_inserted_count from {head_rel} as frontier_inserted_count"
            inserted_rows = snapshot_execute(
                self.warehouse, insert_count_sql, self.snapshot, phase="targeted insert"
            )
            self._note_query(query_ids)
            inserted = int(inserted_rows[0][0]) if inserted_rows and inserted_rows[0][0] is not None else 0
            log_step("targeted insert started")
            started = time.perf_counter()
            self.warehouse.execute(f"insert into {copy} select * from {head_rel}")
            self.phase_timings["targeted insert"] = elapsed_ms(started)
            self._record_job("targeted insert")
            self._note_query(query_ids)
            log_step("targeted insert completed", duration_ms=self.phase_timings["targeted insert"], status="ok")

            log_step("complete repaired-table validation started")
            started = time.perf_counter()
            repaired = snapshot_execute(
                self.warehouse,
                f"select * from {copy} as frontier_repaired_copy",
                None,
                phase="complete repaired-table validation",
            )
            self._note_query(query_ids)
            head_complete = snapshot_execute(
                self.warehouse,
                f"select * from ({(new_sql or '').strip().rstrip(';')}) as frontier_head_complete",
                self.snapshot,
                phase="complete head computation",
            )
            self._note_query(query_ids)
            compared = compare_complete_results(
                repaired,
                head_complete,
                entity_key_index=entity_key_index,
            )
            self.phase_timings["complete repaired-table validation"] = elapsed_ms(started)
            self._record_job("complete repaired-table validation")
            payload.update(
                candidateCount=self.confirmed_count,
                deletedRows=deleted,
                insertedRows=inserted,
                warehouseQueryIds=query_ids,
            )
            if compared.reason_code:
                payload.update(
                    status=REPAIR_FAILED,
                    reasonCode=compared.reason_code,
                    reason=compared.reason,
                    failurePhase="complete repaired-table validation",
                    duplicateEntityKeys=compared.duplicate_entity_keys or None,
                )
                if compared.reason_code not in {SCHEMA_MISALIGNED, GRAIN_VIOLATED}:
                    payload["missingRows"] = compared.missing_rows
                    payload["extraRows"] = compared.extra_rows
                    payload["mismatchedRows"] = compared.mismatched_rows
            elif compared.ok:
                payload.update(
                    status=REPAIR_SUCCEEDED,
                    missingRows=0,
                    extraRows=0,
                    mismatchedRows=0,
                    duplicateEntityKeys=0,
                )
            else:
                payload.update(
                    status=REPAIR_FAILED,
                    missingRows=compared.missing_rows,
                    extraRows=compared.extra_rows,
                    mismatchedRows=compared.mismatched_rows,
                    duplicateEntityKeys=compared.duplicate_entity_keys,
                    reason="repaired disposable table does not equal the complete head result",
                    failurePhase="complete repaired-table validation",
                )
            log_step(
                "complete repaired-table validation completed",
                duration_ms=self.phase_timings["complete repaired-table validation"],
                status="ok" if payload["status"] == REPAIR_SUCCEEDED else "FAILED",
            )
        except Exception as error:
            payload.update(
                status=REPAIR_FAILED,
                reasonCode=getattr(error, "code", None) or WAREHOUSE_EXECUTION_FAILED,
                reason=str(error)[:512],
                failurePhase=payload.get("failurePhase") or "disposable repair",
                warehouseQueryIds=query_ids,
            )
        self.repair_validation = payload
        return payload

    def cleanup(self) -> None:
        if self._cleaned:
            return
        log_step("cleanup started")
        started = time.perf_counter()
        try:
            registered = list(self.guard.registered) if self.guard is not None else []
            relations = list(dict.fromkeys([*registered, *self._created, self.relation]))
            failures: list[str] = []
            for relation in reversed(relations):
                if self.guard is not None:
                    try:
                        self.guard.register(relation)
                    except Exception:
                        continue
                dropped_ok = False
                for attempt in range(3):
                    try:
                        self.warehouse.execute(drop_relation_sql(relation))
                        dropped_ok = True
                        break
                    except Exception:
                        if attempt < 2:
                            time.sleep(0.05 * (attempt + 1))
                if not dropped_ok:
                    failures.append(relation)
            self._cleaned = True
            self.phase_timings["cleanup"] = elapsed_ms(started)
            self._record_job("cleanup")
            if failures:
                if self.repair_validation:
                    self.repair_validation["disposableResourcesCleaned"] = False
                    if self.repair_validation.get("status") == "SUCCEEDED":
                        self.repair_validation["status"] = "FAILED"
                        self.repair_validation["reasonCode"] = "CLEANUP_FAILED"
                        self.repair_validation["reason"] = (
                            f"cleanup failed for {len(failures)} disposable relation(s)"
                        )[:512]
                        self.repair_validation["failurePhase"] = "cleanup"
                log_step(
                    "cleanup completed",
                    duration_ms=self.phase_timings.get("cleanup") or elapsed_ms(started),
                    status="FAILED",
                )
                return
            if self.repair_validation:
                self.repair_validation["disposableResourcesCleaned"] = True
        except Exception as error:
            self._cleaned = True
            if self.repair_validation:
                self.repair_validation["disposableResourcesCleaned"] = False
                if self.repair_validation.get("status") == "SUCCEEDED":
                    self.repair_validation["status"] = "FAILED"
                    self.repair_validation["reasonCode"] = "CLEANUP_FAILED"
                    self.repair_validation["reason"] = str(error)[:512]
                    self.repair_validation["failurePhase"] = "cleanup"
            log_step(
                "cleanup completed",
                duration_ms=elapsed_ms(started),
                status=failure_status(error),
            )
            return
        log_step("cleanup completed", duration_ms=self.phase_timings.get("cleanup") or elapsed_ms(started), status="ok")


def open_isolated_run(
    warehouse: WarehouseAdapter,
    *,
    run_id: str,
    entity_key: str,
    model_database: str | None,
    model_schema: str | None,
    model_relation: str | None = None,
    snapshot: Any | None = None,
) -> IsolatedRun:
    if model_relation:
        assert_not_prod(relation=model_relation)
    database, schema = isolated_location(
        model_database=model_database,
        model_schema=model_schema,
        dialect=warehouse.dialect,
    )
    relation = affected_keys_relation(
        run_id,
        database=database,
        schema=schema,
        dialect=warehouse.dialect,
    )
    return IsolatedRun(
        warehouse=warehouse,
        relation=relation,
        database=database,
        schema=schema,
        run_id=run_id,
        entity_key=entity_key,
        snapshot=snapshot,
    )


def merge_unique_keys(*groups: Iterable[str]) -> list[str]:
    unique: dict[str, str] = {}
    for group in groups:
        for value in group:
            text = str(value).strip()
            if text:
                unique.setdefault(text, text)
    return list(unique.values())
