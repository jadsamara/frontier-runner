from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.config import ConfigError
from frontier.warehouse import WarehouseAdapter, sql_literal, split_relation_parts

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UNSAFE_TOKEN = re.compile(r"[^A-Za-z0-9]+")
_PROD_NAMES = frozenset({"DBT_PROD", "PROD", "PRODUCTION"})
_DEFAULT_SCHEMA = "DBT_CI"
_TABLE_PREFIX = "FRONTIER_"
_AFFECTED_SUFFIX = "AFFECTED_KEYS"
_SNOWFLAKE_NAME_LIMIT = 255

ORIGIN_EVENT = "event"
ORIGIN_SQL_CHANGE = "sql_change"
SQL_CHANGE_REASON = "SQL change candidate"

HANDWRITTEN_FRONTIER_MODELS = (
    "frontier_affected_customers",
    "frontier_customer_orders_target",
    "frontier_customer_orders_target_after",
    "frontier_customer_summary_target",
    "frontier_customer_summary_target_after",
    "customer_summary_repaired",
)


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


def isolated_location(*, model_database: str | None, model_schema: str | None) -> tuple[str, str]:
    database = (
        os.environ.get("FRONTIER_WAREHOUSE_DATABASE") or model_database or ""
    ).strip()
    schema = (os.environ.get("FRONTIER_WAREHOUSE_SCHEMA") or _DEFAULT_SCHEMA).strip()
    if not database:
        raise ConfigError("Frontier isolated execution requires a warehouse database")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ConfigError("warehouse database is not a safe identifier")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ConfigError("warehouse schema is not a safe identifier")
    assert_not_prod(database=database, schema=schema)
    assert_not_prod(database=model_database, schema=model_schema)
    return database, schema


def qualify_relation(database: str, schema: str, table: str) -> str:
    for part in (database, schema, table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part):
            raise ConfigError(f"unsafe warehouse identifier: {part}")
    return f"{database}.{schema}.{table}"


def affected_keys_relation(run_id: str, *, database: str, schema: str) -> str:
    return qualify_relation(database, schema, isolated_table_name(run_id, _AFFECTED_SUFFIX))


def create_schema_sql(database: str, schema: str) -> str:
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


def create_affected_keys_sql(
    relation: str,
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
        f"create or replace table {relation} as "
        f"select distinct {entity_key}, origin from ({inner}) as frontier_union_keys "
        f"where {entity_key} is not null"
    )


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


def drop_relation_sql(relation: str) -> str:
    return f"drop table if exists {relation}"


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
    table = from_.find(exp.Table)
    if table is None:
        return ""
    return str(table.alias or table.name or "")


def _select_can_bind_entity_key(select: exp.Select, entity_key: str) -> bool:
    from_ = select.args.get("from_")
    if from_ is None or from_.find(exp.Table) is None:
        return False
    target = entity_key.lower()
    if select.find(exp.Star):
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


def _inject_keys_join(select: exp.Select, entity_key: str, affected_relation: str) -> bool:
    if any(_is_keys_join(join) for join in (select.args.get("joins") or [])):
        return True
    if not _select_can_bind_entity_key(select, entity_key):
        return False
    alias = _from_table_alias(select)
    key_col = exp.column(entity_key, table=alias) if alias else exp.column(entity_key)
    keys_table = exp.alias_(exp.to_table(affected_relation), "frontier_keys", table=True)
    join = exp.Join(
        this=keys_table,
        kind="INNER",
        on=exp.EQ(this=key_col, expression=exp.column(entity_key, table="frontier_keys")),
    )
    existing = list(select.args.get("joins") or [])
    select.set("joins", [join, *existing])
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
    for key in ("bytes_scanned", "partitions_scanned", "rows_produced"):
        full_value = full.get(key)
        targeted_value = targeted.get(key)
        if isinstance(full_value, (int, float)) and isinstance(targeted_value, (int, float)):
            if targeted_value < full_value:
                return True
    return False


def sql_change_impact_queries(
    comparison: dict[str, Any] | None,
) -> tuple[tuple[str, ...], bool]:
    """Return impact SQL to execute in-warehouse, and whether SQL change requires it.

    When base and PR SQL differ, callers must execute these queries (or fail
    closed with FULL_REBUILD_REQUIRED). Missing or unsafe candidate SQL is
    never treated as an empty extra-key list.
    """
    if not comparison:
        return (), False
    added = list(comparison.get("added") or [])
    removed = list(comparison.get("removed") or [])
    modified = list(comparison.get("modified") or [])
    required = bool(added or removed or modified)
    if not required:
        return (), False
    if added or removed:
        return (), True
    if comparison.get("fullRebuildRequired") or comparison.get("narrowFrontierSafe") is False:
        return (), True
    queries: list[str] = []
    for row in modified:
        if row.get("unsafe") or row.get("impactStatus") == "FULL_REBUILD_REQUIRED":
            return (), True
        sql = str(row.get("candidateSql") or "").strip()
        if not sql:
            return (), True
        queries.append(sql)
    return tuple(queries), True


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
            if isinstance(body, exp.Select) and _inject_keys_join(body, entity_key, affected_relation):
                injected += 1
    if _inject_keys_join(outer, entity_key, affected_relation):
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
        f"select * from {before_relation} "
        f"where {entity_key} not in (select {entity_key} from {affected_relation}) "
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
        self.warehouse.execute(create_schema_sql(self.database, self.schema))
        sql = create_affected_keys_sql(
            self.relation,
            self.entity_key,
            values,
            sql_change_queries=sql_change_queries,
        )
        self.warehouse.execute(sql)
        self._created.append(self.relation)
        query_id = getattr(self.warehouse, "last_query_id", None)
        origin_keys = tuple(
            (str(row[0]), str(row[1]) if len(row) > 1 and row[1] is not None else ORIGIN_EVENT)
            for row in self.warehouse.execute(origin_keys_sql(self.relation, self.entity_key))
            if row and row[0] is not None
        )
        count_rows = self.warehouse.execute(origin_count_sql(self.relation, self.entity_key))
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
            sql = confirmed_changed_sql(
                before_sql=targeted_before,
                after_sql=targeted_after,
                entity_key=self.entity_key,
                dialect=dialect_name,
            )
            rows = self.warehouse.execute(sql)
            self.last_targeted_query_id = getattr(self.warehouse, "last_query_id", None)
        except (ConfigError, SqlglotError):
            return None
        return tuple(str(row[0]) for row in rows if row and row[0] is not None)

    def cleanup(self) -> None:
        if self._cleaned:
            return
        relations = list(dict.fromkeys([*self._created, self.relation]))
        for relation in reversed(relations):
            try:
                self.warehouse.execute(drop_relation_sql(relation))
            except Exception:
                continue
        self._cleaned = True


def open_isolated_run(
    warehouse: WarehouseAdapter,
    *,
    run_id: str,
    entity_key: str,
    model_database: str | None,
    model_schema: str | None,
    model_relation: str | None = None,
) -> IsolatedRun:
    if model_relation:
        assert_not_prod(relation=model_relation)
    database, schema = isolated_location(
        model_database=model_database,
        model_schema=model_schema,
    )
    relation = affected_keys_relation(run_id, database=database, schema=schema)
    return IsolatedRun(
        warehouse=warehouse,
        relation=relation,
        database=database,
        schema=schema,
        run_id=run_id,
        entity_key=entity_key,
    )


def merge_unique_keys(*groups: Iterable[str]) -> list[str]:
    unique: dict[str, str] = {}
    for group in groups:
        for value in group:
            text = str(value).strip()
            if text:
                unique.setdefault(text, text)
    return list(unique.values())
