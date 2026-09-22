"""Fail-closed mutation guard for disposable Frontier work tables.

DML is allowed only against relations this invocation created, registered,
and named as invocation-owned FRONTIER_* work tables. Customer dbt models,
marts, sources, and production relations are never valid DML targets.
A name that merely starts with FRONTIER_ is not sufficient.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.config import ConfigError
from frontier.warehouse import split_relation_parts

READ_ONLY = "SELECT"
CREATE_SCHEMA = "CREATE_SCHEMA"
CREATE_TABLE = "CREATE_TABLE"
CREATE_TEMPORARY_TABLE = "CREATE_TEMPORARY_TABLE"
CREATE_TABLE_AS_SELECT = "CREATE_TABLE_AS_SELECT"
CREATE_OR_REPLACE = "CREATE_OR_REPLACE"
INSERT = "INSERT"
DELETE = "DELETE"
UPDATE = "UPDATE"
MERGE = "MERGE"
TRUNCATE = "TRUNCATE"
ALTER = "ALTER"
DROP_TABLE = "DROP_TABLE"
DROP = "DROP"
UNKNOWN = "UNKNOWN"

ALLOWED_DISPOSABLE_OPERATIONS = frozenset(
    {
        CREATE_TABLE,
        CREATE_TEMPORARY_TABLE,
        CREATE_TABLE_AS_SELECT,
        DELETE,
        INSERT,
        DROP_TABLE,
    }
)

FORBIDDEN_NON_DISPOSABLE = frozenset(
    {
        INSERT,
        UPDATE,
        DELETE,
        MERGE,
        TRUNCATE,
        ALTER,
        DROP,
        DROP_TABLE,
        CREATE_OR_REPLACE,
        CREATE_TABLE,
        CREATE_TEMPORARY_TABLE,
        CREATE_TABLE_AS_SELECT,
    }
)

MUTATION_REJECTED = "MUTATION_REJECTED"


def canonicalize_relation(relation: str) -> str:
    catalog, schema, table = split_relation_parts(relation)
    parts = [part.strip().strip('"').strip("`").upper() for part in (catalog, schema, table) if part]
    return ".".join(parts)


def relation_table_name(relation: str) -> str:
    return canonicalize_relation(relation).split(".")[-1]


@dataclass(frozen=True)
class ParsedMutation:
    operation: str
    target: str | None
    temporary: bool = False
    as_select: bool = False
    or_replace: bool = False


@dataclass
class DisposableResource:
    relation: str
    invocation_id: str
    kind: str
    created: bool = False
    dropped: bool = False


class MutationError(ConfigError):
    def __init__(self, message: str, *, code: str = MUTATION_REJECTED):
        self.code = code
        super().__init__(message)


def parse_mutation(sql: str, *, dialect: str = "snowflake") -> ParsedMutation | None:
    """Return the mutating operation and canonical target, or None for reads."""
    text = (sql or "").strip().rstrip(";")
    if not text:
        return None
    lowered = " ".join(text.lower().split())
    if lowered.startswith("create schema"):
        target = text.split(None, 5)[-1] if " exists " in lowered else text.split(None, 2)[-1]
        return ParsedMutation(CREATE_SCHEMA, canonicalize_relation(target.strip().strip(";")))
    try:
        statements = [item for item in sqlglot.parse(text, dialect=dialect) if item is not None]
    except SqlglotError:
        return _parse_mutation_fallback(text)
    if not statements:
        return None
    if len(statements) != 1:
        raise MutationError("multiple SQL statements are not permitted")
    return _mutation_from_expression(statements[0], fallback=text)


def _table_name(node: exp.Expression | None) -> str | None:
    if node is None:
        return None
    table = node if isinstance(node, exp.Table) else node.find(exp.Table)
    if table is None:
        if isinstance(node, exp.Schema):
            return _table_name(node.this)
        ident = str(getattr(node, "name", None) or node.sql() or "").strip()
        return canonicalize_relation(ident) if ident else None
    catalog = str(table.args.get("catalog") or table.catalog or "")
    db = str(table.args.get("db") or table.db or "")
    name = str(table.name or "")
    parts = [part for part in (catalog, db, name) if part]
    return canonicalize_relation(".".join(parts)) if parts else None


def _mutation_from_expression(root: exp.Expression, *, fallback: str) -> ParsedMutation | None:
    if isinstance(root, exp.Select) or isinstance(root, exp.Union) or isinstance(root, exp.With):
        if root.find(exp.Insert, exp.Delete, exp.Update, exp.Merge, exp.Drop, exp.Create, exp.Command):
            raise MutationError("mutating statements may not be nested in a read")
        return None
    if isinstance(root, exp.Insert):
        return ParsedMutation(INSERT, _table_name(root.this))
    if isinstance(root, exp.Delete):
        return ParsedMutation(DELETE, _table_name(root.this))
    if isinstance(root, exp.Update):
        return ParsedMutation(UPDATE, _table_name(root.this))
    if isinstance(root, exp.Merge):
        return ParsedMutation(MERGE, _table_name(root.this))
    if isinstance(root, exp.Drop):
        kind = str(root.args.get("kind") or "TABLE").upper()
        op = DROP_TABLE if kind == "TABLE" else DROP
        return ParsedMutation(op, _table_name(root.this))
    if isinstance(root, exp.Create):
        kind = str(root.args.get("kind") or "TABLE").upper()
        if kind == "SCHEMA":
            return ParsedMutation(CREATE_SCHEMA, _table_name(root.this))
        temporary = bool(root.args.get("temporary"))
        or_replace = bool(root.args.get("replace"))
        as_select = root.expression is not None and isinstance(root.expression, (exp.Select, exp.Union, exp.With))
        target = _table_name(root.this)
        if kind != "TABLE":
            return ParsedMutation(CREATE_OR_REPLACE if or_replace else UNKNOWN, target, or_replace=or_replace)
        if temporary:
            return ParsedMutation(CREATE_TEMPORARY_TABLE, target, temporary=True, as_select=as_select)
        if as_select and not or_replace:
            return ParsedMutation(CREATE_TABLE_AS_SELECT, target, as_select=True)
        if as_select and or_replace:
            # Dialect form of CTAS on a disposable work table; still CREATE OR REPLACE
            # until the guard rewrites it to CREATE_TABLE_AS_SELECT for owned targets.
            return ParsedMutation(
                CREATE_TABLE_AS_SELECT,
                target,
                as_select=True,
                or_replace=True,
            )
        if or_replace:
            return ParsedMutation(CREATE_OR_REPLACE, target, or_replace=True)
        return ParsedMutation(CREATE_TABLE, target)
    if isinstance(root, exp.Command):
        return _parse_mutation_fallback(fallback)
    if isinstance(root, exp.Alter):
        return ParsedMutation(ALTER, _table_name(root.this))
    return _parse_mutation_fallback(fallback)


def _parse_mutation_fallback(sql: str) -> ParsedMutation | None:
    lowered = " ".join(sql.lower().split())
    if lowered.startswith(("select ", "with ")):
        return None
    if lowered.startswith("truncate"):
        target = sql.split(None, 2)[-1] if " table " in lowered else sql.split(None, 1)[-1]
        return ParsedMutation(TRUNCATE, canonicalize_relation(target))
    if lowered.startswith("alter "):
        tokens = sql.split()
        target = tokens[2] if len(tokens) > 2 else tokens[-1]
        return ParsedMutation(ALTER, canonicalize_relation(target))
    if lowered.startswith("merge "):
        return ParsedMutation(MERGE, canonicalize_relation(sql.split()[2] if len(sql.split()) > 2 else sql))
    return ParsedMutation(UNKNOWN, None)


@dataclass
class MutationGuard:
    invocation_id: str
    database: str
    schema: str
    allowed_relations: set[str]
    isolated_schema: str
    _registry: dict[str, DisposableResource] = field(default_factory=dict)

    @property
    def registered(self) -> tuple[str, ...]:
        return tuple(item.relation for item in self._registry.values() if not item.dropped)

    def register(self, relation: str, *, kind: str = "work") -> str:
        canonical = canonicalize_relation(relation)
        if canonical not in self.allowed_relations:
            raise MutationError(
                f"relation {relation} is not an invocation-owned disposable Frontier table"
            )
        current = self._registry.get(canonical)
        if current is None:
            self._registry[canonical] = DisposableResource(
                relation=canonical,
                invocation_id=self.invocation_id,
                kind=kind,
            )
        return canonical

    def mark_created(self, relation: str) -> None:
        canonical = self.register(relation)
        self._registry[canonical].created = True

    def mark_dropped(self, relation: str) -> None:
        canonical = canonicalize_relation(relation)
        resource = self._registry.get(canonical)
        if resource is not None:
            resource.dropped = True

    def is_registered(self, relation: str) -> bool:
        canonical = canonicalize_relation(relation)
        resource = self._registry.get(canonical)
        return bool(resource and resource.created and not resource.dropped)

    def assert_sql_allowed(self, sql: str, *, dialect: str = "snowflake") -> ParsedMutation | None:
        parsed = parse_mutation(sql, dialect=dialect)
        if parsed is None:
            return None
        self.assert_allowed(parsed.operation, parsed.target, parsed=parsed)
        return parsed

    def assert_allowed(
        self,
        operation: str,
        target: str | None,
        *,
        parsed: ParsedMutation | None = None,
    ) -> None:
        if operation == CREATE_SCHEMA:
            if not target:
                raise MutationError("CREATE SCHEMA requires a target")
            expected = canonicalize_relation(f"{self.database}.{self.schema}")
            if canonicalize_relation(target) not in {expected, canonicalize_relation(self.schema)}:
                raise MutationError("CREATE SCHEMA is only allowed for the isolated Frontier work schema")
            return
        if operation == READ_ONLY:
            return
        if not target:
            raise MutationError(f"{operation} is not permitted without a resolved target relation")
        canonical = canonicalize_relation(target)
        owned = canonical in self.allowed_relations
        creates = operation in {
            CREATE_TABLE,
            CREATE_TEMPORARY_TABLE,
            CREATE_TABLE_AS_SELECT,
            CREATE_OR_REPLACE,
        }
        if creates and owned:
            if operation == CREATE_OR_REPLACE:
                raise MutationError(
                    "CREATE OR REPLACE is not permitted; disposable tables use CREATE TABLE AS SELECT"
                )
            if operation not in ALLOWED_DISPOSABLE_OPERATIONS:
                raise MutationError(f"{operation} is not permitted on disposable Frontier tables")
            self.register(canonical, kind="create")
            return
        if operation in ALLOWED_DISPOSABLE_OPERATIONS and owned and self.is_registered(canonical):
            return
        if operation == DROP_TABLE and owned and canonical in self._registry:
            return
        if operation in FORBIDDEN_NON_DISPOSABLE or operation in ALLOWED_DISPOSABLE_OPERATIONS or operation == UNKNOWN:
            raise MutationError(
                f"{operation} against {target} is not permitted; "
                "DML is allowed only on registered disposable Frontier work tables for this invocation"
            )
        raise MutationError(f"{operation} against {target} is not permitted")


class GuardedWarehouse:
    """Adapter proxy that consults MutationGuard before every execute."""

    def __init__(self, inner: Any, guard: MutationGuard):
        self._inner = inner
        self._guard = guard

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        parsed = self._guard.assert_sql_allowed(sql, dialect=getattr(self._inner, "dialect", "snowflake"))
        rows = self._inner.execute(sql)
        if parsed is not None and parsed.target:
            if parsed.operation in {
                CREATE_TABLE,
                CREATE_TEMPORARY_TABLE,
                CREATE_TABLE_AS_SELECT,
            }:
                self._guard.mark_created(parsed.target)
            elif parsed.operation == DROP_TABLE:
                self._guard.mark_dropped(parsed.target)
        return rows

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def invocation_owned_relations(
    *,
    run_id: str,
    database: str,
    schema: str,
    keys_relation: str,
    dialect: str = "snowflake",
) -> set[str]:
    from frontier.execute import isolated_table_name, qualify_relation, targeted_phase_relation

    owned = {canonicalize_relation(keys_relation)}
    owned.add(canonicalize_relation(targeted_phase_relation(keys_relation, "base")))
    owned.add(canonicalize_relation(targeted_phase_relation(keys_relation, "head")))
    for suffix in ("MART_COPY", "HEAD_REF"):
        owned.add(
            canonicalize_relation(
                qualify_relation(
                    database,
                    schema,
                    isolated_table_name(run_id, suffix),
                    dialect=dialect,
                )
            )
        )
    return owned


def build_guard(
    *,
    run_id: str,
    database: str,
    schema: str,
    keys_relation: str,
    dialect: str = "snowflake",
) -> MutationGuard:
    return MutationGuard(
        invocation_id=run_id,
        database=database,
        schema=schema,
        isolated_schema=canonicalize_relation(f"{database}.{schema}"),
        allowed_relations=invocation_owned_relations(
            run_id=run_id,
            database=database,
            schema=schema,
            keys_relation=keys_relation,
            dialect=dialect,
        ),
    )
