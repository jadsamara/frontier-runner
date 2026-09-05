from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.config import ConfigError
from frontier.execute import discovery_sql_is_unrestricted
from frontier.snowflake_sql import (
    AGGREGATE_CHANGED,
    EXPRESSION_CHANGED,
    FILTER_CHANGED,
    GRAIN_POSSIBLY_CHANGED,
    GROUPING_CHANGED,
    JOIN_CHANGED,
    SOURCE_CHANGED,
    UNSUPPORTED,
    WINDOW_CHANGED,
    DIALECT,
    SqlChangeClassification,
    classify_sql_change,
)
from frontier.sql_fingerprint import sql_fingerprint
from frontier.warehouse import WarehouseAdapter

COMPILED = "COMPILED"
FULL_REBUILD_REQUIRED = "FULL_REBUILD_REQUIRED"

CANDIDATE_SET_EMPTY = "empty"
CANDIDATE_SET_NONEMPTY = "nonempty"
CANDIDATE_SET_NOT_EVALUATED = "not_evaluated"
CANDIDATE_SET_ANALYSIS_FAILED = "analysis_failed"

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_REBUILD_KINDS = frozenset(
    {
        GROUPING_CHANGED,
        GRAIN_POSSIBLY_CHANGED,
        WINDOW_CHANGED,
        SOURCE_CHANGED,
        UNSUPPORTED,
    }
)

_SUPPORTED_KINDS = frozenset(
    {
        FILTER_CHANGED,
        EXPRESSION_CHANGED,
        AGGREGATE_CHANGED,
        JOIN_CHANGED,
    }
)

_NONDET_TYPES = (
    exp.Rand,
    exp.Randn,
    exp.Randstr,
    exp.Uuid,
    exp.CurrentTimestamp,
    exp.CurrentDate,
    exp.CurrentTime,
    exp.CurrentUser,
    exp.CurrentRole,
    exp.CurrentSession,
    exp.CurrentWarehouse,
    exp.CurrentDatabase,
    exp.CurrentSchema,
)

_NONDET_NAMES = frozenset(
    {
        "RANDOM",
        "RAND",
        "RANDSTR",
        "UNIFORM",
        "NORMAL",
        "UUID",
        "UUID_STRING",
        "GENERATE_UNIQUE",
        "SEQ1",
        "SEQ2",
        "SEQ4",
        "SEQ8",
        "NOW",
        "GETDATE",
        "SYSDATE",
        "SYSTIMESTAMP",
        "CURRENT_TIMESTAMP",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_USER",
        "CURRENT_ROLE",
        "CURRENT_WAREHOUSE",
        "CURRENT_DATABASE",
        "CURRENT_SCHEMA",
        "CURRENT_SESSION",
    }
)

_DYNAMIC_FUNCS = frozenset({"IDENTIFIER", "TO_QUERY", "PARSE_JSON"})

# Outer query key in the named-select map. CTE names are lowercased aliases.
_OUTER = ""


@dataclass(frozen=True)
class ImpactCompileResult:
    status: str
    reasons: tuple[str, ...] = ()
    entity_key: str = ""
    candidate_sql: str | None = None
    parameterized_sql: str | None = None
    parameters: tuple[tuple[str, str], ...] = ()
    query_fingerprint: str | None = None
    candidate_set_state: str = CANDIDATE_SET_ANALYSIS_FAILED

    @property
    def candidates(self) -> tuple[str, ...] | None:
        # Failure is never an empty tuple. Empty is only a compiled no-op.
        if self.status != COMPILED:
            return None
        if self.candidate_set_state == CANDIDATE_SET_EMPTY:
            return ()
        return None

    def to_payload(self, *, include_sql: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "impactStatus": self.status,
            "candidateSetState": self.candidate_set_state,
        }
        if self.reasons:
            payload["impactReasons"] = list(self.reasons)
        if self.query_fingerprint:
            payload["queryFingerprint"] = self.query_fingerprint
        if include_sql and self.candidate_sql:
            payload["candidateSql"] = self.candidate_sql
            payload["parameterizedSql"] = self.parameterized_sql
            payload["parameters"] = [
                {"name": name, "value": value} for name, value in self.parameters
            ]
        return payload


@dataclass(frozen=True)
class ImpactEvalResult:
    status: str
    candidate_set_state: str
    keys: tuple[str, ...] | None
    reasons: tuple[str, ...] = ()

    @property
    def candidates(self) -> tuple[str, ...] | None:
        return self.keys


def _rebuild(*reasons: str, entity_key: str = "") -> ImpactCompileResult:
    unique = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return ImpactCompileResult(
        status=FULL_REBUILD_REQUIRED,
        reasons=unique or ("unsupported SQL change",),
        entity_key=entity_key,
        candidate_sql=None,
        parameterized_sql=None,
        parameters=(),
        query_fingerprint=None,
        candidate_set_state=CANDIDATE_SET_ANALYSIS_FAILED,
    )


def _render(node: exp.Expression | None) -> str:
    if node is None:
        return ""
    return node.sql(
        dialect=DIALECT,
        comments=False,
        pretty=False,
        normalize=True,
        normalize_functions="lower",
    ).strip()


def _and_parts(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    if isinstance(node, (exp.Where, exp.Having, exp.From)):
        return _and_parts(node.this)
    if isinstance(node, exp.And):
        return [*_and_parts(node.left), *_and_parts(node.right)]
    return [node]


def _is_distinct(left: exp.Expression, right: exp.Expression) -> exp.Expression:
    return exp.NullSafeNEQ(
        this=exp.Paren(this=left.copy()),
        expression=exp.Paren(this=right.copy()),
    )


def _or_all(nodes: list[exp.Expression]) -> exp.Expression:
    if not nodes:
        return exp.true()
    acc = nodes[0]
    for node in nodes[1:]:
        acc = exp.Or(this=acc, expression=node)
    return acc


def _unalias(node: exp.Expression) -> exp.Expression:
    return node.this if isinstance(node, exp.Alias) else node


def _column_name(node: exp.Expression | None) -> str:
    if isinstance(node, exp.Column):
        return str(node.name or "").lower()
    if isinstance(node, exp.Identifier):
        return str(node.this or "").lower()
    return ""


def _confirmed(name: str, confirmed_keys: frozenset[str]) -> bool:
    return name.lower() in confirmed_keys


@dataclass(frozen=True)
class _ParsedQuery:
    outer: exp.Select
    named: dict[str, exp.Select]
    cte_nodes: dict[str, exp.CTE]
    cte_order: tuple[str, ...]


def _parse_query(sql: str) -> tuple[_ParsedQuery | None, tuple[str, ...]]:
    text = (sql or "").strip()
    if not text:
        return None, ("empty SQL",)
    try:
        statements = sqlglot.parse(text, dialect=DIALECT)
    except SqlglotError as error:
        return None, (f"parse error: {error}",)
    expressions = [item for item in statements if item is not None]
    if len(expressions) != 1:
        return None, ("multiple statements",)
    root = expressions[0]
    if isinstance(root, (exp.Union, exp.Except, exp.Intersect)):
        return None, ("set operation",)
    outer = root if isinstance(root, exp.Select) else None
    if outer is None:
        return None, ("nested or missing select",)

    named: dict[str, exp.Select] = {_OUTER: outer}
    cte_nodes: dict[str, exp.CTE] = {}
    cte_order: list[str] = []
    with_ = outer.args.get("with_")
    if with_ is not None:
        if with_.args.get("recursive"):
            return None, ("RECURSIVE_CTE",)
        for cte in with_.expressions or []:
            if not isinstance(cte, exp.CTE):
                return None, ("nested or missing select",)
            name = str(cte.alias or "").lower()
            body = cte.this
            if not name or not isinstance(body, exp.Select):
                return None, ("nested or missing select",)
            if name in named:
                return None, ("nested or missing select",)
            named[name] = body
            cte_nodes[name] = cte
            cte_order.append(name)

    allowed = set(named.values())
    extras = [node for node in root.find_all(exp.Select) if node not in allowed]
    if extras:
        if any(_has_ancestor(node, exp.Exists) for node in extras):
            return None, ("unsupported correlated query",)
        return None, ("nested or missing select",)
    return (
        _ParsedQuery(
            outer=outer,
            named=named,
            cte_nodes=cte_nodes,
            cte_order=tuple(cte_order),
        ),
        (),
    )


def _cte_deps(select: exp.Select, named: dict[str, exp.Select]) -> set[str]:
    cte_names = {name for name in named if name}
    needed: set[str] = set()

    def visit(node: exp.Select) -> None:
        for table in node.find_all(exp.Table):
            name = str(table.name or "").lower()
            if name in cte_names and name not in needed:
                needed.add(name)
                visit(named[name])

    visit(select)
    return needed


def _attach_with(query: exp.Select, source: exp.Select, parsed: _ParsedQuery) -> exp.Select:
    needed = _cte_deps(source, parsed.named)
    if not needed:
        return query
    expressions = [
        parsed.cte_nodes[name].copy()
        for name in parsed.cte_order
        if name in needed
    ]
    if not expressions:
        return query
    query.set("with_", exp.With(expressions=expressions))
    return query


def _compile_filter_queries(
    base_q: _ParsedQuery,
    pr_q: _ParsedQuery,
    entity_key: str,
) -> tuple[list[exp.Select], tuple[str, ...]]:
    if set(base_q.named) != set(pr_q.named):
        return [], ("SOURCE_CHANGED",)
    queries: list[exp.Select] = []
    for name in base_q.named:
        base_sel = base_q.named[name]
        pr_sel = pr_q.named[name]
        old_where = _clause_predicate(base_sel.args.get("where"))
        new_where = _clause_predicate(pr_sel.args.get("where"))
        if _render(old_where) != _render(new_where):
            query = _build_select(pr_sel, entity_key, where=_is_distinct(old_where, new_where))
            queries.append(_attach_with(query, pr_sel, pr_q))
        old_having = _clause_predicate(base_sel.args.get("having"))
        new_having = _clause_predicate(pr_sel.args.get("having"))
        if _render(old_having) != _render(new_having):
            if pr_sel.args.get("group") is None and base_sel.args.get("group") is None:
                return [], ("unsupported aggregate change",)
            source = pr_sel if pr_sel.args.get("group") is not None else base_sel
            query = _build_select(
                source,
                entity_key,
                having=_is_distinct(old_having, new_having),
            )
            queries.append(_attach_with(query, source, pr_q))
    if not queries:
        return [], ("FILTER_CHANGED",)
    return queries, ()


def _has_ancestor(node: exp.Expression, kind: type[exp.Expression]) -> bool:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, kind):
            return True
        parent = parent.parent
    return False


def _func_name(node: exp.Expression) -> str:
    name = getattr(node, "sql_name", None)
    if callable(name):
        value = name()
        if value:
            return str(value).upper()
    key = getattr(node, "key", None)
    if key:
        return str(key).upper()
    named = getattr(node, "name", None)
    if named:
        return str(named).upper()
    return type(node).__name__.upper()


def _is_nondeterministic(node: exp.Expression) -> bool:
    if isinstance(node, _NONDET_TYPES):
        return True
    if isinstance(node, exp.Func) and _func_name(node) in _NONDET_NAMES:
        return True
    if isinstance(node, exp.Anonymous) and (node.name or "").upper() in _NONDET_NAMES:
        return True
    return False


def _table_is_dynamic(table: exp.Table) -> bool:
    this = table.this
    if isinstance(this, exp.Identifier):
        return False
    name = table.name
    if not name:
        return True
    return not isinstance(this, exp.Identifier)


def _is_correlated(select: exp.Select) -> bool:
    inner_aliases: set[str] = set()
    from_ = select.args.get("from_")
    if from_ is not None:
        for table in from_.find_all(exp.Table):
            alias = table.alias or table.name
            if alias:
                inner_aliases.add(str(alias).lower())
            if table.name:
                inner_aliases.add(str(table.name).lower())
    for join in select.args.get("joins") or []:
        for table in join.find_all(exp.Table):
            alias = table.alias or table.name
            if alias:
                inner_aliases.add(str(alias).lower())
            if table.name:
                inner_aliases.add(str(table.name).lower())
    for sub in select.find_all(exp.Subquery, exp.Exists, exp.Any, exp.All, exp.In):
        if isinstance(sub, exp.In) and not any(
            isinstance(item, (exp.Select, exp.Subquery)) for item in sub.expressions or []
        ):
            continue
        sub_aliases = {
            str(table.alias or table.name).lower()
            for table in sub.find_all(exp.Table)
            if table.alias or table.name
        }
        for column in sub.find_all(exp.Column):
            table = str(column.table).lower() if column.table else ""
            if table and table not in sub_aliases and table in inner_aliases:
                return True
    return False


def _window_is_global_or_unbounded(window: exp.Window) -> bool:
    partitions = window.args.get("partition_by") or []
    if not partitions:
        return True
    spec = window.args.get("spec")
    if spec is None:
        return False
    start = str(spec.args.get("start") or "").upper()
    end = str(spec.args.get("end") or "").upper()
    start_side = str(spec.args.get("start_side") or "").upper()
    end_side = str(spec.args.get("end_side") or "").upper()
    if start == "UNBOUNDED" and end == "UNBOUNDED":
        return True
    if start_side == "PRECEDING" and start == "UNBOUNDED" and end_side == "FOLLOWING" and end == "UNBOUNDED":
        return True
    return False


def _structural_rebuild_reasons(select: exp.Select) -> list[str]:
    reasons: list[str] = []
    for table in select.find_all(exp.Table):
        if _table_is_dynamic(table):
            reasons.append("dynamic SQL")
    for node in select.walk():
        if isinstance(node, exp.Anonymous) and (node.name or "").upper() in _DYNAMIC_FUNCS:
            reasons.append("dynamic SQL")
        if isinstance(node, (exp.Parameter, exp.Placeholder)) and isinstance(node.parent, exp.Table):
            reasons.append("dynamic SQL")
        if _is_nondeterministic(node):
            reasons.append("nondeterministic function")
    if _is_correlated(select):
        reasons.append("unsupported correlated query")
    for window in select.find_all(exp.Window):
        if _window_is_global_or_unbounded(window):
            reasons.append("unbounded or global window")
    if select.find(exp.Lateral, exp.Unnest):
        reasons.append("unsupported correlated query")
    return list(dict.fromkeys(reasons))


def _join_is_inner_or_left(join: exp.Join) -> bool:
    side = str(join.args.get("side") or "").upper()
    kind = str(join.args.get("kind") or "").upper()
    if kind in {"CROSS", "RIGHT", "FULL", "SEMI", "ANTI"}:
        return False
    if side in {"RIGHT", "FULL"}:
        return False
    if not join.args.get("on") and not join.args.get("using"):
        return False
    return side in {"", "LEFT"}


def _equijoin_confirmed(join: exp.Join, confirmed_keys: frozenset[str]) -> bool:
    using = join.args.get("using")
    if using is not None:
        names: list[str] = []
        for item in using if isinstance(using, list) else using.expressions or [using]:
            name = _column_name(item if not isinstance(item, exp.Identifier) else item)
            if isinstance(item, exp.Identifier):
                name = str(item.this or "").lower()
            if not name:
                return False
            names.append(name)
        return bool(names) and all(_confirmed(name, confirmed_keys) for name in names)
    on = join.args.get("on")
    if on is None:
        return False
    parts = _and_parts(on)
    if not parts:
        return False
    for part in parts:
        if not isinstance(part, exp.EQ):
            return False
        if not isinstance(part.left, exp.Column) or not isinstance(part.right, exp.Column):
            return False
        left = _column_name(part.left)
        right = _column_name(part.right)
        if not (_confirmed(left, confirmed_keys) or _confirmed(right, confirmed_keys)):
            return False
    return True


def _validate_joins(select: exp.Select, confirmed_keys: frozenset[str]) -> list[str]:
    reasons: list[str] = []
    for join in select.args.get("joins") or []:
        if not _join_is_inner_or_left(join):
            reasons.append("ambiguous many-to-many join")
            continue
        if not _equijoin_confirmed(join, confirmed_keys):
            reasons.append("ambiguous many-to-many join")
    return list(dict.fromkeys(reasons))


def _clause_predicate(node: exp.Expression | None) -> exp.Expression:
    if node is None:
        return exp.true()
    if isinstance(node, (exp.Where, exp.Having)):
        return node.this.copy() if node.this is not None else exp.true()
    return node.copy()


def _is_simple_expr(node: exp.Expression) -> bool:
    for child in node.walk():
        if isinstance(
            child,
            (
                exp.Subquery,
                exp.Exists,
                exp.Window,
                exp.Anonymous,
                exp.Parameter,
                exp.Placeholder,
                exp.Star,
            ),
        ):
            return False
        if isinstance(child, exp.AggFunc):
            return False
        if _is_nondeterministic(child):
            return False
    return True


def _projection_impacts(base: exp.Select, pr: exp.Select) -> list[exp.Expression] | None:
    base_exprs = [_unalias(item) for item in base.expressions or []]
    pr_exprs = [_unalias(item) for item in pr.expressions or []]
    if len(base_exprs) != len(pr_exprs):
        return None
    impacts: list[exp.Expression] = []
    for old, new in zip(base_exprs, pr_exprs):
        if _render(old) == _render(new):
            continue
        if isinstance(old, exp.AggFunc) or isinstance(new, exp.AggFunc):
            continue
        if not _is_simple_expr(old) or not _is_simple_expr(new):
            return None
        impacts.append(_is_distinct(old, new))
    return impacts


def _case_impacts(base: exp.Select, pr: exp.Select) -> list[exp.Expression] | None:
    base_cases = list(base.find_all(exp.Case))
    pr_cases = list(pr.find_all(exp.Case))
    if not base_cases and not pr_cases:
        return []
    if len(base_cases) != len(pr_cases):
        return None
    impacts: list[exp.Expression] = []
    for old, new in zip(base_cases, pr_cases):
        if _render(old) == _render(new):
            continue
        if not _is_simple_expr(old) or not _is_simple_expr(new):
            return None
        impacts.append(_is_distinct(old, new))
    return impacts


def _agg_inner(node: exp.AggFunc) -> exp.Expression | None:
    if node.this is not None:
        return node.this
    exprs = node.expressions or []
    if len(exprs) == 1:
        return exprs[0]
    return None


def _aggregate_impacts(
    base: exp.Select,
    pr: exp.Select,
) -> tuple[list[exp.Expression], list[exp.Expression]] | None:
    base_aggs = list(base.find_all(exp.AggFunc))
    pr_aggs = list(pr.find_all(exp.AggFunc))
    if len(base_aggs) != len(pr_aggs) or not base_aggs:
        return None
    row_level: list[exp.Expression] = []
    having_level: list[exp.Expression] = []
    for old, new in zip(base_aggs, pr_aggs):
        if _render(old) == _render(new):
            continue
        if type(old) is type(new):
            old_inner = _agg_inner(old)
            new_inner = _agg_inner(new)
            if (
                old_inner is not None
                and new_inner is not None
                and not isinstance(old_inner, exp.Star)
                and not isinstance(new_inner, exp.Star)
                and _is_simple_expr(old_inner)
                and _is_simple_expr(new_inner)
            ):
                row_level.append(_is_distinct(old_inner, new_inner))
                continue
        having_level.append(_is_distinct(old, new))
    return row_level, having_level


def _join_impacts(
    base: exp.Select,
    pr: exp.Select,
    confirmed_keys: frozenset[str],
) -> list[exp.Expression] | None:
    base_joins = list(base.args.get("joins") or [])
    pr_joins = list(pr.args.get("joins") or [])
    if len(base_joins) != len(pr_joins):
        return None
    impacts: list[exp.Expression] = []
    for old, new in zip(base_joins, pr_joins):
        if not _join_is_inner_or_left(old) or not _join_is_inner_or_left(new):
            return None
        if not _equijoin_confirmed(old, confirmed_keys) or not _equijoin_confirmed(new, confirmed_keys):
            return None
        old_on = old.args.get("on")
        new_on = new.args.get("on")
        old_using = old.args.get("using")
        new_using = new.args.get("using")
        if _render(old_on) != _render(new_on) or _render(old_using) != _render(new_using):
            if old_on is None or new_on is None:
                return None
            if not _is_simple_expr(old_on) or not _is_simple_expr(new_on):
                return None
            impacts.append(_is_distinct(old_on, new_on))
            continue
        old_side = str(old.args.get("side") or "").upper()
        new_side = str(new.args.get("side") or "").upper()
        if old_side == new_side:
            continue
        right = new.this
        key_col: exp.Expression | None = None
        on = new.args.get("on")
        if isinstance(on, exp.EQ):
            for side_col in (on.left, on.right):
                if isinstance(side_col, exp.Column) and str(side_col.table).lower() == str(
                    getattr(right, "alias", None) or getattr(right, "name", "")
                ).lower():
                    key_col = side_col
                    break
            if key_col is None and isinstance(on.right, exp.Column):
                key_col = on.right
        if key_col is None:
            return None
        impacts.append(exp.Is(this=key_col.copy(), expression=exp.Null()))
    return impacts


def _entity_column(select: exp.Select, entity_key: str) -> exp.Column:
    from_table = ""
    from_ = select.args.get("from_")
    if from_ is not None:
        table = from_.find(exp.Table)
        if table is not None:
            from_table = str(table.alias or table.name or "").lower()
    preferred: exp.Column | None = None
    for column in select.find_all(exp.Column):
        if str(column.name or "").lower() != entity_key.lower():
            continue
        table = str(column.table).lower() if column.table else ""
        if from_table and table == from_table:
            return column.copy()
        if preferred is None:
            preferred = column.copy()
    if preferred is not None:
        return preferred
    for item in select.expressions or []:
        if isinstance(item, exp.Alias) and str(item.alias).lower() == entity_key.lower():
            inner = _unalias(item)
            if isinstance(inner, exp.Column):
                return inner.copy()
    if from_table:
        return exp.column(entity_key, table=from_table)
    return exp.column(entity_key)


def _parameterize(node: exp.Expression) -> tuple[exp.Expression, tuple[tuple[str, str], ...]]:
    copied = node.copy()
    params: list[tuple[str, str]] = []
    for index, literal in enumerate(list(copied.find_all(exp.Literal))):
        name = f"p{index}"
        params.append((name, str(literal.this)))
        literal.replace(exp.Placeholder(this=name))
    return copied, tuple(params)


def _build_select(
    source: exp.Select,
    entity_key: str,
    *,
    where: exp.Expression | None = None,
    having: exp.Expression | None = None,
) -> exp.Select:
    query = exp.Select()
    query.set("distinct", exp.Distinct())
    query.set("expressions", [_entity_column(source, entity_key)])
    from_ = source.args.get("from_")
    if from_ is not None:
        query.set("from_", from_.copy())
    joins = source.args.get("joins")
    if joins:
        query.set("joins", [join.copy() for join in joins])
    if where is not None:
        query.set("where", exp.Where(this=where))
    if having is not None:
        group = source.args.get("group")
        if group is not None:
            query.set("group", group.copy())
        query.set("having", exp.Having(this=having))
    return query


def compile_impact_query(
    base_sql: str,
    pr_sql: str,
    *,
    entity_key: str,
    confirmed_keys: Iterable[str] = (),
    classification: SqlChangeClassification | None = None,
) -> ImpactCompileResult:
    """Compile old/new Snowflake SQL into a candidate-key query.

    Generated SQL comes only from sqlglot AST nodes. Unsupported changes
    return FULL_REBUILD_REQUIRED and never an empty candidate tuple.
    """
    if not entity_key or not _IDENT.fullmatch(entity_key):
        return _rebuild("entity key is not a confirmed identifier", entity_key=entity_key or "")
    confirmed = frozenset(
        key.lower()
        for key in [entity_key, *confirmed_keys]
        if key and _IDENT.fullmatch(key)
    )
    if entity_key.lower() not in confirmed:
        return _rebuild("entity key is not a confirmed identifier", entity_key=entity_key)

    change = classification or classify_sql_change(base_sql, pr_sql)
    if not change.kinds:
        return ImpactCompileResult(
            status=COMPILED,
            reasons=(),
            entity_key=entity_key,
            candidate_sql=None,
            parameterized_sql=None,
            parameters=(),
            query_fingerprint=None,
            candidate_set_state=CANDIDATE_SET_EMPTY,
        )

    base_q, base_err = _parse_query(base_sql)
    pr_q, pr_err = _parse_query(pr_sql)
    rebuild_reasons: list[str] = []
    rebuild_reasons.extend(base_err)
    rebuild_reasons.extend(pr_err)
    rebuild_kinds = [kind for kind in change.kinds if kind in _REBUILD_KINDS]
    if GRAIN_POSSIBLY_CHANGED in rebuild_kinds or GROUPING_CHANGED in rebuild_kinds:
        rebuild_reasons.append("grain change")
    rebuild_reasons.extend(rebuild_kinds)
    if change.unsupported_reasons:
        rebuild_reasons.extend(change.unsupported_reasons)
    if UNSUPPORTED in rebuild_kinds:
        rebuild_reasons.append("unsupported SQL")
    unsupported_kinds = [kind for kind in change.kinds if kind not in _SUPPORTED_KINDS and kind not in _REBUILD_KINDS]
    rebuild_reasons.extend(unsupported_kinds)
    for parsed in (base_q, pr_q):
        if parsed is None:
            continue
        for select in parsed.named.values():
            rebuild_reasons.extend(_structural_rebuild_reasons(select))
            rebuild_reasons.extend(_validate_joins(select, confirmed))
    if rebuild_reasons:
        return _rebuild(*rebuild_reasons, entity_key=entity_key)
    if base_q is None or pr_q is None:
        return _rebuild("nested or missing select", entity_key=entity_key)
    base_select = base_q.outer
    pr_select = pr_q.outer

    queries: list[exp.Select] = []
    if FILTER_CHANGED in change.kinds:
        filter_queries, filter_err = _compile_filter_queries(base_q, pr_q, entity_key)
        if filter_err:
            return _rebuild(*filter_err, entity_key=entity_key)
        queries.extend(filter_queries)

    where_preds: list[exp.Expression] = []
    having_preds: list[exp.Expression] = []

    if EXPRESSION_CHANGED in change.kinds:
        cases = _case_impacts(base_select, pr_select)
        projs = _projection_impacts(base_select, pr_select)
        if cases is None or projs is None:
            return _rebuild("unsupported expression change", entity_key=entity_key)
        expr_preds = [*cases, *projs]
        if expr_preds:
            where_preds.extend(expr_preds)
        elif AGGREGATE_CHANGED not in change.kinds:
            return _rebuild("unsupported expression change", entity_key=entity_key)

    if AGGREGATE_CHANGED in change.kinds:
        aggs = _aggregate_impacts(base_select, pr_select)
        if aggs is None:
            return _rebuild("unsupported aggregate change", entity_key=entity_key)
        row_aggs, having_aggs = aggs
        where_preds.extend(row_aggs)
        having_preds.extend(having_aggs)
        if not row_aggs and not having_aggs:
            return _rebuild("unsupported aggregate change", entity_key=entity_key)

    if JOIN_CHANGED in change.kinds:
        joins = _join_impacts(base_select, pr_select, confirmed)
        if joins is None:
            return _rebuild("ambiguous many-to-many join", entity_key=entity_key)
        if not joins:
            return _rebuild("unsupported join change", entity_key=entity_key)
        where_preds.extend(joins)

    if where_preds:
        queries.append(
            _attach_with(
                _build_select(pr_select, entity_key, where=_or_all(where_preds)),
                pr_select,
                pr_q,
            )
        )
    if having_preds:
        if pr_select.args.get("group") is None and base_select.args.get("group") is None:
            return _rebuild("unsupported aggregate change", entity_key=entity_key)
        source = pr_select if pr_select.args.get("group") is not None else base_select
        queries.append(
            _attach_with(
                _build_select(source, entity_key, having=_or_all(having_preds)),
                source,
                pr_q,
            )
        )
    if not queries:
        return _rebuild("no supported impact predicate", entity_key=entity_key)

    tree: exp.Expression = queries[0]
    for extra in queries[1:]:
        tree = exp.union(tree, extra, distinct=True)

    candidate_sql = _render(tree)
    if not discovery_sql_is_unrestricted(candidate_sql):
        return _rebuild(
            "candidate discovery joined an affected-key relation",
            entity_key=entity_key,
        )
    parameterized_tree, params = _parameterize(tree)
    parameterized_sql = _render(parameterized_tree)
    return ImpactCompileResult(
        status=COMPILED,
        reasons=tuple(change.kinds),
        entity_key=entity_key,
        candidate_sql=candidate_sql,
        parameterized_sql=parameterized_sql,
        parameters=params,
        query_fingerprint=sql_fingerprint(candidate_sql, dialect=DIALECT),
        candidate_set_state=CANDIDATE_SET_NOT_EVALUATED,
    )


def discovery_counts_sql(
    candidate_sql: str,
    entity_key: str,
    *,
    dialect: str = DIALECT,
) -> str:
    """Count source rows and distinct entity keys in one scan of the impact SQL.

    Does not join frontier_affected_customers or an isolated keys table.
    """
    if not entity_key or not _IDENT.fullmatch(entity_key):
        raise ConfigError("entity key is not a confirmed identifier")
    if not discovery_sql_is_unrestricted(candidate_sql):
        raise ConfigError("candidate discovery referenced an affected-key relation")
    parsed = sqlglot.parse_one(candidate_sql, dialect=dialect)
    if parsed is None:
        raise SqlglotError("impact SQL could not be parsed")
    tree = parsed.copy()

    def strip_distinct(node: exp.Expression) -> None:
        if isinstance(node, exp.Select):
            node.set("distinct", None)
        if isinstance(node, exp.Union):
            node.set("distinct", False)
            if node.this is not None:
                strip_distinct(node.this)
            if node.expression is not None:
                strip_distinct(node.expression)

    strip_distinct(tree)
    inner = _render(tree)
    sql = (
        "select count(*) as changed_source_row_count, "
        f"count(distinct {entity_key}) as sql_change_candidate_count "
        f"from ({inner}) as frontier_discovery"
    )
    if not discovery_sql_is_unrestricted(sql):
        raise ConfigError("candidate discovery referenced an affected-key relation")
    return sql


def evaluate_discovery_counts(
    candidate_sql: str,
    entity_key: str,
    warehouse: WarehouseAdapter,
) -> tuple[int, int]:
    """Return (changed_source_row_count, distinct_candidate_count)."""
    sql = discovery_counts_sql(candidate_sql, entity_key, dialect=warehouse.dialect)
    rows = warehouse.execute(sql)
    if not rows or rows[0][0] is None:
        return 0, 0
    row = rows[0]
    changed = int(row[0] or 0)
    candidates = int(row[1] or 0) if len(row) > 1 else 0
    return changed, candidates


def source_row_count_sql(candidate_sql: str, *, dialect: str = DIALECT) -> str:
    """Count matching source rows, not distinct entity keys."""
    parsed = sqlglot.parse_one(candidate_sql, dialect=dialect)
    if parsed is None:
        raise SqlglotError("impact SQL could not be parsed")
    tree = parsed.copy()

    def strip_distinct(node: exp.Expression) -> None:
        if isinstance(node, exp.Select):
            node.set("distinct", None)
            node.set("expressions", [exp.Literal.number(1)])
        if isinstance(node, exp.Union):
            node.set("distinct", False)
            if node.this is not None:
                strip_distinct(node.this)
            if node.expression is not None:
                strip_distinct(node.expression)

    strip_distinct(tree)
    inner = _render(tree)
    return (
        "select count(*) as changed_source_row_count "
        f"from ({inner}) as frontier_changed_source_rows"
    )


def evaluate_impact_query(
    compiled: ImpactCompileResult,
    warehouse: WarehouseAdapter,
) -> ImpactEvalResult:
    """Run a compiled candidate query. Empty results are not analysis failure."""
    if compiled.status != COMPILED:
        return ImpactEvalResult(
            status=FULL_REBUILD_REQUIRED,
            candidate_set_state=CANDIDATE_SET_ANALYSIS_FAILED,
            keys=None,
            reasons=compiled.reasons,
        )
    if not compiled.candidate_sql:
        return ImpactEvalResult(
            status=COMPILED,
            candidate_set_state=CANDIDATE_SET_EMPTY,
            keys=(),
            reasons=compiled.reasons,
        )
    rows = warehouse.execute(compiled.candidate_sql)
    keys = tuple(str(row[0]) for row in rows if row and row[0] is not None)
    return ImpactEvalResult(
        status=COMPILED,
        candidate_set_state=CANDIDATE_SET_EMPTY if not keys else CANDIDATE_SET_NONEMPTY,
        keys=keys,
        reasons=compiled.reasons,
    )
