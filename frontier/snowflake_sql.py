from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

DIALECT = "snowflake"

FILTER_CHANGED = "FILTER_CHANGED"
JOIN_CHANGED = "JOIN_CHANGED"
EXPRESSION_CHANGED = "EXPRESSION_CHANGED"
AGGREGATE_CHANGED = "AGGREGATE_CHANGED"
GROUPING_CHANGED = "GROUPING_CHANGED"
WINDOW_CHANGED = "WINDOW_CHANGED"
SOURCE_CHANGED = "SOURCE_CHANGED"
GRAIN_POSSIBLY_CHANGED = "GRAIN_POSSIBLY_CHANGED"
UNSUPPORTED = "UNSUPPORTED"

CHANGE_KINDS = (
    SOURCE_CHANGED,
    JOIN_CHANGED,
    FILTER_CHANGED,
    EXPRESSION_CHANGED,
    AGGREGATE_CHANGED,
    GROUPING_CHANGED,
    WINDOW_CHANGED,
    GRAIN_POSSIBLY_CHANGED,
    UNSUPPORTED,
)

UNSAFE_KINDS = frozenset(
    {
        GROUPING_CHANGED,
        GRAIN_POSSIBLY_CHANGED,
        UNSUPPORTED,
    }
)

_UNSUPPORTED_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Command,
    exp.Pivot,
    exp.UnpivotColumns,
    exp.MatchRecognize,
)

@dataclass(frozen=True)
class NormalizedSelect:
    sources: tuple[str, ...] = ()
    filters: tuple[str, ...] = ()
    joins: tuple[str, ...] = ()
    projections: tuple[str, ...] = ()
    case_expressions: tuple[str, ...] = ()
    grouping_keys: tuple[str, ...] = ()
    aggregates: tuple[str, ...] = ()
    windows: tuple[str, ...] = ()
    distinct: bool = False
    limit: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "sources": list(self.sources),
            "filters": list(self.filters),
            "joins": list(self.joins),
            "projections": list(self.projections),
            "caseExpressions": list(self.case_expressions),
            "groupingKeys": list(self.grouping_keys),
            "aggregates": list(self.aggregates),
            "windows": list(self.windows),
            "distinct": self.distinct,
            "limit": self.limit,
        }


@dataclass(frozen=True)
class SnowflakeParseResult:
    ok: bool
    ir: NormalizedSelect | None
    unsupported: tuple[str, ...] = ()

    @property
    def affected_entities(self) -> list[Any]:
        # Parser never yields an affected set. Callers must not treat a
        # parse miss as "no rows changed".
        return []


@dataclass(frozen=True)
class SqlChangeClassification:
    kinds: tuple[str, ...]
    unsafe: bool
    unsupported_reasons: tuple[str, ...] = ()
    base_ir: NormalizedSelect | None = None
    pr_ir: NormalizedSelect | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "changeKinds": list(self.kinds),
            "unsafe": self.unsafe,
        }
        if self.unsupported_reasons:
            payload["unsupportedReasons"] = list(self.unsupported_reasons)
        return payload

    @property
    def affected_entities(self) -> list[Any]:
        return []


def _canonical(node: exp.Expression | None) -> str:
    if node is None:
        return ""
    return node.sql(
        dialect=DIALECT,
        comments=False,
        pretty=False,
        normalize=True,
        normalize_functions="lower",
    ).strip()


def _sorted_unique(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if value}))


def _flatten_and(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    if isinstance(node, (exp.Where, exp.Having, exp.Qualify)):
        return _flatten_and(node.this)
    if isinstance(node, exp.And):
        return [*_flatten_and(node.left), *_flatten_and(node.right)]
    return [node]


def _table_alias_map(root: exp.Expression) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for table in root.find_all(exp.Table):
        copied = table.copy()
        copied.set("alias", None)
        name = _canonical(copied)
        if not name:
            continue
        alias = table.alias
        if alias:
            mapping[str(alias).lower()] = name
        mapping[name.lower()] = name
    return mapping


def _rewrite_aliases(node: exp.Expression, alias_map: dict[str, str]) -> exp.Expression:
    copied = node.copy()
    unique_tables = {name.lower() for name in alias_map.values()}
    single = next(iter(unique_tables)) if len(unique_tables) == 1 else None
    for column in copied.find_all(exp.Column):
        table = column.table
        if not table:
            continue
        key = str(table).lower()
        replacement = alias_map.get(key)
        if single:
            column.set("table", None)
        elif replacement:
            column.set("table", exp.to_identifier(replacement, quoted=False))
    for table in copied.find_all(exp.Table):
        table.set("alias", None)
    return copied


def _projection(expr: exp.Expression, alias_map: dict[str, str]) -> str:
    node = expr
    if isinstance(node, exp.Alias):
        node = node.this
    return _canonical(_rewrite_aliases(node, alias_map))


def _join_kind(join: exp.Join) -> str:
    side = str(join.args.get("side") or "").upper()
    kind = str(join.args.get("kind") or "INNER").upper()
    return f"{side} {kind}".strip()


def _unsupported_reasons(root: exp.Expression) -> list[str]:
    reasons: list[str] = []
    for node in root.walk():
        if isinstance(node, _UNSUPPORTED_TYPES):
            reasons.append(type(node).__name__.upper())
        if isinstance(node, exp.Anonymous):
            name = (node.name or "ANONYMOUS").upper()
            reasons.append(f"unknown UDF {name}")
        if isinstance(node, exp.With) and node.args.get("recursive"):
            reasons.append("RECURSIVE_CTE")
    return reasons


def _from_select(select: exp.Select, alias_map: dict[str, str]) -> NormalizedSelect:
    sources: list[str] = []
    for table in select.find_all(exp.Table):
        copied = table.copy()
        copied.set("alias", None)
        sources.append(_canonical(copied))

    joins: list[str] = []
    for join in select.find_all(exp.Join):
        target = join.this.copy() if join.this else None
        if isinstance(target, exp.Table):
            target.set("alias", None)
        on = join.args.get("on")
        using = join.args.get("using")
        predicate = ""
        if on is not None:
            predicate = _canonical(_rewrite_aliases(on, alias_map))
        elif using is not None:
            predicate = f"using {_canonical(using)}"
        joins.append(f"{_join_kind(join)} {_canonical(target)} on {predicate}".strip())

    filters = [
        _canonical(_rewrite_aliases(predicate, alias_map))
        for predicate in (
            *_flatten_and(select.args.get("where")),
            *_flatten_and(select.args.get("having")),
        )
    ]

    projections = [
        _projection(expr, alias_map) for expr in select.expressions or []
    ]

    grouping: list[str] = []
    group = select.args.get("group")
    if group is not None:
        grouping = [
            _canonical(_rewrite_aliases(expr, alias_map))
            for expr in group.expressions or []
        ]

    aggregates = [
        _canonical(_rewrite_aliases(func, alias_map))
        for func in select.find_all(exp.AggFunc)
    ]

    windows = [
        _canonical(_rewrite_aliases(window, alias_map))
        for window in select.find_all(exp.Window)
    ]
    qualify = select.args.get("qualify")
    if qualify is not None:
        windows.append(f"qualify {_canonical(_rewrite_aliases(qualify, alias_map))}")

    cases = [
        _canonical(_rewrite_aliases(case, alias_map))
        for case in select.find_all(exp.Case)
    ]

    distinct = bool(select.args.get("distinct"))
    limit_node = select.args.get("limit")
    limit = _canonical(_rewrite_aliases(limit_node, alias_map)) if limit_node else None

    return NormalizedSelect(
        sources=_sorted_unique(sources),
        filters=_sorted_unique(filters),
        joins=_sorted_unique(joins),
        projections=tuple(projections),
        case_expressions=_sorted_unique(cases),
        grouping_keys=tuple(grouping),
        aggregates=_sorted_unique(aggregates),
        windows=_sorted_unique(windows),
        distinct=distinct,
        limit=limit or None,
    )


def _merge_irs(parts: list[NormalizedSelect]) -> NormalizedSelect:
    distinct = any(part.distinct for part in parts)
    limits = {part.limit for part in parts if part.limit}
    return NormalizedSelect(
        sources=_sorted_unique(item for part in parts for item in part.sources),
        filters=_sorted_unique(item for part in parts for item in part.filters),
        joins=_sorted_unique(item for part in parts for item in part.joins),
        projections=tuple(item for part in parts for item in part.projections),
        case_expressions=_sorted_unique(item for part in parts for item in part.case_expressions),
        grouping_keys=tuple(item for part in parts for item in part.grouping_keys),
        aggregates=_sorted_unique(item for part in parts for item in part.aggregates),
        windows=_sorted_unique(item for part in parts for item in part.windows),
        distinct=distinct,
        limit=next(iter(limits), None) if len(limits) == 1 else (
            "|".join(sorted(limits)) if limits else None
        ),
    )


def parse_snowflake_sql(sql: str) -> SnowflakeParseResult:
    text = (sql or "").strip()
    if not text:
        return SnowflakeParseResult(
            ok=False,
            ir=None,
            unsupported=("empty SQL",),
        )
    try:
        statements = sqlglot.parse(text, dialect=DIALECT)
    except SqlglotError as error:
        return SnowflakeParseResult(
            ok=False,
            ir=None,
            unsupported=(f"parse error: {error}",),
        )
    expressions = [item for item in statements if item is not None]
    if not expressions:
        return SnowflakeParseResult(
            ok=False,
            ir=None,
            unsupported=("parse error: no statements",),
        )

    reasons: list[str] = []
    parts: list[NormalizedSelect] = []
    for expression in expressions:
        reasons.extend(_unsupported_reasons(expression))
        alias_map = _table_alias_map(expression)
        selects = [node for node in expression.find_all(exp.Select)]
        if not selects:
            reasons.append("unsupported statement")
            continue
        for select in selects:
            parts.append(_from_select(select, alias_map))
        if isinstance(expression, exp.Union):
            distinct_union = expression.args.get("distinct")
            if distinct_union is not False:
                parts.append(
                    NormalizedSelect(distinct=True)
                )

    if reasons:
        return SnowflakeParseResult(
            ok=False,
            ir=None,
            unsupported=_sorted_unique(reasons),
        )
    if not parts:
        return SnowflakeParseResult(
            ok=False,
            ir=None,
            unsupported=("unsupported statement",),
        )
    return SnowflakeParseResult(ok=True, ir=_merge_irs(parts), unsupported=())


def classify_sql_change(base_sql: str, pr_sql: str) -> SqlChangeClassification:
    base = parse_snowflake_sql(base_sql)
    pr = parse_snowflake_sql(pr_sql)
    if not base.ok or not pr.ok:
        reasons = tuple(
            dict.fromkeys(
                [*base.unsupported, *pr.unsupported],
            )
        )
        return SqlChangeClassification(
            kinds=(UNSUPPORTED,),
            unsafe=True,
            unsupported_reasons=reasons,
            base_ir=base.ir,
            pr_ir=pr.ir,
        )
    assert base.ir is not None and pr.ir is not None
    kinds: list[str] = []
    if base.ir.sources != pr.ir.sources:
        kinds.append(SOURCE_CHANGED)
    if base.ir.joins != pr.ir.joins:
        kinds.append(JOIN_CHANGED)
    if base.ir.filters != pr.ir.filters:
        kinds.append(FILTER_CHANGED)
    if (
        base.ir.projections != pr.ir.projections
        or base.ir.case_expressions != pr.ir.case_expressions
    ):
        kinds.append(EXPRESSION_CHANGED)
    if base.ir.aggregates != pr.ir.aggregates:
        kinds.append(AGGREGATE_CHANGED)
    if base.ir.grouping_keys != pr.ir.grouping_keys:
        kinds.append(GROUPING_CHANGED)
    if base.ir.windows != pr.ir.windows:
        kinds.append(WINDOW_CHANGED)
    if base.ir.distinct != pr.ir.distinct or base.ir.limit != pr.ir.limit:
        kinds.append(GRAIN_POSSIBLY_CHANGED)
    ordered = tuple(kind for kind in CHANGE_KINDS if kind in kinds)
    unsafe = any(kind in UNSAFE_KINDS for kind in ordered)
    return SqlChangeClassification(
        kinds=ordered,
        unsafe=unsafe,
        base_ir=base.ir,
        pr_ir=pr.ir,
    )


def describe_sql_change(base_sql: str, pr_sql: str) -> str:
    """Human-readable operator summary. Never includes warehouse rows."""
    change = classify_sql_change(base_sql, pr_sql)
    if not change.kinds:
        return "No semantic SQL change"
    if UNSUPPORTED in change.kinds:
        reason = "; ".join(change.unsupported_reasons) or "unsupported SQL"
        return f"UNSUPPORTED: {reason}"[:512]
    parts: list[str] = []
    if FILTER_CHANGED in change.kinds and change.base_ir and change.pr_ir:
        removed = [item for item in change.base_ir.filters if item not in change.pr_ir.filters]
        added = [item for item in change.pr_ir.filters if item not in change.base_ir.filters]
        if removed or added:
            left = " AND ".join(removed) or "(none)"
            right = " AND ".join(added) or "(none)"
            parts.append(f"FILTER_CHANGED: {left} → {right}")
        else:
            parts.append("FILTER_CHANGED")
    for kind in change.kinds:
        if kind == FILTER_CHANGED:
            continue
        parts.append(kind)
    if not parts:
        parts.extend(change.kinds)
    return "; ".join(parts)[:512]


def narrow_frontier_safe(comparison: dict[str, Any] | None) -> bool:
    if not comparison:
        return True
    if comparison.get("narrowFrontierSafe") is False:
        return False
    for row in comparison.get("modified") or []:
        if row.get("unsafe"):
            return False
        kinds = row.get("changeKinds") or []
        if UNSUPPORTED in kinds or any(kind in UNSAFE_KINDS for kind in kinds):
            return False
        if row.get("impactStatus") == "FULL_REBUILD_REQUIRED":
            return False
    return True
