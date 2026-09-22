"""Static filter-v1 plan correspondence, taint, key-lineage, and candidate compiler.

Milestone 21B classifies a plan as statically eligible. Milestone 21C compiles
RowCover transfer SQL from that matched DAG and never substitutes the less-strict
impact compiler. This module never emits SQL_CERTIFIED and never invents
ADAPTER_VERIFIED snapshot evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Iterable

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from frontier.sql_fingerprint import render_executable_sql, sql_fingerprint, using_sql_dialect

RULE_SET_VERSION = "filter-v1"
FLIP_RULE = "(P_old IS TRUE) IS DISTINCT FROM (P_new IS TRUE)"

PLAN_NODE_CORRESPONDENCE_FAILED = "PLAN_NODE_CORRESPONDENCE_FAILED"
TWO_TAINTED_JOIN_INPUTS = "TWO_TAINTED_JOIN_INPUTS"
KEY_LINEAGE_BROKEN = "KEY_LINEAGE_BROKEN"
UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
NON_TOTAL_PREDICATE = "NON_TOTAL_PREDICATE"
NULLABLE_ENTITY_KEY = "NULLABLE_ENTITY_KEY"

JOIN_TAINT_NEITHER = "neither"
JOIN_TAINT_LEFT = "left"
JOIN_TAINT_RIGHT = "right"
JOIN_TAINT_BOTH = "both"

SUPPORTED_AGGS = frozenset({"count", "sum", "min", "max", "avg"})
JOIN_KINDS = frozenset({"innerjoin", "leftjoin"})

_SCOPE = "This is a filter-v1 scope restriction, not a requirement of the taint lemma."
_MATH_JOIN = (
    "This is required for soundness of the one-sided join transfer rules; "
    "a one-sided join rule must not be applied to this plan."
)

_NON_TOTAL_TYPES = tuple(
    cls
    for cls in (
        getattr(exp, "Div", None),
        getattr(exp, "IntDiv", None),
        getattr(exp, "Mod", None),
        getattr(exp, "Cast", None),
        getattr(exp, "TryCast", None),
    )
    if cls is not None
)
_EQ_TYPES = tuple(
    cls
    for cls in (exp.EQ, getattr(exp, "NullSafeEQ", None))
    if cls is not None
)
_SECRET = re.compile(r"(password|secret|frn_|entityValue|entity_id)", re.I)


class PlanError(Exception):
    def __init__(self, code: str, diagnostic: str) -> None:
        super().__init__(diagnostic)
        self.code = code
        self.diagnostic = diagnostic


@dataclass(frozen=True)
class ColumnBinding:
    name: str
    origin: str
    kind: str  # column | expr | ambiguous | literal


@dataclass
class Schema:
    columns: dict[str, ColumnBinding] = field(default_factory=dict)
    order: tuple[str, ...] = ()
    star_origin: str | None = None
    star_ambiguous: bool = False

    def lookup(self, name: str) -> ColumnBinding | None:
        key = name.lower()
        if key in self.columns:
            return self.columns[key]
        if self.star_ambiguous:
            return ColumnBinding(name=key, origin="ambiguous", kind="ambiguous")
        return None

    def passthrough(self) -> Schema:
        return Schema(
            columns=dict(self.columns),
            order=self.order,
            star_origin=self.star_origin,
            star_ambiguous=self.star_ambiguous,
        )


@dataclass
class BoundInput:
    node_id: str
    alias: str
    schema: Schema


@dataclass(frozen=True)
class PlanNode:
    id: str
    kind: str
    payload: str
    inputs: tuple[str, ...]
    columns: tuple[str, ...] = ()
    origins: tuple[tuple[str, str], ...] = ()
    star_origin: str | None = None
    star_ambiguous: bool = False
    join_kind: str = ""
    join_keys: tuple[tuple[str, str], ...] = ()


@dataclass
class PlanGraph:
    nodes: dict[str, PlanNode]
    root_id: str
    unsupported: tuple[tuple[str, str], ...] = ()

    def node(self, node_id: str) -> PlanNode:
        return self.nodes[node_id]


@dataclass(frozen=True)
class StaticEligibility:
    eligible: bool
    rule_set_version: str = RULE_SET_VERSION
    semantic_change: bool = True
    reason_code: str | None = None
    diagnostic: str | None = None
    old_plan_fingerprint: str | None = None
    new_plan_fingerprint: str | None = None
    changed_filter_node_id: str | None = None
    matched_node_ids: tuple[str, ...] = ()
    covered_node_ids: tuple[str, ...] = ()
    operators: tuple[str, ...] = ()
    join_taint: tuple[dict[str, str], ...] = ()
    key_lineage: tuple[dict[str, str], ...] = ()
    manifest_version: int | None = None
    manifest_fingerprint: str | None = None
    compiled: bool = False
    candidate_sql: str | None = None
    candidate_fingerprint: str | None = None
    compile_reason_code: str | None = None
    checked_assumptions: tuple[str, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "eligible": self.eligible,
            "ruleSetVersion": self.rule_set_version,
        }
        if self.semantic_change is False:
            payload["semanticChange"] = False
        if self.reason_code:
            payload["reasonCode"] = self.reason_code
        if self.diagnostic:
            payload["diagnostic"] = _safe_diagnostic(self.diagnostic)
        if self.old_plan_fingerprint:
            payload["oldPlanFingerprint"] = self.old_plan_fingerprint
        if self.new_plan_fingerprint:
            payload["newPlanFingerprint"] = self.new_plan_fingerprint
        if self.changed_filter_node_id:
            payload["changedFilterNodeId"] = self.changed_filter_node_id
        if self.matched_node_ids:
            payload["matchedNodeIds"] = list(self.matched_node_ids)
        if self.covered_node_ids:
            payload["coveredNodeIds"] = list(self.covered_node_ids)
        if self.operators:
            payload["operators"] = list(self.operators)
        if self.join_taint:
            payload["joinTaint"] = [dict(item) for item in self.join_taint]
        if self.key_lineage:
            payload["keyLineage"] = [dict(item) for item in self.key_lineage]
        deps = _manifest_dependencies(self.manifest_version, self.manifest_fingerprint)
        if deps:
            payload["manifestDependencies"] = deps
        if self.eligible:
            payload["compiled"] = self.compiled
        if self.candidate_fingerprint:
            payload["candidateFingerprint"] = self.candidate_fingerprint
        if self.compile_reason_code:
            payload["compileReasonCode"] = self.compile_reason_code
        if self.checked_assumptions:
            payload["checkedAssumptions"] = list(self.checked_assumptions)
        assert_eligibility_payload_is_safe(payload)
        return payload


def analyze_static_eligibility(
    base_sql: str,
    pr_sql: str,
    *,
    entity_key: str,
    dialect: str = "snowflake",
    manifest_version: int | None = None,
    manifest_fingerprint: str | None = None,
    target_model: str | None = None,
    schema_catalog: dict[str, tuple[str, ...]] | None = None,
) -> StaticEligibility:
    """Return static filter-v1 eligibility. Never claims snapshot assurance."""
    del target_model
    with using_sql_dialect(dialect):
        return _analyze(
            base_sql,
            pr_sql,
            entity_key=entity_key,
            dialect=dialect,
            manifest_version=manifest_version,
            manifest_fingerprint=manifest_fingerprint,
            schema_catalog=schema_catalog,
        )


def assert_eligibility_payload_is_safe(payload: dict[str, Any]) -> None:
    dumped = json.dumps(payload)
    if "ADAPTER_VERIFIED" in dumped:
        raise PlanError(UNSUPPORTED_OPERATION, "static eligibility must not invent snapshot assurance")
    if _SECRET.search(dumped):
        raise PlanError(UNSUPPORTED_OPERATION, "static eligibility must not include secrets or entity IDs")
    for key in ("predicate", "sql", "candidateSql", "baseSql", "prSql"):
        if key in payload:
            raise PlanError(UNSUPPORTED_OPERATION, "static eligibility must not include SQL")


def _analyze(
    base_sql: str,
    pr_sql: str,
    *,
    entity_key: str,
    dialect: str,
    manifest_version: int | None,
    manifest_fingerprint: str | None,
    schema_catalog: dict[str, tuple[str, ...]] | None,
) -> StaticEligibility:
    base_graph, base_error = _try_parse(base_sql, dialect=dialect, schema_catalog=schema_catalog)
    pr_graph, pr_error = _try_parse(pr_sql, dialect=dialect, schema_catalog=schema_catalog)
    old_fp = _plan_fingerprint(base_graph) if base_graph else None
    new_fp = _plan_fingerprint(pr_graph) if pr_graph else None
    deps = (manifest_version, manifest_fingerprint)

    if base_error or pr_error:
        error = pr_error or base_error
        assert error is not None
        return _reject(
            error.code,
            error.diagnostic,
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph or base_graph,
        )

    assert base_graph is not None and pr_graph is not None
    if old_fp == new_fp:
        return StaticEligibility(
            eligible=False,
            semantic_change=False,
            diagnostic="alias, comment, or formatting changes do not change the resolved plan",
            old_plan_fingerprint=old_fp,
            new_plan_fingerprint=new_fp,
            matched_node_ids=tuple(sorted(pr_graph.nodes)),
            covered_node_ids=tuple(sorted(pr_graph.nodes)),
            operators=_all_operator_kinds(pr_graph),
            manifest_version=manifest_version,
            manifest_fingerprint=manifest_fingerprint,
        )

    mapping, match_error = _correspond(base_graph, pr_graph)
    if match_error:
        return _reject(
            PLAN_NODE_CORRESPONDENCE_FAILED,
            match_error,
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
        )

    changed_filters: list[tuple[PlanNode, PlanNode]] = []
    extra_changes: list[str] = []
    for base_id, pr_id in mapping.items():
        base_node = base_graph.node(base_id)
        pr_node = pr_graph.node(pr_id)
        if base_node.kind == "filter":
            if base_node.payload != pr_node.payload:
                changed_filters.append((base_node, pr_node))
            continue
        if base_node.payload != pr_node.payload or base_node.kind != pr_node.kind:
            extra_changes.append(base_node.kind)

    if len(changed_filters) > 1:
        return _reject(
            PLAN_NODE_CORRESPONDENCE_FAILED,
            "more than one corresponding filter node changed; an empty candidate set must not be inferred",
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
        )
    if len(changed_filters) == 0:
        kind = extra_changes[0] if extra_changes else "plan"
        return _reject(
            UNSUPPORTED_OPERATION,
            f"SQL changed but is not a single filter-predicate change ({kind} differed); "
            "this is ineligible for filter-v1 and is not a no-semantic-change classification",
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
        )

    changed = changed_filters[0][1]
    unsupported = pr_graph.unsupported or base_graph.unsupported
    if unsupported:
        code, diagnostic = unsupported[0]
        return _reject(
            code,
            diagnostic,
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
        )

    totality = _predicate_totality(pr_sql, changed, dialect=dialect)
    if totality:
        return _reject(
            totality[0],
            totality[1],
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
        )

    taint_error, join_taint, covered, operators = _taint_analysis(pr_graph, changed.id)
    if taint_error:
        return _reject(
            taint_error[0],
            taint_error[1],
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
            join_taint=join_taint,
            covered=covered,
            operators=operators,
        )

    if not (entity_key or "").strip():
        return _reject(
            KEY_LINEAGE_BROKEN,
            "pinned semantic manifest did not identify an entity key; "
            "a manifest field is not evidence that SQL key lineage was verified",
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
            join_taint=join_taint,
            covered=covered,
            operators=operators,
        )

    lineage_error, lineage = _key_lineage(pr_graph, entity_key=entity_key.strip())
    if lineage_error:
        return _reject(
            lineage_error[0],
            lineage_error[1],
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
            join_taint=join_taint,
            covered=covered,
            operators=operators,
            lineage=lineage,
        )

    grouping_error = _check_grouping_form(pr_graph, entity_key=entity_key.strip(), lineage=lineage)
    if grouping_error:
        return _reject(
            grouping_error[0],
            grouping_error[1],
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
            join_taint=join_taint,
            covered=covered,
            operators=operators,
            lineage=lineage,
        )

    if extra_changes:
        kinds = ", ".join(sorted(set(extra_changes)))
        return _reject(
            UNSUPPORTED_OPERATION,
            f"filter-v1 requires every corresponding node except the changed filter to be "
            f"semantically unchanged ({kinds} also differed)",
            old_fp=old_fp,
            new_fp=new_fp,
            deps=deps,
            graph=pr_graph,
            matched=tuple(mapping.values()),
            changed=changed.id,
            join_taint=join_taint,
            covered=covered,
            operators=operators,
            lineage=lineage,
        )

    compiled, compile_error, candidate_sql, candidate_fp = compile_candidate_query(
        base_graph,
        pr_graph,
        mapping,
        changed_base=changed_filters[0][0],
        changed_pr=changed,
        entity_key=entity_key.strip(),
        dialect=dialect,
    )
    assumptions = (
        "single_filter_predicate_change",
        "plan_node_correspondence",
        "at_most_one_tainted_join_input",
        "entity_key_tracked_1_1",
        FLIP_RULE,
        "rowcover_until_terminal_group",
    )
    return StaticEligibility(
        eligible=True,
        old_plan_fingerprint=old_fp,
        new_plan_fingerprint=new_fp,
        changed_filter_node_id=changed.id,
        matched_node_ids=tuple(mapping.values()),
        covered_node_ids=covered,
        operators=operators,
        join_taint=join_taint,
        key_lineage=lineage,
        manifest_version=manifest_version,
        manifest_fingerprint=manifest_fingerprint,
        compiled=compiled,
        candidate_sql=candidate_sql,
        candidate_fingerprint=candidate_fp,
        compile_reason_code=None if compiled else (compile_error[0] if compile_error else UNSUPPORTED_OPERATION),
        diagnostic=None if compiled else (compile_error[1] if compile_error else "candidate compiler failed closed"),
        checked_assumptions=assumptions if compiled else assumptions[:4],
    )


def _try_parse(
    sql: str,
    *,
    dialect: str,
    schema_catalog: dict[str, tuple[str, ...]] | None = None,
) -> tuple[PlanGraph | None, PlanError | None]:
    try:
        return parse_plan(sql, dialect=dialect, schema_catalog=schema_catalog), None
    except PlanError as error:
        return None, error


def parse_plan(
    sql: str,
    *,
    dialect: str = "snowflake",
    schema_catalog: dict[str, tuple[str, ...]] | None = None,
) -> PlanGraph:
    text = (sql or "").strip()
    if not text:
        raise PlanError(UNSUPPORTED_OPERATION, "SQL is empty")
    try:
        root = sqlglot.parse_one(text, dialect=dialect)
    except SqlglotError as error:
        raise PlanError(UNSUPPORTED_OPERATION, "SQL could not be parsed") from error
    if root is None:
        raise PlanError(UNSUPPORTED_OPERATION, "SQL could not be parsed")
    builder = PlanBuilder(dialect=dialect, schema_catalog=schema_catalog)
    builder.scan_unsupported(root)
    root_id = builder.build_query(root)
    return builder.graph(root_id)


class PlanBuilder:
    def __init__(
        self,
        *,
        dialect: str,
        schema_catalog: dict[str, tuple[str, ...]] | None = None,
    ) -> None:
        self.dialect = dialect
        self.schema_catalog = {
            key.lower(): tuple(col.lower() for col in cols)
            for key, cols in (schema_catalog or {}).items()
        }
        self.nodes: dict[str, PlanNode] = {}
        self.unsupported: list[tuple[str, str]] = []
        self._used_ids: set[str] = set()

    def graph(self, root_id: str) -> PlanGraph:
        if self.unsupported:
            code, diagnostic = self.unsupported[0]
            raise PlanError(code, diagnostic)
        return PlanGraph(nodes=dict(self.nodes), root_id=root_id, unsupported=tuple(self.unsupported))

    def scan_unsupported(self, root: exp.Expression) -> None:
        for with_ in root.find_all(exp.With):
            if with_.args.get("recursive"):
                self._scope("recursive CTEs are excluded from filter-v1")
                return
        for node in root.walk():
            if isinstance(node, exp.Window) or (
                isinstance(node, exp.Select) and node.args.get("qualify") is not None
            ):
                self._scope(
                    "window functions and QUALIFY are excluded from filter-v1",
                )
            if isinstance(node, (exp.Union, exp.Except, exp.Intersect)):
                self._scope(
                    "set operations (UNION/EXCEPT/INTERSECT) can change multiplicity and are excluded from filter-v1",
                )
            if isinstance(node, exp.With) and node.args.get("recursive"):
                self._scope("recursive CTEs are excluded from filter-v1")
            extra_types = tuple(
                cls
                for cls in (
                    getattr(exp, "Pivot", None),
                    getattr(exp, "Unpivot", None),
                    getattr(exp, "UnpivotColumns", None),
                    getattr(exp, "MatchRecognize", None),
                    getattr(exp, "Lateral", None),
                    getattr(exp, "GroupingSets", None),
                )
                if cls is not None
            )
            if extra_types and isinstance(node, extra_types):
                self._scope(f"{type(node).__name__} is excluded from filter-v1")
            if isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Command)):
                self._scope("mutating SQL is excluded from filter-v1")
            if isinstance(node, exp.Lateral):
                self._scope("LATERAL is excluded from filter-v1")
            if isinstance(node, exp.GroupingSets):
                self._scope("GROUPING SETS/ROLLUP/CUBE are excluded from filter-v1")
            if isinstance(node, exp.Exists):
                self._scope(
                    "EXISTS subqueries are excluded from filter-v1; correlation cannot be ruled out"
                )
            if isinstance(node, exp.In) and _in_contains_subquery(node):
                self._scope(
                    "IN/subquery predicates are excluded from filter-v1; correlation cannot be ruled out"
                )
            extra_subquery = tuple(
                cls
                for cls in (
                    getattr(exp, "Any", None),
                    getattr(exp, "All", None),
                    getattr(exp, "Some", None),
                )
                if cls is not None
            )
            if extra_subquery and isinstance(node, extra_subquery):
                self._scope(
                    "quantified subqueries are excluded from filter-v1; correlation cannot be ruled out"
                )
            if isinstance(node, (exp.Subquery, exp.Select)) and not _is_relation_subquery(node, root):
                self._scope(
                    "a subquery whose correlation status cannot be established is excluded from filter-v1"
                )

    def build_query(self, node: exp.Expression, cte_map: dict[str, str] | None = None) -> str:
        cte_map = dict(cte_map or {})
        if isinstance(node, (exp.Union, exp.Except, exp.Intersect)):
            self._scope(
                "set operations (UNION/EXCEPT/INTERSECT) can change multiplicity and are excluded from filter-v1",
            )
            raise PlanError(self.unsupported[0][0], self.unsupported[0][1])
        if isinstance(node, exp.Subquery):
            return self.build_query(node.this, cte_map)
        if isinstance(node, exp.With):
            return self._build_select(node.this, with_=node, cte_map=cte_map)
        if isinstance(node, exp.Select):
            return self._build_select(
                node,
                with_=_arg(node, "with", "with_"),
                cte_map=cte_map,
            )
        raise PlanError(UNSUPPORTED_OPERATION, f"{type(node).__name__} is excluded from filter-v1. {_SCOPE}")

    def _build_select(
        self,
        select: exp.Select,
        *,
        with_: exp.With | None,
        cte_map: dict[str, str],
    ) -> str:
        local_ctes = dict(cte_map)
        if with_ is not None:
            if with_.args.get("recursive"):
                self._scope("recursive CTEs are excluded from filter-v1")
                raise PlanError(self.unsupported[0][0], self.unsupported[0][1])
            for cte in with_.expressions or []:
                name = _cte_alias(cte)
                inner = cte.this
                local_ctes[name] = self.build_query(inner, local_ctes)

        from_ = select.args.get("from") or select.args.get("from_")
        if from_ is None:
            raise PlanError(UNSUPPORTED_OPERATION, "SELECT without FROM is excluded from filter-v1")
        current = self._from_item(from_.this, local_ctes)
        aliases = {current.alias.lower(): current} if current.alias else {}
        if current.alias:
            aliases[current.alias.lower()] = current

        for join in select.args.get("joins") or []:
            right = self._from_item(join.this, local_ctes)
            if right.alias:
                aliases[right.alias.lower()] = right
            current = self._add_join(current, right, join, aliases)

        where = select.args.get("where")
        if where is not None:
            current = self._add_predicate_node(current, where.this, aliases, kind="filter")

        group = select.args.get("group")
        if group is not None:
            current = self._add_group(current, select, group, aliases)
            having = select.args.get("having")
            if having is not None:
                current = self._add_predicate_node(current, having.this, aliases, kind="having")
        else:
            if select.args.get("having") is not None:
                self._scope("HAVING without GROUP BY is excluded from filter-v1")
            current = self._add_project(current, select, aliases)
            if select.args.get("distinct"):
                self._scope("SELECT DISTINCT without GROUP BY is excluded from filter-v1")

        if select.args.get("qualify") is not None:
            self._scope("window functions and QUALIFY are excluded from filter-v1")
        if select.find(exp.Window):
            self._scope("window functions and QUALIFY are excluded from filter-v1")
        return current.node_id

    def _from_item(self, node: exp.Expression, cte_map: dict[str, str]) -> BoundInput:
        alias = _alias_name(node)
        inner = node.this if isinstance(node, (exp.Alias, exp.Subquery)) else node
        if isinstance(inner, exp.Subquery):
            alias = alias or _alias_name(inner)
            inner = inner.this
        if isinstance(inner, (exp.Select, exp.With)):
            node_id = self.build_query(inner, cte_map)
            schema = _schema_of(self.nodes[node_id])
            return BoundInput(node_id, alias or node_id.split(":")[-1], schema)
        if isinstance(inner, exp.Table):
            table_name = _table_canonical(inner, self.dialect)
            short = str(inner.name or "").lower()
            if not (inner.db or inner.catalog) and short in cte_map:
                node_id = cte_map[short]
                schema = _schema_of(self.nodes[node_id])
                return BoundInput(node_id, alias or short, schema)
            node_id = self._add_source(table_name)
            schema = _schema_of(self.nodes[node_id])
            return BoundInput(node_id, alias or short or table_name, schema)
        raise PlanError(
            UNSUPPORTED_OPERATION,
            f"FROM {type(inner).__name__} is excluded from filter-v1. {_SCOPE}",
        )

    def _add_source(self, name: str) -> str:
        node_id = self._allocate(f"source:{name}")
        origin = name.lower()
        columns = self._catalog_columns(origin)
        origins = tuple((col, f"{origin}.{col}") for col in columns)
        self.nodes[node_id] = PlanNode(
            id=node_id,
            kind="source",
            payload=origin,
            inputs=(),
            columns=columns,
            origins=origins,
            star_origin=origin,
        )
        return node_id

    def _catalog_columns(self, name: str) -> tuple[str, ...]:
        if not self.schema_catalog:
            return ()
        key = name.lower()
        if key in self.schema_catalog:
            return self.schema_catalog[key]
        short = key.rsplit(".", 1)[-1]
        return self.schema_catalog.get(short, ())

    def _add_predicate_node(
        self,
        current: BoundInput,
        predicate: exp.Expression,
        aliases: dict[str, BoundInput],
        *,
        kind: str,
    ) -> BoundInput:
        payload = self._canonical_expr(predicate, aliases)
        node_id = self._allocate(f"{kind}:{current.node_id}")
        schema = current.schema.passthrough()
        self.nodes[node_id] = PlanNode(
            id=node_id,
            kind=kind,
            payload=payload,
            inputs=(current.node_id,),
            columns=schema.order,
            origins=tuple((name, binding.origin) for name, binding in schema.columns.items()),
            star_origin=schema.star_origin,
            star_ambiguous=schema.star_ambiguous,
        )
        return BoundInput(node_id, current.alias, schema)

    def _add_join(
        self,
        left: BoundInput,
        right: BoundInput,
        join: exp.Join,
        aliases: dict[str, BoundInput],
    ) -> BoundInput:
        join_kind = _join_kind(join)
        if join_kind is None:
            side = str(join.args.get("side") or join.args.get("kind") or "JOIN").upper()
            self._scope(f"{side} joins are excluded from filter-v1")
            raise PlanError(self.unsupported[0][0], self.unsupported[0][1])
        on = join.args.get("on")
        using = join.args.get("using")
        if on is None and using is None:
            self._scope("non-equijoin or CROSS joins are excluded from filter-v1")
            raise PlanError(self.unsupported[0][0], self.unsupported[0][1])
        if using is not None and on is None:
            names = [str(item.name or item).lower() for item in using.expressions or using.flatten()]
            payload = f"using:{','.join(names)}"
            join_keys = tuple((name, name) for name in names)
            if not _equijoin_using_ok(left.schema, right.schema, names):
                raise PlanError(KEY_LINEAGE_BROKEN, "join USING columns are not tracked 1:1 on both inputs")
        else:
            if not _is_equijoin(on):
                self._scope("non-equijoin predicates are excluded from filter-v1")
                raise PlanError(self.unsupported[0][0], self.unsupported[0][1])
            payload = self._canonical_expr(on, aliases)
            join_keys = _join_key_names(on)
        node_id = self._allocate(f"{join_kind}:{left.node_id}|{right.node_id}")
        schema = _merge_schemas(left.schema, right.schema)
        self.nodes[node_id] = PlanNode(
            id=node_id,
            kind=join_kind,
            payload=f"{join_kind}:{payload}",
            inputs=(left.node_id, right.node_id),
            columns=schema.order,
            origins=tuple((name, binding.origin) for name, binding in schema.columns.items()),
            star_origin=schema.star_origin,
            star_ambiguous=schema.star_ambiguous,
            join_kind=join_kind,
            join_keys=join_keys,
        )
        return BoundInput(node_id, left.alias, schema)

    def _add_project(
        self,
        current: BoundInput,
        select: exp.Select,
        aliases: dict[str, BoundInput],
    ) -> BoundInput:
        expressions = self._expand_stars(list(select.expressions or []), aliases, current)
        columns: dict[str, ColumnBinding] = {}
        order: list[str] = []
        parts: list[str] = []
        for expr in expressions:
            name, binding = self._projection_binding(expr, aliases, current.schema)
            columns[name] = binding
            order.append(name)
            parts.append(f"{name}={binding.origin}")
        schema = Schema(columns=columns, order=tuple(order))
        node_id = self._allocate(f"project:{current.node_id}")
        self.nodes[node_id] = PlanNode(
            id=node_id,
            kind="project",
            payload="|".join(parts),
            inputs=(current.node_id,),
            columns=schema.order,
            origins=tuple((name, binding.origin) for name, binding in schema.columns.items()),
        )
        return BoundInput(node_id, current.alias, schema)

    def _expand_stars(
        self,
        expressions: list[exp.Expression],
        aliases: dict[str, BoundInput],
        current: BoundInput,
    ) -> list[exp.Expression]:
        expanded: list[exp.Expression] = []
        for expr in expressions:
            table = _star_table(expr)
            if table is None:
                expanded.append(expr)
                continue
            bound = current
            if table:
                found = aliases.get(table)
                if found is None:
                    raise PlanError(
                        KEY_LINEAGE_BROKEN,
                        "SELECT * refers to an alias that is not in scope; "
                        "star expansion requires an unambiguous input schema",
                    )
                bound = found
            schema = bound.schema
            if schema.star_ambiguous or any(
                binding.kind == "ambiguous" for binding in schema.columns.values()
            ):
                raise PlanError(
                    KEY_LINEAGE_BROKEN,
                    "SELECT * is ambiguous across joined inputs; entity key and join keys "
                    "cannot be tracked through an unresolved star",
                )
            if not schema.order:
                raise PlanError(
                    KEY_LINEAGE_BROKEN,
                    "SELECT * could not be expanded from dbt artifact or warehouse schema metadata; "
                    "star is not verified key preservation",
                )
            for name in schema.order:
                column = exp.column(name, table=bound.alias or None, quoted=True)
                expanded.append(column)
        return expanded

    def _add_group(
        self,
        current: BoundInput,
        select: exp.Select,
        group: exp.Group,
        aliases: dict[str, BoundInput],
    ) -> BoundInput:
        if group.args.get("grouping_sets") or group.find(exp.GroupingSets, exp.Cube, exp.Rollup):
            self._scope("GROUPING SETS/ROLLUP/CUBE are excluded from filter-v1")
        projections: list[tuple[str, ColumnBinding, bool]] = []
        agg_parts: list[str] = []
        for expr in select.expressions or []:
            is_agg = bool(expr.find(exp.AggFunc))
            if is_agg:
                for func in expr.find_all(exp.AggFunc):
                    if func.args.get("filter"):
                        self._scope("FILTER(WHERE) aggregates are excluded from filter-v1")
                    agg_name = (func.sql_name() or type(func).__name__).lower()
                    if agg_name not in SUPPORTED_AGGS:
                        self._scope(
                            f"aggregate {agg_name.upper()} is excluded from filter-v1; "
                            "COUNT/SUM/MIN/MAX/AVG over fixed deterministic arguments are required"
                        )
                name, binding = self._projection_binding(expr, aliases, current.schema)
                binding = ColumnBinding(name=name, origin=f"agg:{binding.origin}", kind="expr")
                agg_parts.append(f"{name}={self._canonical_expr(expr, aliases)}")
            else:
                name, binding = self._projection_binding(expr, aliases, current.schema)
            projections.append((name, binding, is_agg))
        key_exprs = []
        for expr in group.expressions or []:
            if isinstance(expr, exp.Literal) and str(expr.this).isdigit():
                index = int(str(expr.this)) - 1
                if 0 <= index < len(projections):
                    key_exprs.append(projections[index][0])
                    continue
            key_exprs.append(self._canonical_expr(expr, aliases))
        columns = {name: binding for name, binding, _is_agg in projections}
        order = tuple(name for name, _binding, _is_agg in projections)
        payload = f"keys={','.join(key_exprs)};aggs={','.join(agg_parts)}"
        schema = Schema(columns=columns, order=order)
        node_id = self._allocate(f"group:{current.node_id}")
        self.nodes[node_id] = PlanNode(
            id=node_id,
            kind="group",
            payload=payload,
            inputs=(current.node_id,),
            columns=schema.order,
            origins=tuple((name, binding.origin) for name, binding in schema.columns.items()),
        )
        return BoundInput(node_id, current.alias, schema)

    def _projection_binding(
        self,
        expr: exp.Expression,
        aliases: dict[str, BoundInput],
        fallback: Schema,
    ) -> tuple[str, ColumnBinding]:
        node = expr
        alias = None
        if isinstance(node, exp.Alias):
            alias = str(node.alias or "").lower()
            node = node.this
        if isinstance(node, exp.Star):
            return "*", ColumnBinding(name="*", origin=fallback.star_origin or "*", kind="column")
        name = alias or (str(node.name).lower() if isinstance(node, exp.Column) else _expr_alias(node, self.dialect))
        binding = self._resolve_expr(node, aliases, fallback)
        return name, ColumnBinding(name=name, origin=binding.origin, kind=binding.kind)

    def _resolve_expr(
        self,
        node: exp.Expression,
        aliases: dict[str, BoundInput],
        fallback: Schema,
    ) -> ColumnBinding:
        if isinstance(node, exp.Column):
            table = str(node.table or "").lower()
            col = str(node.name or "").lower()
            if table:
                bound = aliases.get(table)
                if bound is not None:
                    found = bound.schema.lookup(col)
                    if found:
                        return found
                    invented = _named_column_through_star(bound.schema, col)
                    if invented:
                        return invented
                found = fallback.lookup(col)
                if found:
                    return found
                invented = _named_column_through_star(fallback, col)
                return invented or ColumnBinding(name=col, origin="untracked", kind="ambiguous")
            found = _unqualified_lookup(aliases, col) or fallback.lookup(col)
            if found:
                return found
            invented = _named_column_through_star(fallback, col)
            if invented:
                return invented
            for bound in aliases.values():
                invented = _named_column_through_star(bound.schema, col)
                if invented:
                    return invented
            return ColumnBinding(name=col, origin="untracked", kind="ambiguous")
        if isinstance(node, exp.Literal):
            return ColumnBinding(name="", origin="literal", kind="literal")
        origin = self._canonical_expr(node, aliases)
        return ColumnBinding(name="", origin=f"expr:{origin}", kind="expr")

    def _canonical_expr(self, node: exp.Expression, aliases: dict[str, BoundInput]) -> str:
        copied = node.copy()
        for column in copied.find_all(exp.Column):
            table = str(column.table or "").lower()
            col = str(column.name or "").lower()
            binding: ColumnBinding | None = None
            if table and table in aliases:
                binding = aliases[table].schema.lookup(col)
            elif not table:
                binding = _unqualified_lookup(aliases, col)
            if binding and binding.kind == "column" and "." in binding.origin:
                relation, _, rest = binding.origin.partition(".")
                column.set("table", exp.to_identifier(relation, quoted=False))
                column.set("this", exp.to_identifier(rest.split(".")[-1], quoted=False))
            elif table and table in aliases:
                column.set("table", exp.to_identifier(aliases[table].schema.star_origin or table, quoted=False))
        for table in copied.find_all(exp.Table):
            table.set("alias", None)
        return copied.sql(
            dialect=self.dialect,
            comments=False,
            pretty=False,
            normalize=True,
            normalize_functions="lower",
        ).strip()

    def _allocate(self, base: str) -> str:
        if base not in self._used_ids:
            self._used_ids.add(base)
            return base
        index = 2
        while f"{base}#{index}" in self._used_ids:
            index += 1
        node_id = f"{base}#{index}"
        self._used_ids.add(node_id)
        return node_id

    def _scope(self, message: str) -> None:
        diagnostic = f"{message}. {_SCOPE}"
        self.unsupported.append((UNSUPPORTED_OPERATION, diagnostic))


def _schema_of(node: PlanNode) -> Schema:
    return Schema(
        columns={name: ColumnBinding(name=name, origin=origin, kind=_origin_kind(origin)) for name, origin in node.origins},
        order=node.columns,
        star_origin=node.star_origin,
        star_ambiguous=node.star_ambiguous,
    )


def _origin_kind(origin: str) -> str:
    if origin in {"ambiguous", "untracked"}:
        return "ambiguous"
    if origin == "literal" or origin.startswith("literal"):
        return "literal"
    if origin.startswith("expr:") or origin.startswith("agg:"):
        return "expr"
    return "column"


def _merge_schemas(left: Schema, right: Schema) -> Schema:
    columns = dict(left.columns)
    order = list(left.order)
    for name, binding in right.columns.items():
        if name in columns and columns[name].origin != binding.origin:
            columns[name] = ColumnBinding(name=name, origin="ambiguous", kind="ambiguous")
        elif name not in columns:
            columns[name] = binding
            order.append(name)
    if left.star_origin and right.star_origin and left.star_origin != right.star_origin:
        return Schema(columns=columns, order=tuple(order), star_origin=None, star_ambiguous=True)
    return Schema(
        columns=columns,
        order=tuple(order),
        star_origin=left.star_origin or right.star_origin,
        star_ambiguous=left.star_ambiguous or right.star_ambiguous,
    )


def _unqualified_lookup(aliases: dict[str, BoundInput], name: str) -> ColumnBinding | None:
    seen: dict[str, BoundInput] = {}
    for bound in aliases.values():
        seen[bound.node_id] = bound
    hits: list[ColumnBinding] = []
    for bound in seen.values():
        found = bound.schema.lookup(name) or _named_column_through_star(bound.schema, name)
        if found is not None:
            hits.append(found)
    if not hits:
        return None
    origins = {hit.origin for hit in hits}
    if len(origins) == 1:
        return hits[0]
    return ColumnBinding(name=name.lower(), origin="ambiguous", kind="ambiguous")


def _arg(node: exp.Expression, *keys: str) -> Any:
    for key in keys:
        value = node.args.get(key)
        if value is not None:
            return value
    return None


def _cte_alias(cte: exp.Expression) -> str:
    alias = cte.args.get("alias")
    if isinstance(alias, exp.TableAlias):
        return str(alias.this or alias.name or "").lower()
    if alias:
        return str(alias).lower()
    return str(getattr(cte, "alias", "") or "").lower()


def _alias_name(node: exp.Expression) -> str:
    alias = node.args.get("alias") if hasattr(node, "args") else None
    if isinstance(alias, exp.TableAlias):
        return str(alias.this or alias.name or "").lower()
    if alias:
        return str(alias).lower()
    if isinstance(node, exp.Table):
        return str(node.alias or node.name or "").lower()
    return ""


def _table_canonical(table: exp.Table, dialect: str) -> str:
    copied = table.copy()
    copied.set("alias", None)
    copied.set("when", None)
    return copied.sql(dialect=dialect, comments=False, pretty=False, normalize=True).strip().lower()


def _join_kind(join: exp.Join) -> str | None:
    side = str(join.args.get("side") or "").upper()
    kind = str(join.args.get("kind") or "").upper()
    if side in {"FULL", "RIGHT", "SEMI", "ANTI"} or kind in {"FULL", "RIGHT", "CROSS", "SEMI", "ANTI"}:
        return None
    if side == "LEFT":
        return "leftjoin"
    return "innerjoin"


def _is_equijoin(on: exp.Expression | None) -> bool:
    if on is None:
        return False
    parts = _flatten_and(on)
    if not parts:
        return False
    for part in parts:
        if not isinstance(part, _EQ_TYPES):
            return False
        if not isinstance(part.left, exp.Column) or not isinstance(part.right, exp.Column):
            return False
    return True


def _join_key_names(on: exp.Expression) -> tuple[tuple[str, str], ...]:
    keys: list[tuple[str, str]] = []
    for part in _flatten_and(on):
        if isinstance(part, _EQ_TYPES) and isinstance(part.left, exp.Column) and isinstance(part.right, exp.Column):
            keys.append((str(part.left.name).lower(), str(part.right.name).lower()))
    return tuple(keys)


def _equijoin_using_ok(left: Schema, right: Schema, names: list[str]) -> bool:
    for name in names:
        left_b = left.lookup(name)
        right_b = right.lookup(name)
        if left_b is None or right_b is None:
            return False
        if left_b.kind != "column" or right_b.kind != "column":
            return False
    return True


def _flatten_and(node: exp.Expression | None) -> list[exp.Expression]:
    if node is None:
        return []
    if isinstance(node, exp.And):
        return [*_flatten_and(node.left), *_flatten_and(node.right)]
    return [node]


def _named_column_through_star(schema: Schema, col: str) -> ColumnBinding | None:
    """Bind an explicit column name through a single unresolved source star.

    SELECT * itself is not key preservation. A named reference that appears in
    the SQL text may still be bound to a single source origin.
    """
    if schema.star_ambiguous or schema.columns:
        return None
    if not schema.star_origin:
        return None
    return ColumnBinding(name=col.lower(), origin=f"{schema.star_origin}.{col.lower()}", kind="column")


def _star_table(expr: exp.Expression) -> str | None:
    node = expr.this if isinstance(expr, exp.Alias) else expr
    if not isinstance(node, exp.Star):
        return None
    table = node.args.get("table") if hasattr(node, "args") else None
    if table is None:
        return ""
    if isinstance(table, exp.Identifier):
        return str(table.this or "").lower()
    return str(table).lower()


def _in_contains_subquery(node: exp.In) -> bool:
    if node.args.get("query") is not None:
        return True
    for item in node.expressions or []:
        if isinstance(item, (exp.Select, exp.Subquery, exp.Union)):
            return True
        if item.find(exp.Select) is not None:
            return True
    this = node.this
    if isinstance(this, (exp.Select, exp.Subquery)):
        return True
    return False


def _is_relation_subquery(node: exp.Expression, root: exp.Expression) -> bool:
    """True only for FROM/JOIN/CTE relation bodies. Unknown correlation fails closed."""
    if node is root:
        return True
    current: exp.Expression | None = node
    while current is not None:
        parent = current.parent
        if parent is None:
            return current is root
        if isinstance(parent, exp.From):
            return True
        if isinstance(parent, exp.Join) and parent.this is current:
            return True
        if isinstance(parent, exp.CTE):
            return True
        if isinstance(parent, exp.With) and current in (parent.expressions or []):
            return True
        if isinstance(parent, (exp.Subquery, exp.Alias, exp.Paren, exp.TableAlias)):
            current = parent
            continue
        if isinstance(parent, exp.Select) and (
            current is parent.args.get("from") or current is parent.args.get("from_")
        ):
            return True
        return False
    return False


def _expr_alias(node: exp.Expression, dialect: str) -> str:
    rendered = node.sql(dialect=dialect, comments=False, pretty=False).strip().lower()
    rendered = re.sub(r"[^a-z0-9_]+", "_", rendered).strip("_")
    return rendered[:64] or "expr"


def _correspond(base: PlanGraph, pr: PlanGraph) -> tuple[dict[str, str], str | None]:
    mapping: dict[str, str] = {}
    reverse: dict[str, str] = {}

    def walk(base_id: str, pr_id: str) -> bool:
        if base_id in mapping:
            return mapping[base_id] == pr_id
        if pr_id in reverse:
            return False
        base_node = base.nodes.get(base_id)
        pr_node = pr.nodes.get(pr_id)
        if base_node is None or pr_node is None:
            return False
        if base_node.kind != pr_node.kind:
            return False
        if len(base_node.inputs) != len(pr_node.inputs):
            return False
        mapping[base_id] = pr_id
        reverse[pr_id] = base_id
        for left, right in zip(base_node.inputs, pr_node.inputs):
            if not walk(left, right):
                return False
        return True

    if not walk(base.root_id, pr.root_id):
        return {}, "old and new plans do not have an unambiguous 1:1 node correspondence"
    if len(mapping) != len(base.nodes) or len(reverse) != len(pr.nodes):
        return {}, "old and new plan skeletons are not isomorphic after alias resolution"
    return mapping, None


def _taint_analysis(
    graph: PlanGraph,
    changed_filter_id: str,
) -> tuple[tuple[str, str] | None, tuple[dict[str, str], ...], tuple[str, ...], tuple[str, ...]]:
    consumers: dict[str, list[str]] = defaultdict(list)
    for node in graph.nodes.values():
        for input_id in node.inputs:
            consumers[input_id].append(node.id)
    tainted: set[str] = set()
    queue: deque[str] = deque([changed_filter_id])
    tainted.add(changed_filter_id)
    while queue:
        current = queue.popleft()
        for consumer in consumers.get(current, ()):
            if consumer not in tainted:
                tainted.add(consumer)
                queue.append(consumer)

    join_taint: list[dict[str, str]] = []
    error: tuple[str, str] | None = None
    covered: list[str] = []
    operators: list[str] = []
    for node_id in _topo_from(graph, changed_filter_id):
        node = graph.node(node_id)
        if node_id not in tainted and node_id != changed_filter_id:
            continue
        covered.append(node_id)
        operators.append(node.kind)
        if node.kind not in {"source", "filter", "project", "innerjoin", "leftjoin", "group", "having"}:
            error = (
                UNSUPPORTED_OPERATION,
                f"operator {node.kind} on the path from the changed filter to the terminal group is excluded. {_SCOPE}",
            )
        if node.kind in JOIN_KINDS:
            left_tainted = node.inputs[0] in tainted
            right_tainted = node.inputs[1] in tainted
            if left_tainted and right_tainted:
                side = JOIN_TAINT_BOTH
                error = (
                    TWO_TAINTED_JOIN_INPUTS,
                    "both join inputs are tainted (including a reused CTE feeding both sides). " + _MATH_JOIN,
                )
            elif left_tainted:
                side = JOIN_TAINT_LEFT
            elif right_tainted:
                side = JOIN_TAINT_RIGHT
            else:
                side = JOIN_TAINT_NEITHER
            join_taint.append({"nodeId": node.id, "joinKind": node.kind, "taint": side})
    return error, tuple(join_taint), tuple(covered), tuple(operators)


def _topo_from(graph: PlanGraph, start_id: str) -> list[str]:
    consumers: dict[str, list[str]] = defaultdict(list)
    indegree: dict[str, int] = {start_id: 0}
    for node in graph.nodes.values():
        for input_id in node.inputs:
            consumers[input_id].append(node.id)
    seen = {start_id}
    queue: deque[str] = deque([start_id])
    while queue:
        current = queue.popleft()
        for consumer in consumers.get(current, ()):
            indegree[consumer] = indegree.get(consumer, 0) + 1
            if consumer not in seen:
                seen.add(consumer)
                queue.append(consumer)
    ready = deque([node_id for node_id, degree in indegree.items() if degree == 0])
    ordered: list[str] = []
    remaining = dict(indegree)
    while ready:
        node_id = ready.popleft()
        ordered.append(node_id)
        for consumer in consumers.get(node_id, ()):
            if consumer not in remaining:
                continue
            remaining[consumer] -= 1
            if remaining[consumer] == 0:
                ready.append(consumer)
    return ordered


def _key_lineage(
    graph: PlanGraph,
    *,
    entity_key: str,
) -> tuple[tuple[str, str] | None, tuple[dict[str, str], ...]]:
    key = entity_key.lower()
    traces: list[dict[str, str]] = []

    def origin_of(node: PlanNode, column: str) -> ColumnBinding:
        schema = _schema_of(node)
        found = schema.lookup(column)
        if found is not None:
            return found
        invented = _named_column_through_star(schema, column)
        if invented is not None:
            return invented
        return ColumnBinding(name=column, origin="untracked", kind="ambiguous")

    for node in graph.nodes.values():
        if node.kind in JOIN_KINDS:
            for left_key, right_key in node.join_keys:
                left_node = graph.node(node.inputs[0])
                right_node = graph.node(node.inputs[1])
                left_b = origin_of(left_node, left_key)
                right_b = origin_of(right_node, right_key)
                traces.append(_lineage_entry(left_key, left_b, expected=key))
                traces.append(_lineage_entry(right_key, right_b, expected=key))
                if left_b.kind != "column" or right_b.kind != "column":
                    return (
                        (
                            KEY_LINEAGE_BROKEN,
                            "a required join key was dropped, overwritten, ambiguous, or not tracked 1:1 "
                            "through projections",
                        ),
                        tuple(traces),
                    )

    terminal = _terminal_output(graph)
    entity = origin_of(terminal, key)
    entry = _lineage_entry(key, entity, expected=key)
    traces.append(entry)
    if entity.kind == "ambiguous" or entity.origin in {"ambiguous", "untracked"}:
        return (
            (
                KEY_LINEAGE_BROKEN,
                "entity key is ambiguous or untracked at the terminal output",
            ),
            tuple(traces),
        )
    if entity.kind == "literal" or entity.origin == "literal":
        return (
            (
                KEY_LINEAGE_BROKEN,
                "entity key was overwritten by a literal or expression; 1:1 rename is required",
            ),
            tuple(traces),
        )
    if entity.kind == "expr" and not entity.origin.startswith("agg:"):
        return (
            (
                KEY_LINEAGE_BROKEN,
                "entity key was overwritten by an expression; only a verified 1:1 rename is allowed",
            ),
            tuple(traces),
        )
    if entity.kind != "column":
        return (
            (
                KEY_LINEAGE_BROKEN,
                "entity key does not survive as a tracked source column through projections to GROUP BY",
            ),
            tuple(traces),
        )
    return None, tuple(_dedupe_lineage(traces))


def _lineage_entry(column: str, binding: ColumnBinding, *, expected: str) -> dict[str, str]:
    status = "tracked"
    entry: dict[str, str] = {"column": column, "origin": binding.origin}
    if binding.kind == "ambiguous" or binding.origin in {"ambiguous", "untracked"}:
        status = "ambiguous" if binding.origin == "ambiguous" else "untracked"
    elif binding.kind == "literal":
        status = "overwritten"
    elif binding.kind == "expr":
        status = "overwritten"
    elif column != expected and binding.kind == "column":
        status = "renamed"
        source_col = binding.origin.rsplit(".", 1)[-1]
        if source_col and source_col != column:
            entry["renameFrom"] = source_col
        else:
            status = "tracked"
    if column == expected and binding.kind == "column":
        source_col = binding.origin.rsplit(".", 1)[-1]
        if source_col and source_col != column:
            status = "renamed"
            entry["renameFrom"] = source_col
    entry["status"] = status
    return entry


def _dedupe_lineage(entries: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    seen: dict[tuple[str, str, str], dict[str, str]] = {}
    for entry in entries:
        key = (entry.get("column", ""), entry.get("origin", ""), entry.get("status", ""))
        seen[key] = entry
    return list(seen.values())


def _terminal_output(graph: PlanGraph) -> PlanNode:
    return graph.node(graph.root_id)


def _check_grouping_form(
    graph: PlanGraph,
    *,
    entity_key: str,
    lineage: tuple[dict[str, str], ...],
) -> tuple[str, str] | None:
    groups = [node for node in graph.nodes.values() if node.kind == "group"]
    if len(groups) != 1:
        return (
            UNSUPPORTED_OPERATION,
            "filter-v1 requires exactly one terminal ordinary GROUP BY on the tracked entity key. " + _SCOPE,
        )
    node = graph.node(graph.root_id)
    while node.kind in {"project", "having"}:
        if len(node.inputs) != 1:
            return (
                UNSUPPORTED_OPERATION,
                "post-group operators must be a key-preserving projection or an unchanged HAVING. " + _SCOPE,
            )
        node = graph.node(node.inputs[0])
    if node.kind != "group":
        return (
            UNSUPPORTED_OPERATION,
            "filter-v1 requires one terminal ordinary GROUP BY on the tracked entity key. " + _SCOPE,
        )
    payload = node.payload
    keys_part = payload.split(";", 1)[0]
    keys = [item for item in keys_part.removeprefix("keys=").split(",") if item]
    if len(keys) != 1:
        return (
            UNSUPPORTED_OPERATION,
            "filter-v1 grouping must be ordinary GROUP BY of the single tracked entity key. " + _SCOPE,
        )
    grouped = keys[0].lower()
    key = entity_key.lower()
    grouped_name = grouped.rsplit(".", 1)[-1]
    if grouped_name != key and grouped != key:
        entity_origin = next((item["origin"] for item in lineage if item.get("column") == key), "")
        origin_col = entity_origin.rsplit(".", 1)[-1] if entity_origin else ""
        if grouped_name != origin_col and grouped != entity_origin.lower():
            return (
                KEY_LINEAGE_BROKEN,
                "terminal GROUP BY is not the tracked entity key after alias resolution",
            )
    entity_status = next((item.get("status") for item in lineage if item.get("column") == key), None)
    if entity_status not in {"tracked", "renamed"}:
        return (
            KEY_LINEAGE_BROKEN,
            "entity key was not tracked 1:1 through projections to the terminal GROUP BY",
        )
    return None


def _predicate_totality(sql: str, changed: PlanNode, *, dialect: str) -> tuple[str, str] | None:
    del changed
    try:
        root = sqlglot.parse_one(sql, dialect=dialect)
    except SqlglotError:
        return None
    if root is None:
        return None
    for where in root.find_all(exp.Where):
        for node in where.walk():
            if isinstance(node, _NON_TOTAL_TYPES):
                return (
                    NON_TOTAL_PREDICATE,
                    "changed filter uses a non-total operator (division, modulo, or unsafe cast); "
                    "filter-v1 does not statically claim arbitrary casts or division cannot fail",
                )
            if isinstance(node, exp.Anonymous):
                name = (node.name or "").lower()
                if name in {"rand", "random", "uuid_string", "uuid"}:
                    return (
                        UNSUPPORTED_OPERATION,
                        f"nondeterministic function {name} is excluded from filter-v1. {_SCOPE}",
                    )
                if name not in {"", "ifnull", "nvl", "coalesce", "nullif", "iff"}:
                    return (
                        NON_TOTAL_PREDICATE,
                        "changed filter calls a function that is not on the filter-v1 totality allowlist",
                    )
            rand_cls = getattr(exp, "Rand", None)
            if rand_cls is not None and isinstance(node, rand_cls):
                return (
                    UNSUPPORTED_OPERATION,
                    f"nondeterministic function rand is excluded from filter-v1. {_SCOPE}",
                )
    return None


def _plan_fingerprint(graph: PlanGraph | None) -> str | None:
    if graph is None:
        return None
    rows = []
    for node_id in _all_topo(graph):
        node = graph.node(node_id)
        rows.append([node.id, node.kind, node.payload, list(node.inputs)])
    canonical = json.dumps(rows, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _all_topo(graph: PlanGraph) -> list[str]:
    indegree = {node_id: 0 for node_id in graph.nodes}
    consumers: dict[str, list[str]] = defaultdict(list)
    for node in graph.nodes.values():
        for input_id in node.inputs:
            consumers[input_id].append(node.id)
            indegree[node.id] = indegree.get(node.id, 0) + 1
    ready = deque([node_id for node_id, degree in indegree.items() if degree == 0])
    ordered: list[str] = []
    remaining = dict(indegree)
    while ready:
        node_id = ready.popleft()
        ordered.append(node_id)
        for consumer in consumers.get(node_id, ()):
            remaining[consumer] -= 1
            if remaining[consumer] == 0:
                ready.append(consumer)
    if len(ordered) != len(graph.nodes):
        return list(graph.nodes)
    return ordered


def _all_operator_kinds(graph: PlanGraph) -> tuple[str, ...]:
    return tuple(graph.node(node_id).kind for node_id in _all_topo(graph))


def _manifest_dependencies(
    version: int | None,
    fingerprint: str | None,
) -> list[dict[str, Any]]:
    if version is None or not fingerprint:
        return []
    return [
        {
            "manifestVersion": version,
            "fingerprint": fingerprint,
            "fields": ["targetModel", "entity", "entityKey"],
        }
    ]


def _reject(
    code: str,
    diagnostic: str,
    *,
    old_fp: str | None,
    new_fp: str | None,
    deps: tuple[int | None, str | None],
    graph: PlanGraph | None = None,
    matched: tuple[str, ...] = (),
    changed: str | None = None,
    join_taint: tuple[dict[str, str], ...] = (),
    covered: tuple[str, ...] = (),
    operators: tuple[str, ...] = (),
    lineage: tuple[dict[str, str], ...] = (),
) -> StaticEligibility:
    return StaticEligibility(
        eligible=False,
        reason_code=code,
        diagnostic=diagnostic,
        old_plan_fingerprint=old_fp,
        new_plan_fingerprint=new_fp,
        changed_filter_node_id=changed,
        matched_node_ids=matched or (tuple(sorted(graph.nodes)) if graph else ()),
        covered_node_ids=covered,
        operators=operators or (_all_operator_kinds(graph) if graph else ()),
        join_taint=join_taint,
        key_lineage=lineage,
        manifest_version=deps[0],
        manifest_fingerprint=deps[1],
    )


def _safe_diagnostic(text: str) -> str:
    cleaned = re.sub(r"'[^']*'", "'…'", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:512]


def schema_catalog_from_manifests(*manifests: Any) -> dict[str, tuple[str, ...]]:
    catalog: dict[str, tuple[str, ...]] = {}
    for manifest in manifests:
        if manifest is None:
            continue
        models = manifest.models() if hasattr(manifest, "models") else {}
        for node in models.values():
            columns = tuple(str(col).lower() for col in (getattr(node, "columns", ()) or ()) if col)
            if not columns:
                continue
            name = str(getattr(node, "name", "") or "").lower()
            unique_id = str(getattr(node, "unique_id", "") or "").lower()
            if name:
                catalog[name] = columns
            if unique_id:
                catalog[unique_id] = columns
            short = unique_id.rsplit(".", 1)[-1]
            if short:
                catalog[short] = columns
    return catalog


def compile_candidate_query(
    base_graph: PlanGraph,
    pr_graph: PlanGraph,
    mapping: dict[str, str],
    *,
    changed_base: PlanNode,
    changed_pr: PlanNode,
    entity_key: str,
    dialect: str,
) -> tuple[bool, tuple[str, str] | None, str | None, str | None]:
    """Compile RowCover transfer SQL from the matched 21B DAG.

    Failure is never an empty candidate set.
    """
    try:
        compiler = _CandidateCompiler(
            base_graph=base_graph,
            pr_graph=pr_graph,
            mapping=mapping,
            changed_base=changed_base,
            changed_pr=changed_pr,
            entity_key=entity_key.lower(),
            dialect=dialect,
        )
        sql = compiler.compile()
    except PlanError as error:
        return False, (error.code, error.diagnostic), None, None
    except SqlglotError as error:
        return False, (UNSUPPORTED_OPERATION, f"candidate SQL could not be constructed: {error}"), None, None
    if not sql or not sql.strip():
        return False, (UNSUPPORTED_OPERATION, "candidate compiler produced no SQL"), None, None
    return True, None, sql, sql_fingerprint(sql, dialect=dialect)


class _CandidateCompiler:
    def __init__(
        self,
        *,
        base_graph: PlanGraph,
        pr_graph: PlanGraph,
        mapping: dict[str, str],
        changed_base: PlanNode,
        changed_pr: PlanNode,
        entity_key: str,
        dialect: str,
    ) -> None:
        self.base_graph = base_graph
        self.pr_graph = pr_graph
        self.pr_to_base = {pr_id: base_id for base_id, pr_id in mapping.items()}
        self.changed_base = changed_base
        self.changed_pr = changed_pr
        self.entity_key = entity_key
        self.dialect = dialect
        self._n = 0
        self._tainted = _tainted_ids(pr_graph, changed_pr.id)
        self._relation_cache: dict[tuple[str, str], exp.Select] = {}

    def alias(self, prefix: str = "r") -> str:
        self._n += 1
        return f"{prefix}{self._n}"

    def compile(self) -> str:
        covers: dict[str, exp.Select] = {}
        covers[self.changed_pr.id] = self._flip_filter()
        for node_id in _topo_from(self.pr_graph, self.changed_pr.id):
            if node_id == self.changed_pr.id:
                continue
            node = self.pr_graph.node(node_id)
            if node.kind == "filter":
                covers[node_id] = covers[node.inputs[0]]
            elif node.kind == "project":
                covers[node_id] = self._project_cover(covers[node.inputs[0]], node, self.pr_graph)
            elif node.kind in JOIN_KINDS:
                covers[node_id] = self._join_cover(node, covers)
            elif node.kind == "having":
                covers[node_id] = covers[node.inputs[0]]
            elif node.kind == "group":
                covers[node_id] = self._distinct_keys(covers[node.inputs[0]], node)
            elif node.kind == "source":
                raise PlanError(
                    UNSUPPORTED_OPERATION,
                    "a source node cannot be downstream of the changed filter",
                )
            else:
                raise PlanError(
                    UNSUPPORTED_OPERATION,
                    f"operator {node.kind} has no filter-v1 RowCover transfer rule",
                )
        terminal = covers.get(self.pr_graph.root_id)
        if terminal is None:
            group = next((n for n in self.pr_graph.nodes.values() if n.kind == "group"), None)
            if group is None or group.id not in covers:
                raise PlanError(UNSUPPORTED_OPERATION, "candidate compiler did not reach the terminal GROUP BY")
            terminal = covers[group.id]
        return self._render(terminal)

    def _flip_filter(self) -> exp.Select:
        input_id = self.changed_pr.inputs[0]
        inner = self._relation(self.pr_graph, input_id)
        alias = self.alias("i")
        old_pred = self._qualify_pred(self._parse_pred(self.changed_base.payload), alias, self.pr_graph.node(input_id))
        new_pred = self._qualify_pred(self._parse_pred(self.changed_pr.payload), alias, self.pr_graph.node(input_id))
        flip = exp.NullSafeNEQ(
            this=exp.Paren(this=self._keep(old_pred)),
            expression=exp.Paren(this=self._keep(new_pred)),
        )
        select = exp.Select().from_(self._subquery(inner, alias)).where(flip)
        return self._select_node_columns(select, self.changed_pr, alias)

    def _project_cover(self, cover: exp.Select, node: PlanNode, graph: PlanGraph) -> exp.Select:
        alias = self.alias("p")
        select = exp.Select().from_(self._subquery(cover, alias))
        input_node = graph.node(node.inputs[0])
        input_names = {name.lower() for name in input_node.columns}
        if not node.columns:
            return select.select(exp.Star())
        exprs: list[exp.Expression] = []
        origin_by_name = {name.lower(): origin for name, origin in node.origins}
        for name in node.columns:
            source = name.lower()
            origin = origin_by_name.get(source, "")
            if origin.startswith("expr:"):
                raise PlanError(
                    UNSUPPORTED_OPERATION,
                    "RowCover projection of a computed expression is not a column transfer",
                )
            origin_col = origin.rsplit(".", 1)[-1].lower() if origin else source
            if source in input_names:
                col = source
            elif origin_col in input_names or not input_names:
                col = origin_col
            else:
                col = source
            exprs.append(exp.alias_(self._col(alias, col), name, quoted=True))
        return select.select(*exprs)

    def _join_cover(self, node: PlanNode, covers: dict[str, exp.Select]) -> exp.Select:
        left_id, right_id = node.inputs
        left_tainted = left_id in self._tainted
        right_tainted = right_id in self._tainted
        if left_tainted and right_tainted:
            raise PlanError(
                TWO_TAINTED_JOIN_INPUTS,
                "both join inputs are tainted; the candidate compiler must not apply a one-sided rule",
            )
        pairs = self._join_pairs(node, self.pr_graph)
        if node.kind == "innerjoin":
            if left_tainted:
                return self._equijoin(
                    covers[left_id],
                    self._relation(self.pr_graph, right_id),
                    node,
                    pairs,
                    inner=True,
                    graph=self.pr_graph,
                )
            return self._equijoin(
                self._relation(self.pr_graph, left_id),
                covers[right_id],
                node,
                pairs,
                inner=True,
                graph=self.pr_graph,
            )
        if node.kind != "leftjoin":
            raise PlanError(UNSUPPORTED_OPERATION, f"{node.kind} has no filter-v1 transfer rule")
        if left_tainted:
            return self._equijoin(
                covers[left_id],
                self._relation(self.pr_graph, right_id),
                node,
                pairs,
                inner=False,
                graph=self.pr_graph,
            )
        old_right = self._relation(self.base_graph, self.pr_to_base[right_id])
        new_right = self._relation(self.pr_graph, right_id)
        left_rel = self._relation(self.pr_graph, left_id)
        driving = self._left_rows_for_right_keys(left_rel, covers[right_id], node, pairs)
        old_out = self._equijoin(driving, old_right, node, pairs, inner=False, graph=self.pr_graph)
        new_out = self._equijoin(driving, new_right, node, pairs, inner=False, graph=self.pr_graph)
        return exp.union(old_out, new_out, distinct=True)

    def _left_rows_for_right_keys(
        self,
        left: exp.Select,
        right_cover: exp.Select,
        node: PlanNode,
        pairs: tuple[tuple[str, str], ...],
    ) -> exp.Select:
        left_alias = self.alias("a")
        key_alias = self.alias("k")
        cover_alias = self.alias("c")
        left_node = self.pr_graph.node(node.inputs[0])
        key_selects: list[exp.Expression] = []
        not_null: list[exp.Expression] = []
        for _left_key, right_key in pairs:
            col = self._col(cover_alias, right_key)
            key_selects.append(exp.alias_(col, right_key, quoted=True))
            not_null.append(exp.Not(this=exp.Is(this=self._col(cover_alias, right_key), expression=exp.null())))
        keys = exp.Select().from_(self._subquery(right_cover, cover_alias)).select(*key_selects).distinct()
        if not_null:
            keys = keys.where(exp.and_(*not_null) if len(not_null) > 1 else not_null[0])
        on_parts = [
            exp.EQ(this=self._col(left_alias, left_key), expression=self._col(key_alias, right_key))
            for left_key, right_key in pairs
        ]
        on = exp.and_(*on_parts) if len(on_parts) > 1 else on_parts[0]
        joined = (
            exp.Select()
            .from_(self._subquery(left, left_alias))
            .join(self._subquery(keys, key_alias), on=on, join_type="inner")
        )
        if left_node.columns:
            return joined.select(*[self._col(left_alias, name) for name in left_node.columns])
        return joined.select(exp.Star())

    def _equijoin(
        self,
        left: exp.Select,
        right: exp.Select,
        node: PlanNode,
        pairs: tuple[tuple[str, str], ...],
        *,
        inner: bool,
        graph: PlanGraph,
    ) -> exp.Select:
        left_alias = self.alias("l")
        right_alias = self.alias("r")
        on_parts = [
            exp.EQ(this=self._col(left_alias, left_key), expression=self._col(right_alias, right_key))
            for left_key, right_key in pairs
        ]
        on = exp.and_(*on_parts) if len(on_parts) > 1 else on_parts[0]
        join_type = "inner" if inner else "left"
        select = (
            exp.Select()
            .from_(self._subquery(left, left_alias))
            .join(self._subquery(right, right_alias), on=on, join_type=join_type)
        )
        left_node = graph.node(node.inputs[0])
        right_node = graph.node(node.inputs[1])
        exprs = self._join_output_exprs(left_alias, right_alias, left_node, right_node)
        return select.select(*exprs)

    def _join_output_exprs(
        self,
        left_alias: str,
        right_alias: str,
        left_node: PlanNode,
        right_node: PlanNode,
    ) -> list[exp.Expression]:
        seen: set[str] = set()
        exprs: list[exp.Expression] = []
        if not left_node.columns:
            star = exp.Star()
            star.set("table", exp.to_identifier(left_alias))
            exprs.append(star)
        for name in left_node.columns:
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            exprs.append(exp.alias_(self._col(left_alias, name), name, quoted=True))
        if not right_node.columns and not left_node.columns:
            star = exp.Star()
            star.set("table", exp.to_identifier(right_alias))
            exprs.append(star)
            return exprs
        for name in right_node.columns:
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            exprs.append(exp.alias_(self._col(right_alias, name), name, quoted=True))
        if not exprs:
            return [exp.Star()]
        return exprs

    def _distinct_keys(self, cover: exp.Select, node: PlanNode) -> exp.Select:
        alias = self.alias("g")
        key = self.entity_key
        input_node = self.pr_graph.node(node.inputs[0])
        names = {name.lower() for name in input_node.columns}
        if names and key not in names:
            for name, origin in input_node.origins:
                if origin.rsplit(".", 1)[-1].lower() == key or name.lower() == key:
                    key = name
                    break
        select = (
            exp.Select()
            .from_(self._subquery(cover, alias))
            .select(exp.alias_(self._col(alias, key), self.entity_key, quoted=True))
            .distinct()
        )
        return select.where(exp.Not(this=exp.Is(this=self._col(alias, key), expression=exp.null())))

    def _relation(self, graph: PlanGraph, node_id: str) -> exp.Select:
        cache_key = (id(graph), node_id)
        cached = self._relation_cache.get(cache_key)
        if cached is not None:
            return cached.copy()
        built = self._build_relation(graph, node_id)
        self._relation_cache[cache_key] = built
        return built.copy()

    def _build_relation(self, graph: PlanGraph, node_id: str) -> exp.Select:
        node = graph.node(node_id)
        if node.kind == "source":
            table = self._table(node.payload)
            alias = self.alias("s")
            table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
            select = exp.Select().from_(table)
            if node.columns:
                return select.select(*[self._col(alias, name) for name in node.columns])
            return select.select(exp.Star())
        if node.kind == "filter":
            inner = self._relation(graph, node.inputs[0])
            alias = self.alias("f")
            pred = self._qualify_pred(self._parse_pred(node.payload), alias, graph.node(node.inputs[0]))
            select = exp.Select().from_(self._subquery(inner, alias)).where(pred)
            return self._select_node_columns(select, node, alias)
        if node.kind == "project":
            inner = self._relation(graph, node.inputs[0])
            return self._project_cover(inner, node, graph)
        if node.kind in JOIN_KINDS:
            left = self._relation(graph, node.inputs[0])
            right = self._relation(graph, node.inputs[1])
            return self._equijoin(
                left,
                right,
                node,
                self._join_pairs(node, graph),
                inner=node.kind == "innerjoin",
                graph=graph,
            )
        if node.kind in {"group", "having"}:
            raise PlanError(
                UNSUPPORTED_OPERATION,
                "fixed-side reconstruction must not include the terminal GROUP BY",
            )
        raise PlanError(UNSUPPORTED_OPERATION, f"cannot reconstruct relation for {node.kind}")

    def _select_node_columns(
        self,
        select: exp.Select,
        node: PlanNode,
        alias: str,
    ) -> exp.Select:
        names = list(node.columns)
        if names:
            return select.select(*[self._col(alias, name) for name in names])
        return select.select(exp.Star())

    def _join_pairs(self, node: PlanNode, graph: PlanGraph) -> tuple[tuple[str, str], ...]:
        left = graph.node(node.inputs[0])
        right = graph.node(node.inputs[1])
        left_names = {name.lower() for name in left.columns}
        right_names = {name.lower() for name in right.columns}
        pairs: list[tuple[str, str]] = []
        for first, second in node.join_keys:
            a, b = first.lower(), second.lower()
            if a in left_names and b in right_names:
                pairs.append((a, b))
            elif b in left_names and a in right_names:
                pairs.append((b, a))
            else:
                pairs.append((a, b))
        if not pairs:
            raise PlanError(KEY_LINEAGE_BROKEN, "join keys were not available to the candidate compiler")
        return tuple(pairs)

    def _parse_pred(self, payload: str) -> exp.Expression:
        try:
            tree = sqlglot.parse_one(payload, dialect=self.dialect)
        except SqlglotError as error:
            raise PlanError(UNSUPPORTED_OPERATION, "changed filter predicate could not be reparsed") from error
        if tree is None:
            raise PlanError(UNSUPPORTED_OPERATION, "changed filter predicate could not be reparsed")
        return tree

    def _qualify_pred(self, pred: exp.Expression, alias: str, input_node: PlanNode) -> exp.Expression:
        copied = pred.copy()
        names = {name.lower() for name in input_node.columns}
        for column in copied.find_all(exp.Column):
            col = str(column.name or "").lower()
            if names and col not in names:
                origin_match = next(
                    (name for name, origin in input_node.origins if origin.rsplit(".", 1)[-1].lower() == col),
                    col,
                )
                col = origin_match
            column.set("table", exp.to_identifier(alias, quoted=True))
            column.set("this", exp.to_identifier(col, quoted=True))
        return copied

    def _keep(self, pred: exp.Expression) -> exp.Expression:
        """P IS TRUE ≡ COALESCE(P, FALSE). Snowflake sqlglot drops IS TRUE."""
        return exp.Coalesce(
            this=exp.Paren(this=pred.copy()),
            expressions=[exp.false()],
        )

    def _col(self, table: str, name: str) -> exp.Column:
        return exp.column(str(name), table=table, quoted=True)

    def _subquery(self, select: exp.Select, alias: str) -> exp.Subquery:
        return exp.Subquery(
            this=select.copy(),
            alias=exp.TableAlias(this=exp.to_identifier(alias)),
        )

    def _table(self, payload: str) -> exp.Table:
        try:
            parsed = sqlglot.parse_one(payload, dialect=self.dialect)
        except SqlglotError:
            parsed = None
        if isinstance(parsed, exp.Table):
            table = parsed.copy()
        else:
            table = exp.to_table(payload)
        for identifier in table.find_all(exp.Identifier):
            identifier.set("quoted", True)
        return table

    def _render(self, node: exp.Expression) -> str:
        return render_executable_sql(node, dialect=self.dialect)


def _tainted_ids(graph: PlanGraph, changed_filter_id: str) -> set[str]:
    consumers: dict[str, list[str]] = defaultdict(list)
    for node in graph.nodes.values():
        for input_id in node.inputs:
            consumers[input_id].append(node.id)
    tainted = {changed_filter_id}
    queue = deque([changed_filter_id])
    while queue:
        current = queue.popleft()
        for consumer in consumers.get(current, ()):
            if consumer not in tainted:
                tainted.add(consumer)
                queue.append(consumer)
    return tainted
