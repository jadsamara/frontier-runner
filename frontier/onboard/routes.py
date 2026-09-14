"""Derive and validate change-scoped semantic routes from dbt artifacts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable

from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.onboard.discover import (
    ModelSuggestion,
    SourceSuggestion,
    _entity_from_key,
    _is_candidate_mart,
    _source_models,
)

_ID_SUFFIX = "_id"


@dataclass(frozen=True)
class RouteHop:
    model: str
    column: str


@dataclass(frozen=True)
class DerivedRoute:
    name: str
    change_key: str
    join_route: str
    route_path: tuple[RouteHop, ...]
    route_status: str
    origin: str
    confidence: str
    evidence: tuple[str, ...]
    sql_change_blocker: bool
    cdc_blocker: bool
    mutation_policy: str = "targeted_repair"
    deletes_require_before_image: bool = True
    temporal_mode: str = "none"

    def to_source_suggestion(self) -> SourceSuggestion:
        return SourceSuggestion(
            name=self.name,
            change_key=self.change_key,
            join_route=self.join_route,
            mutation_policy=self.mutation_policy,
            deletes_require_before_image=self.deletes_require_before_image,
            temporal_mode=self.temporal_mode,
            event_time_column=None,
            maximum_lateness=None,
            confidence=self.confidence,
            origin=self.origin,
            route_status=self.route_status,
            route_path=self.route_path,
            evidence=self.evidence,
            sql_change_blocker=self.sql_change_blocker,
            cdc_blocker=self.cdc_blocker,
        )


def _columns(node: DbtNode) -> tuple[str, ...]:
    catalog = tuple(getattr(node, "columns", ()) or ())
    sql_cols = _columns_from_sql(node.compiled_code or "")
    seen: list[str] = []
    for column in (*catalog, *sql_cols):
        if column and column not in seen:
            seen.append(column)
    return tuple(seen)


def _columns_from_sql(sql: str) -> tuple[str, ...]:
    if not sql.strip():
        return ()
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return ()
    try:
        parsed = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception:
        return ()
    if parsed is None:
        return ()
    names: list[str] = []
    seen: set[str] = set()

    def add(name: str | None) -> None:
        if not name or name == "*" or name in seen:
            return
        seen.add(name)
        names.append(name)

    for alias in parsed.find_all(exp.Alias):
        add(alias.alias)
    for column in parsed.find_all(exp.Column):
        add(column.name)
    return tuple(names)


def unique_tested_columns(manifest: Manifest, node: DbtNode) -> tuple[str, ...]:
    columns = _columns(node)
    found: list[str] = []
    for test in manifest.tests_for(node.unique_id):
        name = test.name.lower()
        if "unique" not in name:
            continue
        if "unique_combination" in name:
            continue
        for column in columns:
            if column.lower() in name:
                if column not in found:
                    found.append(column)
    return tuple(found)


def _identity_column(node: DbtNode) -> str | None:
    columns = _columns(node)
    lowered = node.name.lower()
    matches = [
        column
        for column in columns
        if column.endswith(_ID_SUFFIX) and column[: -len(_ID_SUFFIX)].lower() in lowered
    ]
    if len(matches) == 1:
        return matches[0]
    if matches:
        longest = max(matches, key=len)
        return longest
    return None


def select_change_key(
    manifest: Manifest,
    node: DbtNode,
    *,
    entity_key: str,
) -> tuple[str | None, str, tuple[str, ...]]:
    """Prefer uniqueness tests, then declared identity, then structure. Never the target key."""
    evidence: list[str] = []
    tested = unique_tested_columns(manifest, node)
    if tested:
        key = tested[0]
        evidence.append(f"dbt uniqueness test covers {key}")
        return key, "high", tuple(evidence)
    identity = _identity_column(node)
    if identity and identity != entity_key:
        evidence.append(f"structurally inferred unique key {identity}")
        return identity, "medium", tuple(evidence)
    if identity == entity_key and entity_key in _columns(node):
        evidence.append(f"source contains the target entity key {entity_key}")
        return entity_key, "high", tuple(evidence)
    columns = _columns(node)
    if not columns:
        evidence.append("no catalog columns or compiled SQL columns were available")
        return None, "low", tuple(evidence)
    evidence.append("no uniqueness test or structural unique key was found")
    return None, "low", tuple(evidence)


def _join_column(upstream: DbtNode, downstream: DbtNode) -> str | None | str:
    """Return the shared join column, None if missing, or 'ambiguous'."""
    up_cols = set(_columns(upstream))
    down_cols = set(_columns(downstream))
    up_unique = unique_tested_columns(
        Manifest(project_name="", adapter_type=None, nodes={}, sources={}),
        upstream,
    )
    # unique_tested needs the full manifest; caller should pass tested sets.
    del up_unique
    shared_ids = [
        column
        for column in _columns(upstream)
        if column.endswith(_ID_SUFFIX) and column in down_cols
    ]
    up_identity = _identity_column(upstream)
    down_identity = _identity_column(downstream)
    if up_identity and up_identity in down_cols:
        return up_identity
    if down_identity and down_identity in up_cols:
        return down_identity
    if len(shared_ids) == 1:
        return shared_ids[0]
    if len(shared_ids) > 1:
        return "ambiguous"
    return None


def join_column(
    manifest: Manifest,
    upstream: DbtNode,
    downstream: DbtNode,
) -> str | None | str:
    up_cols = set(_columns(upstream))
    down_cols = set(_columns(downstream))
    up_tested = unique_tested_columns(manifest, upstream)
    down_tested = unique_tested_columns(manifest, downstream)
    for key in up_tested:
        if key in down_cols:
            return key
    for key in down_tested:
        if key in up_cols:
            return key
    return _join_column(upstream, downstream)


def _lineage_chain(manifest: Manifest, source: DbtNode, target: DbtNode) -> list[DbtNode] | None:
    if source.unique_id == target.unique_id:
        return [target]
    upstream = {node.unique_id: node for node in manifest.upstream_models(target.unique_id)}
    if source.unique_id not in upstream and source.unique_id != target.unique_id:
        return None
    parent: dict[str, str] = {}
    stack = [target.unique_id]
    seen = {target.unique_id}
    while stack:
        current_id = stack.pop()
        current = manifest.get(current_id)
        if current is None:
            continue
        for dep in current.depends_on:
            if dep in seen:
                continue
            node = manifest.get(dep)
            if node is None or node.resource_type != "model":
                continue
            seen.add(dep)
            parent[dep] = current_id
            if dep == source.unique_id:
                stack.clear()
                break
            stack.append(dep)
    if source.unique_id != target.unique_id and source.unique_id not in parent:
        return None
    chain_ids = [source.unique_id]
    while chain_ids[-1] != target.unique_id:
        nxt = parent.get(chain_ids[-1])
        if nxt is None:
            return None
        chain_ids.append(nxt)
    return [manifest.get(unique_id) for unique_id in chain_ids if manifest.get(unique_id)]


def derive_route_path(
    manifest: Manifest,
    source: DbtNode,
    target: DbtNode,
    *,
    change_key: str,
    entity_key: str,
) -> tuple[tuple[RouteHop, ...] | None, str, tuple[str, ...]]:
    source_cols = set(_columns(source))
    evidence: list[str] = []
    if change_key not in source_cols and source_cols:
        return (
            None,
            "INVALID",
            (f"column {change_key} does not exist on {source.name}",),
        )
    if entity_key in source_cols and change_key == entity_key:
        path = (RouteHop(source.name, entity_key), RouteHop(target.name, entity_key))
        if source.name == target.name:
            path = (RouteHop(source.name, entity_key),)
        evidence.append(f"{source.name} contains {entity_key}")
        return path, "VERIFIED", tuple(evidence)
    if entity_key in source_cols:
        path = (RouteHop(source.name, change_key), RouteHop(source.name, entity_key))
        if source.name != target.name:
            path = (*path, RouteHop(target.name, entity_key))
        evidence.append(f"{source.name} contains {entity_key} on the same row")
        return path, "VERIFIED", tuple(evidence)

    chain = _lineage_chain(manifest, source, target)
    if not chain or len(chain) < 2:
        return None, "UNRESOLVED", (f"no dbt ancestor path from {source.name} to {target.name}",)

    hops: list[RouteHop] = [RouteHop(source.name, change_key)]
    current_col = change_key
    ambiguous = False
    for index in range(len(chain) - 1):
        upstream = chain[index]
        downstream = chain[index + 1]
        edge = join_column(manifest, upstream, downstream)
        if edge == "ambiguous":
            ambiguous = True
            evidence.append(f"multiple join columns between {upstream.name} and {downstream.name}")
            continue
        if not edge:
            return (
                None,
                "UNRESOLVED",
                (
                    *evidence,
                    f"no join column from {upstream.name} to {downstream.name}",
                ),
            )
        up_cols = set(_columns(upstream))
        down_cols = set(_columns(downstream))
        if edge not in up_cols and up_cols:
            return (
                None,
                "INVALID",
                (f"column {edge} does not exist on {upstream.name}",),
            )
        if edge not in down_cols and down_cols:
            return (
                None,
                "INVALID",
                (f"column {edge} does not exist on {downstream.name}",),
            )
        if current_col != edge:
            hops.append(RouteHop(upstream.name, edge))
        hops.append(RouteHop(downstream.name, edge))
        current_col = edge
        evidence.append(f"{upstream.name}.{edge} → {downstream.name}.{edge}")

    if current_col != entity_key:
        last = chain[-1]
        last_cols = set(_columns(last))
        if entity_key in last_cols:
            hops.append(RouteHop(last.name, entity_key))
            evidence.append(f"{last.name} carries {entity_key} on the same row")
        elif entity_key in set(_columns(chain[-2])) if len(chain) > 1 else set():
            hops.append(RouteHop(chain[-2].name, entity_key))
            hops.append(RouteHop(last.name, entity_key))
            evidence.append(f"{chain[-2].name}.{entity_key} → {last.name}.{entity_key}")
        else:
            return (
                None,
                "UNRESOLVED",
                (*evidence, f"route does not reach {entity_key}"),
            )

    if hops[-1].column != entity_key:
        return None, "UNRESOLVED", (*evidence, f"route does not reach {entity_key}")
    status = "AMBIGUOUS" if ambiguous else "VERIFIED"
    confidence_note = "shortest dbt lineage path"
    evidence.append(confidence_note)
    return tuple(hops), status, tuple(evidence)


def compile_route_query(path: Iterable[RouteHop], *, change_key: str, entity_key: str) -> str:
    hops = list(path)
    if not hops:
        raise ValueError("route path is empty")
    aliases: dict[str, str] = {}
    from_model = hops[0].model
    aliases[from_model] = "src"
    join_sql: list[str] = []
    current_model = from_model
    current_column = hops[0].column
    alias_n = 1
    for hop in hops[1:]:
        if hop.model == current_model:
            current_column = hop.column
            continue
        if hop.model not in aliases:
            aliases[hop.model] = f"hop{alias_n}"
            alias_n += 1
        left = aliases[current_model]
        right = aliases[hop.model]
        join_sql.append(
            f"inner join {{{{ ref('{hop.model}') }}}} as {right} "
            f"on {right}.{hop.column} = {left}.{current_column}"
        )
        current_model = hop.model
        current_column = hop.column
    target_alias = aliases[current_model]
    joins = ("\n" + "\n".join(join_sql)) if join_sql else ""
    return (
        f"select distinct {target_alias}.{entity_key}\n"
        f"from {{{{ ref('{from_model}') }}}} as src"
        f"{joins}\n"
        f"where src.{change_key} in ({{{{ changed_values }}}})"
    )


def format_join_route(path: Iterable[RouteHop], *, change_key: str, entity_key: str) -> str:
    hops = list(path)
    if len(hops) <= 1 or (len(hops) == 2 and hops[0].column == entity_key):
        return "direct"
    columns: list[str] = []
    for hop in hops:
        if not columns or columns[-1] != hop.column:
            columns.append(hop.column)
    if columns[0] != change_key:
        columns.insert(0, change_key)
    if columns[-1] != entity_key:
        columns.append(entity_key)
    return " -> ".join(columns)


def derive_source_route(
    manifest: Manifest,
    source: DbtNode,
    target: DbtNode,
    *,
    entity_key: str,
) -> DerivedRoute:
    source_cols = set(_columns(source))
    change_key, key_confidence, key_evidence = select_change_key(
        manifest,
        source,
        entity_key=entity_key,
    )
    evidence = list(key_evidence)
    if entity_key in source_cols and (change_key in {None, entity_key}):
        return DerivedRoute(
            name=source.name,
            change_key=entity_key,
            join_route="direct",
            route_path=(RouteHop(source.name, entity_key),),
            route_status="VERIFIED",
            origin="derived",
            confidence="high" if key_confidence == "high" else "medium",
            evidence=tuple([*evidence, f"column {entity_key} exists on {source.name}"]),
            sql_change_blocker=False,
            cdc_blocker=False,
            deletes_require_before_image=False,
        )
    if entity_key in source_cols and change_key and change_key != entity_key:
        # Direct is only valid when the change key is the entity key.
        pass
    if change_key is None:
        return DerivedRoute(
            name=source.name,
            change_key="unresolved",
            join_route="unresolved",
            route_path=(),
            route_status="UNRESOLVED",
            origin="derived",
            confidence="low",
            evidence=tuple(evidence),
            sql_change_blocker=False,
            cdc_blocker=True,
        )
    if change_key == "unresolved":
        change_key = None
    if not change_key:
        return DerivedRoute(
            name=source.name,
            change_key="unresolved",
            join_route="unresolved",
            route_path=(),
            route_status="UNRESOLVED",
            origin="derived",
            confidence="low",
            evidence=tuple(evidence),
            sql_change_blocker=False,
            cdc_blocker=True,
        )
    if change_key not in source_cols and source_cols:
        path, status, path_evidence = derive_route_path(
            manifest,
            source,
            target,
            change_key=change_key,
            entity_key=entity_key,
        )
        return DerivedRoute(
            name=source.name,
            change_key=change_key,
            join_route=format_join_route(path or (), change_key=change_key, entity_key=entity_key)
            if path
            else "unresolved",
            route_path=path or (),
            route_status="INVALID" if status == "INVALID" else status,
            origin="derived",
            confidence="low",
            evidence=tuple([*evidence, *path_evidence]),
            sql_change_blocker=False,
            cdc_blocker=True,
        )
    path, status, path_evidence = derive_route_path(
        manifest,
        source,
        target,
        change_key=change_key,
        entity_key=entity_key,
    )
    evidence.extend(path_evidence)
    join_route = (
        format_join_route(path, change_key=change_key, entity_key=entity_key)
        if path
        else "unresolved"
    )
    if status == "INVALID":
        confidence = "low"
    elif status == "VERIFIED":
        confidence = "high" if key_confidence == "high" else "medium"
    else:
        confidence = "medium" if path else "low"
    return DerivedRoute(
        name=source.name,
        change_key=change_key,
        join_route=join_route,
        route_path=path or (),
        route_status=status,
        origin="derived",
        confidence=confidence,
        evidence=tuple(evidence),
        sql_change_blocker=False,
        cdc_blocker=status != "VERIFIED",
        deletes_require_before_image=join_route != "direct",
    )


def reject_direct_without_column(
    source: DbtNode,
    *,
    entity_key: str,
) -> bool:
    return entity_key not in set(_columns(source))


def derive_target(
    manifest: Manifest,
    target: DbtNode,
) -> ModelSuggestion:
    tested = unique_tested_columns(manifest, target)
    entity_key = tested[0] if tested else (_identity_column(target) or "id")
    entity = _entity_from_key(entity_key, target.name)
    sources = tuple(
        derive_source_route(manifest, source, target, entity_key=entity_key).to_source_suggestion()
        for source in _source_models(manifest, target)
    )
    verified = sum(1 for source in sources if source.route_status == "VERIFIED")
    unresolved = sum(1 for source in sources if source.route_status in {"UNRESOLVED", "AMBIGUOUS"})
    invalid = sum(1 for source in sources if source.route_status == "INVALID")
    overall = "high" if invalid == 0 and unresolved == 0 else ("medium" if invalid == 0 else "low")
    return ModelSuggestion(
        model=target.name,
        entity=entity,
        entity_key=entity_key,
        grain=f"one_row_per_{entity}",
        sources=sources,
        confidence=overall,
        reasons=(
            f"Selected {target.name} as the assessment target",
            f"Entity key {entity_key}",
            f"Verified routes: {verified}",
            f"Unresolved routes: {unresolved}",
            f"Invalid routes: {invalid}",
        ),
        target_unique_id=target.unique_id,
    )


def suggest_generated_models(manifest: Manifest) -> list[ModelSuggestion]:
    marts = [
        node
        for node in manifest.models().values()
        if node.package_name in {None, manifest.project_name} and _is_candidate_mart(node)
    ]
    return [
        derive_target(manifest, mart)
        for mart in sorted(marts, key=lambda node: node.unique_id)
        if _source_models(manifest, mart)
    ]


def merge_human_overrides(
    generated: ModelSuggestion,
    existing: dict[str, Any] | None,
    manifest: Manifest,
    target: DbtNode,
) -> ModelSuggestion:
    if not existing:
        return generated
    existing_sources = {
        str(item.get("name") or ""): item
        for item in (
            existing.get("sources")
            or (existing.get("document") or {}).get("sources")
            or []
        )
        if isinstance(item, dict)
    }
    merged: list[SourceSuggestion] = []
    discarded: list[SourceSuggestion] = []
    for source in generated.sources:
        override = existing_sources.get(source.name)
        if not override or override.get("origin") != "confirmed":
            merged.append(source)
            continue
        node = None
        try:
            node = manifest.find_model(source.name)
        except Exception:
            node = None
        change_key = str(override.get("changeKey") or "")
        join_route = str(override.get("joinRoute") or "")
        valid = True
        evidence = ["human override"]
        if node is not None:
            columns = set(_columns(node))
            if columns and change_key not in columns:
                valid = False
                evidence.append(f"column {change_key} does not exist on {source.name}")
            if join_route == "direct" and generated.entity_key not in columns and columns:
                valid = False
                evidence.append(
                    f"direct route requires {generated.entity_key} on {source.name}"
                )
        if not valid:
            discarded.append(
                replace(
                    source,
                    change_key=change_key or source.change_key,
                    join_route=join_route or source.join_route,
                    origin="confirmed",
                    route_status="INVALID",
                    route_path=(),
                    confidence="low",
                    evidence=tuple(evidence),
                    sql_change_blocker=False,
                    cdc_blocker=True,
                )
            )
            merged.append(source)
            continue
        merged.append(
            replace(
                source,
                change_key=change_key or source.change_key,
                join_route=join_route or source.join_route,
                origin="confirmed",
                route_status="VERIFIED",
                route_path=(),
                confidence=str(override.get("confidence") or "high"),
                mutation_policy=str(override.get("mutationPolicy") or source.mutation_policy),
                deletes_require_before_image=bool(
                    override.get("deletesRequireBeforeImage", source.deletes_require_before_image)
                ),
                temporal_mode=str(override.get("temporalMode") or source.temporal_mode),
                event_time_column=override.get("eventTimeColumn"),
                evidence=("human override still valid",),
                sql_change_blocker=False,
                cdc_blocker=False,
            )
        )
    return replace(generated, sources=tuple(merged), discarded_overrides=tuple(discarded))


def readiness(suggestion: ModelSuggestion) -> tuple[bool, bool, int, int, int]:
    verified = sum(1 for source in suggestion.sources if source.route_status == "VERIFIED")
    unresolved = sum(
        1 for source in suggestion.sources if source.route_status in {"UNRESOLVED", "AMBIGUOUS"}
    )
    invalid = sum(1 for source in suggestion.sources if source.route_status == "INVALID")
    sql_ready = invalid == 0 or all(not source.sql_change_blocker for source in suggestion.sources)
    sql_ready = all(
        source.route_status == "VERIFIED"
        for source in suggestion.sources
        if source.sql_change_blocker
    )
    cdc_ready = unresolved == 0 and invalid == 0
    return sql_ready, cdc_ready, verified, unresolved, invalid


def impact_returns_entity_key(sql: str | None, entity_key: str) -> bool:
    if not sql or not entity_key:
        return False
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return entity_key.lower() in sql.lower()
    try:
        parsed = sqlglot.parse_one(sql, dialect="snowflake")
    except Exception:
        return entity_key.lower() in sql.lower()
    if parsed is None:
        return False
    for column in parsed.find_all(exp.Column):
        if (column.name or "").lower() == entity_key.lower():
            return True
    return False


def _source_name(source: Any) -> str:
    return str(getattr(source, "name", "") or "")


def _source_status(source: Any) -> str:
    return str(
        getattr(source, "route_status", None)
        or (source.get("routeStatus") if isinstance(source, dict) else "")
        or ""
    )


def sql_change_required_sources(
    sources: Iterable[Any],
    *,
    impact_sql: str | None,
    entity_key: str,
    changed_models: Iterable[str] = (),
) -> tuple[str, ...]:
    """Routes that must be verified for this SQL-change assessment."""
    if impact_returns_entity_key(impact_sql, entity_key):
        return ()
    changed = {str(name).lower() for name in changed_models if str(name).strip()}
    required: list[str] = []
    for source in sources:
        name = _source_name(source) if not isinstance(source, dict) else str(source.get("name") or "")
        status = _source_status(source)
        if not name or status == "VERIFIED":
            continue
        if changed and name.lower() not in changed:
            continue
        required.append(name)
    return tuple(required)


def cdc_unresolved_for_batch(
    sources: Iterable[Any],
    batch_sources: Iterable[str],
) -> tuple[str, ...]:
    needed = {str(name).lower() for name in batch_sources if str(name).strip()}
    blocked: list[str] = []
    for source in sources:
        name = _source_name(source) if not isinstance(source, dict) else str(source.get("name") or "")
        status = _source_status(source)
        if name.lower() in needed and status != "VERIFIED":
            blocked.append(name)
    return tuple(blocked)
