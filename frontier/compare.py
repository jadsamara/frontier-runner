from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from frontier.config import ConfigError
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.execute import is_sql_change_impact_model
from frontier.impact import (
    CANDIDATE_SET_ANALYSIS_FAILED,
    CANDIDATE_SET_NOT_EVALUATED,
    COMPILED,
    FULL_REBUILD_REQUIRED,
    ImpactCompileResult,
    compile_impact_query,
)
from frontier.sql_fingerprint import sql_dialect, sql_fingerprint, using_sql_dialect
from frontier.filter_v1 import (
    UNSUPPORTED_OPERATION,
    StaticEligibility,
    analyze_static_eligibility,
    assert_eligibility_payload_is_safe,
    schema_catalog_from_manifests,
)
from frontier.snowflake_sql import (
    classify_sql_change,
    describe_sql_change,
    narrow_frontier_safe,
)


def compiled_sql_for(node: DbtNode, compiled_root: Path | None = None) -> str | None:
    if node.compiled_code and node.compiled_code.strip():
        return node.compiled_code
    if compiled_root is None or not node.original_file_path:
        return None
    relative = Path(node.original_file_path)
    candidates = []
    if node.package_name:
        candidates.append(compiled_root / node.package_name / relative)
    candidates.append(compiled_root / relative)
    for path in candidates:
        if path.is_file():
            text = path.read_text(encoding="utf-8")
            if text.strip():
                return text
    return None


def compiled_sql_for_name(
    manifest: Manifest | None,
    name: str,
    compiled_root: Path | None = None,
) -> str | None:
    if manifest is None or not name:
        return None
    try:
        node = manifest.find_model(name)
    except ConfigError:
        return None
    return compiled_sql_for(node, compiled_root)


def compiled_sql_pair_for_sql_change(
    *,
    target_name: str,
    pr_manifest: Manifest,
    base_manifest: Manifest | None,
    pr_compiled_root: Path | None = None,
    base_compiled_root: Path | None = None,
    sql_comparison: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    """Compiled SQL used to confirm and prove a SQL change.

    The configured mart often only `select`s from a changed intermediate
    model. Proving that wrapper against the already-built PR relation
    makes before and after identical, so confirmed=0 and every candidate
    looks extra. Prefer the production model whose compiled SQL actually
    changed.
    """
    names: list[str] = []
    for row in (sql_comparison or {}).get("modified") or []:
        name = str(row.get("name") or "")
        if (
            is_sql_change_impact_model(
                name,
                tags=tuple(row.get("tags") or ()),
                target_name=target_name,
            )
            and name not in names
        ):
            names.append(name)
    if target_name and target_name not in names:
        names.append(target_name)

    fallback: tuple[str | None, str | None] = (None, None)
    for name in names:
        after = compiled_sql_for_name(pr_manifest, name, pr_compiled_root)
        before = compiled_sql_for_name(base_manifest, name, base_compiled_root)
        if before and after:
            if before.strip() != after.strip():
                return before, after
            if fallback == (None, None):
                fallback = (before, after)
        elif fallback == (None, None):
            fallback = (before, after)
    return fallback


def sql_change_reference_relation(
    *,
    target_name: str,
    pr_manifest: Manifest,
    sql_comparison: dict[str, Any] | None = None,
) -> str | None:
    """Already-built PR relation for the production model whose SQL changed."""
    names: list[str] = []
    for row in (sql_comparison or {}).get("modified") or []:
        name = str(row.get("name") or "")
        if (
            is_sql_change_impact_model(
                name,
                tags=tuple(row.get("tags") or ()),
                target_name=target_name,
            )
            and name not in names
        ):
            names.append(name)
    if target_name and target_name not in names:
        names.append(target_name)
    for name in names:
        try:
            node = pr_manifest.find_model(name)
        except ConfigError:
            continue
        relation = (node.relation or "").strip()
        if relation:
            return relation
    return None


def model_sql_fingerprint(
    node: DbtNode,
    *,
    dialect: str | None,
    compiled_root: Path | None = None,
) -> str:
    sql = compiled_sql_for(node, compiled_root) or ""
    return sql_fingerprint(sql, dialect=dialect)


def artifact_fingerprint(model_fingerprints: dict[str, str]) -> str:
    canonical = json.dumps(
        [[unique_id, model_fingerprints[unique_id]] for unique_id in sorted(model_fingerprints)],
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _ref(node: DbtNode) -> dict[str, str]:
    return {"uniqueId": node.unique_id, "name": node.name}


def _downstream_payload(manifest: Manifest, unique_id: str) -> list[dict[str, str]]:
    return [_ref(node) for node in manifest.downstream_models(unique_id)]


def _model_payload(
    *,
    unique_id: str,
    name: str,
    base_fingerprint: str | None,
    pr_fingerprint: str | None,
    downstream: list[dict[str, str]],
    change_kinds: list[str] | None = None,
    unsafe: bool | None = None,
    unsupported_reasons: list[str] | None = None,
    impact: dict[str, Any] | None = None,
    change_summary: str | None = None,
    tags: tuple[str, ...] | list[str] | None = None,
    static_eligibility: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uniqueId": unique_id,
        "name": name,
        "baseFingerprint": base_fingerprint,
        "prFingerprint": pr_fingerprint,
        "downstream": downstream,
    }
    if tags:
        payload["tags"] = list(tags)
    if change_kinds:
        payload["changeKinds"] = change_kinds
    if unsafe is not None:
        payload["unsafe"] = unsafe
    if unsupported_reasons:
        payload["unsupportedReasons"] = unsupported_reasons
    if impact:
        payload.update(impact)
    if change_summary:
        payload["changeSummary"] = change_summary[:512]
    if static_eligibility:
        payload["staticEligibility"] = static_eligibility
    if extra:
        payload.update(extra)
    return payload


@dataclass(frozen=True)
class SqlComparison:
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.payload))


def compare_manifests(
    base: Manifest,
    pr: Manifest,
    *,
    base_compiled_root: Path | None = None,
    pr_compiled_root: Path | None = None,
    base_commit_sha: str | None = None,
    pr_commit_sha: str | None = None,
    entity_key: str | None = None,
    confirmed_keys: Iterable[str] | None = None,
    target_name: str | None = None,
    semantic_manifest_version: int | None = None,
    semantic_manifest_fingerprint: str | None = None,
) -> SqlComparison:
    dialect = sql_dialect(pr.adapter_type or base.adapter_type) or "snowflake"
    with using_sql_dialect(dialect):
        return _compare_manifests(
            base,
            pr,
            dialect=dialect,
            base_compiled_root=base_compiled_root,
            pr_compiled_root=pr_compiled_root,
            base_commit_sha=base_commit_sha,
            pr_commit_sha=pr_commit_sha,
            entity_key=entity_key,
            confirmed_keys=confirmed_keys,
            target_name=target_name,
            semantic_manifest_version=semantic_manifest_version,
            semantic_manifest_fingerprint=semantic_manifest_fingerprint,
        )


def _compare_manifests(
    base: Manifest,
    pr: Manifest,
    *,
    dialect: str,
    base_compiled_root: Path | None,
    pr_compiled_root: Path | None,
    base_commit_sha: str | None,
    pr_commit_sha: str | None,
    entity_key: str | None,
    confirmed_keys: Iterable[str] | None,
    target_name: str | None,
    semantic_manifest_version: int | None,
    semantic_manifest_fingerprint: str | None,
) -> SqlComparison:
    base_models = base.models()
    pr_models = pr.models()
    base_prints = {
        unique_id: model_sql_fingerprint(
            node,
            dialect=dialect,
            compiled_root=base_compiled_root,
        )
        for unique_id, node in base_models.items()
    }
    pr_prints = {
        unique_id: model_sql_fingerprint(
            node,
            dialect=dialect,
            compiled_root=pr_compiled_root,
        )
        for unique_id, node in pr_models.items()
    }

    added: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    modified: list[dict[str, Any]] = []
    schema_catalog = schema_catalog_from_manifests(base, pr)
    added_removed_impact = {
        "impactStatus": FULL_REBUILD_REQUIRED,
        "candidateSetState": "analysis_failed",
        "impactReasons": ["model added or removed"],
    }

    for unique_id in sorted(pr_models):
        node = pr_models[unique_id]
        if unique_id not in base_models:
            added.append(
                _model_payload(
                    unique_id=unique_id,
                    name=node.name,
                    base_fingerprint=None,
                    pr_fingerprint=pr_prints[unique_id],
                    downstream=_downstream_payload(pr, unique_id),
                    tags=node.tags,
                    impact=(
                        added_removed_impact
                        if is_sql_change_impact_model(
                            node.name,
                            tags=node.tags,
                            target_name=target_name,
                        )
                        else None
                    ),
                )
            )
            continue
        if base_prints[unique_id] == pr_prints[unique_id]:
            continue
        base_sql = compiled_sql_for(base_models[unique_id], base_compiled_root) or ""
        pr_sql = compiled_sql_for(node, pr_compiled_root) or ""
        classification = classify_sql_change(base_sql, pr_sql, dialect=dialect)
        if not classification.kinds:
            continue
        compile_impact = is_sql_change_impact_model(
            node.name,
            tags=node.tags,
            target_name=target_name,
        )
        eligibility_result = analyze_static_eligibility(
            base_sql,
            pr_sql,
            entity_key=entity_key or "",
            dialect=dialect,
            manifest_version=semantic_manifest_version,
            manifest_fingerprint=semantic_manifest_fingerprint,
            target_model=node.name,
            schema_catalog=schema_catalog,
        )
        eligibility = eligibility_result.to_payload()
        assert_eligibility_payload_is_safe(eligibility)
        impact = None
        extra: dict[str, Any] | None = None
        if compile_impact:
            if eligibility_result.eligible:
                impact = _filter_v1_impact(eligibility_result, entity_key=entity_key or "")
            else:
                diagnostic = compile_impact_query(
                    base_sql,
                    pr_sql,
                    entity_key=entity_key or "",
                    confirmed_keys=confirmed_keys or (),
                    classification=classification,
                    dialect=dialect,
                )
                extra = {
                    "legacyImpactCompilation": diagnostic.status,
                }
                if diagnostic.candidate_sql:
                    extra["legacyImpactSql"] = diagnostic.candidate_sql
                reason = eligibility_result.reason_code or "UNCERTIFIED"
                diagnostic_reason = eligibility_result.diagnostic
                reasons = [f"filter-v1 UNCERTIFIED ({reason})"]
                if diagnostic_reason:
                    reasons.append(diagnostic_reason)
                reasons.append("legacy impact compilation is diagnostic only and must not be executed")
                impact = ImpactCompileResult(
                    status=FULL_REBUILD_REQUIRED,
                    reasons=tuple(reasons),
                    entity_key=entity_key or "",
                    candidate_sql=None,
                    parameterized_sql=None,
                    parameters=(),
                    query_fingerprint=None,
                    candidate_set_state=CANDIDATE_SET_ANALYSIS_FAILED,
                )
        modified.append(
            _model_payload(
                unique_id=unique_id,
                name=node.name,
                base_fingerprint=base_prints[unique_id],
                pr_fingerprint=pr_prints[unique_id],
                downstream=_downstream_payload(pr, unique_id),
                change_kinds=list(classification.kinds),
                unsafe=classification.unsafe if compile_impact else None,
                unsupported_reasons=list(classification.unsupported_reasons) or None,
                impact=impact.to_payload(include_sql=True) if impact is not None else None,
                change_summary=describe_sql_change(base_sql, pr_sql),
                tags=node.tags,
                static_eligibility=eligibility,
                extra=extra,
            )
        )

    for unique_id in sorted(base_models):
        if unique_id in pr_models:
            continue
        node = base_models[unique_id]
        removed.append(
            _model_payload(
                unique_id=unique_id,
                name=node.name,
                base_fingerprint=base_prints[unique_id],
                pr_fingerprint=None,
                downstream=_downstream_payload(base, unique_id),
                tags=node.tags,
                impact=(
                    added_removed_impact
                    if is_sql_change_impact_model(
                        node.name,
                        tags=node.tags,
                        target_name=target_name,
                    )
                    else None
                ),
            )
        )

    base_side: dict[str, Any] = {
        "fingerprint": artifact_fingerprint(base_prints),
        "modelCount": len(base_prints),
    }
    pr_side: dict[str, Any] = {
        "fingerprint": artifact_fingerprint(pr_prints),
        "modelCount": len(pr_prints),
    }
    if base_commit_sha:
        base_side["commitSha"] = base_commit_sha
    if pr_commit_sha:
        pr_side["commitSha"] = pr_commit_sha

    impact_rows = [
        row
        for row in (*added, *removed, *modified)
        if is_sql_change_impact_model(
            str(row.get("name") or ""),
            tags=tuple(row.get("tags") or ()),
            target_name=target_name,
        )
    ]
    full_rebuild = any(row.get("impactStatus") == FULL_REBUILD_REQUIRED for row in impact_rows)
    payload = {
        "base": base_side,
        "pr": pr_side,
        "added": added,
        "removed": removed,
        "modified": modified,
        "narrowFrontierSafe": narrow_frontier_safe({"modified": [
            row for row in modified
            if is_sql_change_impact_model(
                str(row.get("name") or ""),
                tags=tuple(row.get("tags") or ()),
                target_name=target_name,
            )
        ]}),
        "fullRebuildRequired": full_rebuild,
    }
    top_eligibility = _top_level_eligibility(modified, target_name=target_name)
    if top_eligibility:
        payload["staticEligibility"] = top_eligibility
        if top_eligibility.get("eligible") is False:
            payload["narrowFrontierSafe"] = False
            payload["fullRebuildRequired"] = True
    return SqlComparison(payload)


def format_compare_report(comparison: dict[str, Any]) -> str:
    base = comparison["base"]
    pr = comparison["pr"]
    lines = [
        f"Base artifact: {base['fingerprint']}",
        f"PR artifact:   {pr['fingerprint']}",
        f"Models: {base['modelCount']} base, {pr['modelCount']} PR",
    ]
    if base.get("commitSha") or pr.get("commitSha"):
        lines.append(
            f"Commits: base={base.get('commitSha') or '—'} pr={pr.get('commitSha') or '—'}"
        )

    def section(title: str, rows: list[dict[str, Any]]) -> None:
        lines.extend(["", f"{title}:"])
        if not rows:
            lines.append("  (none)")
            return
        for row in rows:
            lines.append(f"  - {row['name']} ({row['uniqueId']})")
            downstream = row.get("downstream") or []
            if downstream:
                names = ", ".join(item["name"] for item in downstream)
                lines.append(f"    downstream: {names}")
            else:
                lines.append("    downstream: (none)")
            kinds = row.get("changeKinds") or []
            if kinds:
                lines.append(f"    change kinds: {', '.join(kinds)}")
            summary = row.get("changeSummary")
            if summary:
                lines.append(f"    change: {summary}")
            if row.get("unsafe"):
                lines.append("    unsafe: narrow frontier is not allowed")
            impact_status = row.get("impactStatus")
            if impact_status:
                lines.append(f"    impact compilation: {impact_status}")
            execution = row.get("impactExecution")
            if execution:
                lines.append(f"    impact execution: {execution}")
            reasons = row.get("impactReasons") or []
            if reasons:
                lines.append(f"    impact reasons: {', '.join(str(reason) for reason in reasons)}")
            if row.get("candidateSql"):
                compiled = (row.get("staticEligibility") or {}).get("compiled")
                label = "filter-v1 candidate sql" if compiled else "candidate sql"
                lines.append(f"    {label}: {row['candidateSql']}")
            if row.get("legacyImpactSql"):
                lines.append(
                    "    legacy impact sql (not certified; not executed): "
                    + str(row["legacyImpactSql"])
                )
            if row.get("legacyImpactCompilation"):
                lines.append(
                    f"    legacy impact compilation: {row['legacyImpactCompilation']} (diagnostic only)"
                )
            eligibility = row.get("staticEligibility") or {}
            if eligibility:
                lines.append(_format_eligibility_line(eligibility, indent="    "))

    section("Added", comparison.get("added") or [])
    section("Removed", comparison.get("removed") or [])
    section("Modified", comparison.get("modified") or [])
    eligibility = comparison.get("staticEligibility") or {}
    certified = eligibility.get("eligible") is True and eligibility.get("compiled") is True
    safe = comparison.get("narrowFrontierSafe")
    if eligibility.get("eligible") is False:
        lines.extend(
            [
                "",
                "Narrow frontier safe: no — filter-v1 certification rejected this plan; "
                "legacy impact SQL is not evidence that repair is safe",
            ]
        )
    elif eligibility.get("eligible") is True:
        lines.extend(
            [
                "",
                "certification: pending warehouse execution",
                "narrow frontier decision: not yet established",
            ]
        )
        if certified:
            lines.append("candidate SQL: compiled")
    elif safe is False:
        lines.extend(
            [
                "",
                "Narrow frontier safe: no — filter-v1 certification rejected this plan; "
                "legacy impact SQL is not evidence that repair is safe",
            ]
        )
    if comparison.get("fullRebuildRequired"):
        lines.append("Full rebuild required: yes")
    if comparison.get("fullRebuildRecommended"):
        lines.append("Full rebuild recommended: yes")
    eligibility = comparison.get("staticEligibility")
    if eligibility:
        lines.extend(["", _format_eligibility_line(eligibility, indent="")])
    return "\n".join(lines)


_INGEST_STRIP_KEYS = (
    "candidateSql",
    "parameterizedSql",
    "parameters",
    "tags",
    "legacyImpactSql",
    "legacyImpactCompilation",
)
_ELIGIBILITY_STRIP_KEYS = ("predicate", "sql", "candidateSql", "baseSql", "prSql", "parameters")

IMPACT_EXECUTION_EXECUTED = "EXECUTED"
IMPACT_EXECUTION_NOT_EVALUATED = "NOT_EVALUATED"
IMPACT_EXECUTION_NOT_RUN = "NOT_RUN"
IMPACT_EXECUTION_FAILED = "FAILED"
TARGETED_VALIDATION_NOT_RUN = "NOT_RUN"


def stamp_impact_execution(
    comparison: dict[str, Any] | None,
    *,
    run_mode: str,
    full_rebuild_required: bool,
    sql_change_executed: bool,
    impact_attempted: bool = False,
    proof_status: str | None = None,
    failure_phase: str | None = None,
    failure_code: str | None = None,
    failure_reason: str | None = None,
) -> dict[str, Any] | None:
    """Record whether compiled impact SQL ran in the warehouse.

    COMPILED is the predicate compiler. EXECUTED means Snowflake (or the
    configured adapter) actually ran the candidate query. NOT_RUN means
    targeted execution was skipped. FAILED is reserved for an attempt
    that ran and failed. EXECUTION_FAILED is a later proof phase, not an
    unsupported-SQL rebuild.
    """
    if not comparison:
        return comparison
    skipped_rebuild = full_rebuild_required and not impact_attempted and not sql_change_executed
    if skipped_rebuild:
        default = IMPACT_EXECUTION_NOT_RUN
    elif proof_status == "EXECUTION_FAILED" and (impact_attempted or sql_change_executed):
        default = IMPACT_EXECUTION_EXECUTED
    elif full_rebuild_required:
        default = IMPACT_EXECUTION_FAILED
    elif run_mode == "live" and sql_change_executed:
        default = IMPACT_EXECUTION_EXECUTED
    else:
        default = IMPACT_EXECUTION_NOT_EVALUATED
    copied = json.loads(json.dumps(comparison))
    for group in ("added", "removed", "modified"):
        for row in copied.get(group) or []:
            if row.get("impactStatus") == FULL_REBUILD_REQUIRED:
                row["impactExecution"] = (
                    IMPACT_EXECUTION_FAILED
                    if (impact_attempted or sql_change_executed)
                    and proof_status != "EXECUTION_FAILED"
                    else IMPACT_EXECUTION_NOT_RUN
                )
            elif row.get("impactStatus") or row.get("changeKinds"):
                row["impactExecution"] = default
    if skipped_rebuild:
        copied["targetedValidation"] = TARGETED_VALIDATION_NOT_RUN
    if proof_status:
        copied["proofStatus"] = proof_status
    if failure_phase:
        copied["failurePhase"] = failure_phase
    if failure_code:
        copied["failureCode"] = failure_code
    if failure_reason:
        copied["failureReason"] = failure_reason
    return copied


def comparison_for_ingest(comparison: dict[str, Any] | None) -> dict[str, Any] | None:
    """Drop generated SQL from the SaaS payload. Status and fingerprints remain."""
    if not comparison:
        return None
    copied = json.loads(json.dumps(comparison))
    for group in ("added", "removed", "modified"):
        for row in copied.get(group) or []:
            for key in _INGEST_STRIP_KEYS:
                row.pop(key, None)
            _strip_eligibility_sql(row.get("staticEligibility"))
    _strip_eligibility_sql(copied.get("staticEligibility"))
    return copied


def _strip_eligibility_sql(payload: dict[str, Any] | None) -> None:
    if not isinstance(payload, dict):
        return
    for key in _ELIGIBILITY_STRIP_KEYS:
        payload.pop(key, None)


def _filter_v1_impact(eligibility: StaticEligibility, *, entity_key: str) -> ImpactCompileResult:
    if eligibility.compiled and eligibility.candidate_sql and eligibility.candidate_fingerprint:
        return ImpactCompileResult(
            status=COMPILED,
            reasons=("filter-v1 candidate-key compiler",),
            entity_key=entity_key,
            candidate_sql=eligibility.candidate_sql,
            parameterized_sql=eligibility.candidate_sql,
            parameters=(),
            query_fingerprint=eligibility.candidate_fingerprint,
            candidate_set_state=CANDIDATE_SET_NOT_EVALUATED,
        )
    diagnostic = eligibility.diagnostic or "filter-v1 candidate compiler failed closed"
    return ImpactCompileResult(
        status=FULL_REBUILD_REQUIRED,
        reasons=(diagnostic,),
        entity_key=entity_key,
        candidate_sql=None,
        parameterized_sql=None,
        parameters=(),
        query_fingerprint=None,
        candidate_set_state=CANDIDATE_SET_ANALYSIS_FAILED,
    )


def _static_eligibility_payload(
    base_sql: str,
    pr_sql: str,
    *,
    entity_key: str,
    dialect: str,
    manifest_version: int | None,
    manifest_fingerprint: str | None,
    target_model: str,
) -> dict[str, Any]:
    try:
        result = analyze_static_eligibility(
            base_sql,
            pr_sql,
            entity_key=entity_key,
            dialect=dialect,
            manifest_version=manifest_version,
            manifest_fingerprint=manifest_fingerprint,
            target_model=target_model,
        )
        payload = result.to_payload()
        assert_eligibility_payload_is_safe(payload)
        return payload
    except Exception:
        fallback = StaticEligibility(
            eligible=False,
            reason_code=UNSUPPORTED_OPERATION,
            diagnostic="static eligibility analysis failed closed",
            manifest_version=manifest_version,
            manifest_fingerprint=manifest_fingerprint,
        )
        return fallback.to_payload()


def _top_level_eligibility(
    modified: list[dict[str, Any]],
    *,
    target_name: str | None,
) -> dict[str, Any] | None:
    if target_name:
        for row in modified:
            if row.get("name") == target_name and row.get("staticEligibility"):
                return row["staticEligibility"]
    for row in modified:
        if row.get("impactStatus") and row.get("staticEligibility"):
            return row["staticEligibility"]
    for row in modified:
        if row.get("staticEligibility"):
            return row["staticEligibility"]
    return None


def _format_eligibility_line(eligibility: dict[str, Any], *, indent: str) -> str:
    if eligibility.get("semanticChange") is False:
        return f"{indent}filter-v1 static eligibility: no semantic change"
    status = "eligible" if eligibility.get("eligible") else "ineligible"
    reason = eligibility.get("reasonCode")
    line = f"{indent}filter-v1 static eligibility: {status}"
    if reason:
        line += f" ({reason})"
    if eligibility.get("eligible") and eligibility.get("compiled"):
        line += "; candidate SQL compiled"
    elif eligibility.get("eligible") and eligibility.get("compiled") is False:
        compile_reason = eligibility.get("compileReasonCode") or "compiler failed"
        line += f"; candidate SQL not compiled ({compile_reason})"
    diagnostic = eligibility.get("diagnostic")
    if diagnostic and not eligibility.get("eligible"):
        line += f" — {diagnostic}"
    return line
