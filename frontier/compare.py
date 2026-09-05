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
    FULL_REBUILD_REQUIRED,
    compile_impact_query,
)
from frontier.sql_fingerprint import sql_dialect, sql_fingerprint
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
        if is_sql_change_impact_model(name) and name not in names:
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
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uniqueId": unique_id,
        "name": name,
        "baseFingerprint": base_fingerprint,
        "prFingerprint": pr_fingerprint,
        "downstream": downstream,
    }
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
) -> SqlComparison:
    dialect = sql_dialect(pr.adapter_type or base.adapter_type)
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
                    impact=added_removed_impact,
                )
            )
            continue
        if base_prints[unique_id] == pr_prints[unique_id]:
            continue
        base_sql = compiled_sql_for(base_models[unique_id], base_compiled_root) or ""
        pr_sql = compiled_sql_for(node, pr_compiled_root) or ""
        classification = classify_sql_change(base_sql, pr_sql)
        if not classification.kinds:
            continue
        impact = compile_impact_query(
            base_sql,
            pr_sql,
            entity_key=entity_key or "",
            confirmed_keys=confirmed_keys or (),
            classification=classification,
        )
        modified.append(
            _model_payload(
                unique_id=unique_id,
                name=node.name,
                base_fingerprint=base_prints[unique_id],
                pr_fingerprint=pr_prints[unique_id],
                downstream=_downstream_payload(pr, unique_id),
                change_kinds=list(classification.kinds),
                unsafe=classification.unsafe,
                unsupported_reasons=list(classification.unsupported_reasons) or None,
                impact=impact.to_payload(include_sql=True),
                change_summary=describe_sql_change(base_sql, pr_sql),
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
                impact=added_removed_impact,
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

    full_rebuild = any(
        row.get("impactStatus") == FULL_REBUILD_REQUIRED
        for row in (*added, *removed, *modified)
    )
    payload = {
        "base": base_side,
        "pr": pr_side,
        "added": added,
        "removed": removed,
        "modified": modified,
        "narrowFrontierSafe": narrow_frontier_safe({"modified": modified}),
        "fullRebuildRequired": full_rebuild,
    }
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
                lines.append(f"    candidate sql: {row['candidateSql']}")

    section("Added", comparison.get("added") or [])
    section("Removed", comparison.get("removed") or [])
    section("Modified", comparison.get("modified") or [])
    safe = comparison.get("narrowFrontierSafe")
    if safe is not None:
        lines.extend(
            [
                "",
                f"Narrow frontier safe: {'yes' if safe else 'no'}",
            ]
        )
    if comparison.get("fullRebuildRequired"):
        lines.append("Full rebuild required: yes")
    return "\n".join(lines)


_INGEST_STRIP_KEYS = ("candidateSql", "parameterizedSql", "parameters")

IMPACT_EXECUTION_EXECUTED = "EXECUTED"
IMPACT_EXECUTION_NOT_EVALUATED = "NOT_EVALUATED"
IMPACT_EXECUTION_FAILED = "FAILED"


def stamp_impact_execution(
    comparison: dict[str, Any] | None,
    *,
    run_mode: str,
    full_rebuild_required: bool,
    sql_change_executed: bool,
) -> dict[str, Any] | None:
    """Record whether compiled impact SQL ran in the warehouse.

    COMPILED is the predicate compiler. EXECUTED means Snowflake (or the
    configured adapter) actually ran the candidate query.
    """
    if not comparison:
        return comparison
    if full_rebuild_required:
        default = IMPACT_EXECUTION_FAILED
    elif run_mode == "live" and sql_change_executed:
        default = IMPACT_EXECUTION_EXECUTED
    else:
        default = IMPACT_EXECUTION_NOT_EVALUATED
    copied = json.loads(json.dumps(comparison))
    for group in ("added", "removed", "modified"):
        for row in copied.get(group) or []:
            if row.get("impactStatus") == FULL_REBUILD_REQUIRED:
                row["impactExecution"] = IMPACT_EXECUTION_FAILED
            elif row.get("impactStatus") or row.get("changeKinds"):
                row["impactExecution"] = default
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
    return copied
