from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from frontier.config import ConfigError
from frontier.github import env_flag, pull_request_number

COMMENT_MARKER = "<!-- frontier-impact-assessment -->"
_USER_AGENT = "frontier-runner"

_EVIDENCE_LABELS = {
    "none": "none",
    "aggregates": "aggregates",
    "equivalence": "equivalence",
    "empirically_validated": "empirically validated",
}


def dashboard_run_url(api_url: str, run_id: str) -> str:
    return f"{api_url.rstrip('/')}/runs/{run_id}"


def _format_count(value: int) -> str:
    return f"{value:,}"


def _pluralize(noun: str, count: int) -> str:
    if count == 1:
        return noun
    if noun.endswith("s"):
        return noun
    return f"{noun}s"


def _validation_difference_total(results: list[dict[str, Any]]) -> int:
    return sum(int(item.get("differenceCount") or 0) for item in results)


def _failed_checks(results: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for item in results:
        if item.get("status") != "failed":
            continue
        name = str(item.get("testName") or "check")
        differences = int(item.get("differenceCount") or 0)
        lines.append(f"- {name} (differences={differences})")
    return lines


def format_pr_comment(payload: dict[str, Any], *, run_url: str) -> str:
    """Aggregate-only PR comment. Never include entity IDs or warehouse values."""
    status = str(payload.get("status") or "failed").upper()
    model = payload.get("model") or {}
    model_name = str(model.get("name") or "unknown")
    entity_type = str(model.get("entityType") or "entity")
    metrics = payload.get("metrics") or {}
    full_count = int(metrics.get("fullEntityCount") or 0)
    frontier_count = int(metrics.get("frontierEntityCount") or 0)
    percent = metrics.get("percentRowsAvoided")
    percent_text = f"{percent}%" if percent is not None else "n/a"
    events = payload.get("changeEvents") or []
    validations = payload.get("validationResults") or []
    evidence = _EVIDENCE_LABELS.get(str(payload.get("evidenceLevel") or ""), "none")
    headline = f"Frontier impact assessment: {status}"
    if status != "PASSED":
        headline = f"**{headline}**"
    comparison = payload.get("sqlComparison") or {}
    modified = comparison.get("modified") or []
    sql_kinds = list(
        dict.fromkeys(
            str(kind)
            for row in modified
            for kind in (row.get("changeKinds") or [])
        )
    )
    candidate_count = metrics.get("candidateFrontierCount")
    if candidate_count is None:
        candidate_count = metrics.get("unionCandidateCount")
    confirmed_count = metrics.get("confirmedFrontierCount")
    changed_source = metrics.get("changedSourceRowCount")
    if changed_source is None:
        changed_source = metrics.get("sourcePopulationCount")
    event_candidates = metrics.get("eventCandidateCount")
    before_count = metrics.get("beforeEntityCount")
    after_count = metrics.get("afterEntityCount")
    recommended = bool(comparison.get("fullRebuildRecommended"))
    full_rebuild = bool(comparison.get("fullRebuildRequired") or comparison.get("narrowFrontierSafe") is False)
    targeted_safe = (
        int(metrics.get("mismatchedFinalRows") or 0) == 0
        and int(metrics.get("missingFrontierEntities") or 0) == 0
        and not full_rebuild
        and not recommended
    )
    run_mode = payload.get("runMode")
    origin = payload.get("candidateSetOrigin")

    lines = [
        COMMENT_MARKER,
        headline,
        "",
        f"Model: {model_name}",
    ]
    if run_mode == "fixture":
        lines.append("Demo fixture — not executed against a live warehouse")
    elif run_mode == "live":
        lines.append("Live warehouse assessment")
    if origin:
        lines.append(f"Candidate set origin: {origin}")
    if sql_kinds or modified:
        operator = sql_kinds[0] if sql_kinds else "SQL change"
        lines.append(f"SQL operator: {operator}")
        if changed_source is not None:
            lines.append(f"Changed source rows: {_format_count(int(changed_source))}")
        if candidate_count is not None:
            lines.append(f"Candidate customers: {_format_count(int(candidate_count))}")
        lines.append(
            f"Event-derived candidates: {_format_count(int(event_candidates or 0))}"
        )
        if confirmed_count is not None:
            lines.append(
                f"Customer summaries that actually differ: {_format_count(int(confirmed_count))}"
            )
        if before_count is not None and after_count is not None:
            lines.append(
                f"Row count: {_format_count(int(before_count))} → {_format_count(int(after_count))}"
            )
        lines.append(f"Targeted repair: {'skipped' if recommended else ('safe' if targeted_safe else 'not safe')}")
        if full_rebuild:
            lines.append("Full backfill: required")
        elif recommended:
            lines.append("Full backfill: recommended")
        else:
            lines.append("Full backfill: not required")
    elif events:
        lines.append(f"Changed source events: {len(events)}")
        lines.append(
            f"Affected {_pluralize(entity_type, frontier_count)}: "
            f"{_format_count(frontier_count)} of {_format_count(full_count)}"
        )
    else:
        lines.append(
            f"Affected {_pluralize(entity_type, frontier_count)}: "
            f"{_format_count(frontier_count)} of {_format_count(full_count)}"
        )
    lines.extend(
        [
            f"Rows avoided: {percent_text}",
            f"Validation differences: {_validation_difference_total(validations)}",
            f"Evidence: {evidence}",
        ]
    )
    event_count = metrics.get("eventCandidateCount")
    sql_count = metrics.get("sqlChangeCandidateCount")
    union_count = metrics.get("unionCandidateCount")
    if event_count is not None or sql_count is not None or union_count is not None:
        lines.append(
            "Candidate keys: "
            f"{_format_count(int(event_count or 0))} event, "
            f"{_format_count(int(sql_count or 0))} SQL-change, "
            f"{_format_count(int(union_count or 0))} union"
        )
    comparison = payload.get("sqlComparison") or {}
    if comparison:
        base = comparison.get("base") or {}
        pr = comparison.get("pr") or {}
        added = len(comparison.get("added") or [])
        removed = len(comparison.get("removed") or [])
        modified = comparison.get("modified") or []
        lines.extend(
            [
                f"SQL models changed: {len(modified)} modified, {added} added, {removed} removed",
                f"Base artifact: {base.get('fingerprint') or '—'}",
                f"PR artifact: {pr.get('fingerprint') or '—'}",
            ]
        )
        kinds = list(
            dict.fromkeys(
                str(kind)
                for row in modified
                for kind in (row.get("changeKinds") or [])
            )
        )
        if kinds:
            lines.append(f"SQL change kinds: {', '.join(kinds)}")
        if comparison.get("narrowFrontierSafe") is False:
            lines.append("Narrow frontier: not allowed")
        if comparison.get("fullRebuildRequired"):
            lines.append("Impact: full rebuild required")
        elif comparison.get("fullRebuildRecommended"):
            lines.append("Impact: full rebuild recommended")
        statuses = list(
            dict.fromkeys(
                str(row.get("impactStatus"))
                for row in modified
                if row.get("impactStatus")
            )
        )
        if statuses:
            lines.append(f"Impact compilation: {', '.join(statuses)}")
        executions = list(
            dict.fromkeys(
                str(row.get("impactExecution"))
                for row in (
                    *(comparison.get("modified") or []),
                    *(comparison.get("added") or []),
                    *(comparison.get("removed") or []),
                )
                if row.get("impactExecution")
            )
        )
        if not executions:
            if payload.get("runMode") == "fixture":
                executions = ["NOT_EVALUATED"]
            elif full_rebuild:
                executions = ["FAILED"]
            elif payload.get("runMode") == "live" and statuses:
                executions = ["EXECUTED"]
        if executions and statuses:
            lines.append(f"Impact execution: {', '.join(executions)}")
        reasons = list(
            dict.fromkeys(
                str(reason)
                for row in modified
                if row.get("impactStatus") == "FULL_REBUILD_REQUIRED"
                for reason in (row.get("impactReasons") or [])
            )
        )
        if reasons:
            lines.append(f"Impact reasons: {', '.join(reasons[:8])}")
    failed = _failed_checks(validations)
    if failed:
        lines.extend(["", "Failed checks:", *failed])
    lines.extend(["", f"View full assessment: {run_url}"])
    return "\n".join(lines) + "\n"


def _github_api_root() -> str:
    return (os.environ.get("GITHUB_API_URL") or "https://api.github.com").rstrip("/")


def _request_json(
    method: str,
    url: str,
    *,
    github_token: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    encoded = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        url,
        data=encoded,
        method=method,
        headers={
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": _USER_AGENT,
            **({"Content-Type": "application/json"} if body is not None else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            parsed = json.loads(raw) if raw else {}
            return response.status, parsed
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise ConfigError(f"GitHub PR comment failed with HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise ConfigError(f"GitHub PR comment failed: {error.reason}") from error


def _list_issue_comments(repository: str, pr_number: int, github_token: str) -> list[dict[str, Any]]:
    url = f"{_github_api_root()}/repos/{repository}/issues/{pr_number}/comments?per_page=100"
    _status, parsed = _request_json("GET", url, github_token=github_token)
    if not isinstance(parsed, list):
        raise ConfigError("GitHub PR comment list was not an array")
    return parsed


def upsert_pr_comment(
    *,
    body: str,
    repository: str,
    pr_number: int,
    github_token: str,
) -> str:
    comments = _list_issue_comments(repository, pr_number, github_token)
    existing = next(
        (
            item
            for item in comments
            if COMMENT_MARKER in str(item.get("body") or "")
        ),
        None,
    )
    if existing and existing.get("id") is not None:
        url = f"{_github_api_root()}/repos/{repository}/issues/comments/{existing['id']}"
        _request_json("PATCH", url, github_token=github_token, body={"body": body})
        return "updated"
    url = f"{_github_api_root()}/repos/{repository}/issues/{pr_number}/comments"
    _request_json("POST", url, github_token=github_token, body={"body": body})
    return "created"


def maybe_upsert_pr_comment(
    payload: dict[str, Any],
    *,
    api_url: str,
    run_id: str | None,
) -> str | None:
    """Post or update the Frontier PR comment when running in GitHub Actions.

    Uses GITHUB_TOKEN locally in CI. Never sends it to Frontier SaaS.
    """
    if env_flag("FRONTIER_SKIP_PR_COMMENT"):
        return None
    if (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() != "true":
        return None
    github_token = (os.environ.get("GITHUB_TOKEN") or "").strip()
    repository = (os.environ.get("GITHUB_REPOSITORY") or "").strip()
    pr_number = pull_request_number()
    if not github_token or not repository or pr_number is None:
        return None
    if not run_id:
        return None
    body = format_pr_comment(payload, run_url=dashboard_run_url(api_url, run_id))
    return upsert_pr_comment(
        body=body,
        repository=repository,
        pr_number=pr_number,
        github_token=github_token,
    )
