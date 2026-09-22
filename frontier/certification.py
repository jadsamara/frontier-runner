"""Separated assessment result dimensions for filter-v1.

Certification, validation, economics, and execution are independent
claims. Warehouse execution failure, economic fallback, and
full-reference validation must not overwrite certification status.
"""

from __future__ import annotations

from typing import Any

from frontier.snapshot import (
    ASSURANCE_ADAPTER,
    MODE_NONE,
    RULE_SET_VERSION,
    SOURCE_SNAPSHOT_NOT_PINNED,
    SourceSnapshot,
    unpinned_snapshot,
)

SQL_CERTIFIED = "SQL_CERTIFIED"
CONTRACT_CERTIFIED = "CONTRACT_CERTIFIED"
UNCERTIFIED = "UNCERTIFIED"
_LEGACY_UNCIFIED = "UNCIFIED"


def normalize_certification_status(value: str | None) -> str:
    """Accept the 21A UNCIFIED misspelling; emit only UNCERTIFIED."""
    if value == _LEGACY_UNCIFIED:
        return UNCERTIFIED
    if value in {SQL_CERTIFIED, CONTRACT_CERTIFIED, UNCERTIFIED}:
        return value
    return UNCERTIFIED


VALIDATION_NOT_RUN = "NOT_RUN"
CANDIDATES_CONFIRMED = "CANDIDATES_CONFIRMED"
FULL_REFERENCE_VALIDATED = "FULL_REFERENCE_VALIDATED"
VALIDATION_FAILED = "FAILED"

TARGETED_REPAIR_RECOMMENDED = "TARGETED_REPAIR_RECOMMENDED"
FULL_REBUILD_RECOMMENDED = "FULL_REBUILD_RECOMMENDED"
ECONOMICS_NOT_EVALUATED = "NOT_EVALUATED"

EXECUTION_PENDING = "PENDING"
EXECUTION_SUCCEEDED = "SUCCEEDED"
EXECUTION_FAILED = "FAILED"


def certification_status(
    snapshot: SourceSnapshot | None,
    *,
    static_certified: bool = False,
    contract_certified: bool = False,
) -> tuple[str, str | None]:
    """Return (status, failure_code). Never inspects execution or validation."""
    pinned = snapshot if snapshot is not None else unpinned_snapshot()
    if not pinned.allows_sql_certified():
        code = pinned.failure_code or SOURCE_SNAPSHOT_NOT_PINNED
        return UNCERTIFIED, code
    if static_certified:
        return SQL_CERTIFIED, None
    if contract_certified:
        return CONTRACT_CERTIFIED, None
    return UNCERTIFIED, None


def validation_status(
    *,
    full_reference_validated: bool = False,
    full_reference_failed: bool = False,
    candidates_confirmed: bool = False,
    confirmation_failed: bool = False,
) -> str:
    if full_reference_validated:
        return FULL_REFERENCE_VALIDATED
    if full_reference_failed or confirmation_failed:
        return VALIDATION_FAILED
    if candidates_confirmed:
        return CANDIDATES_CONFIRMED
    return VALIDATION_NOT_RUN


def economics_decision(
    *,
    full_rebuild_recommended: bool = False,
    targeted_ran: bool = False,
    frontier_bytes: int | None = None,
    full_comparison_bytes: int | None = None,
    warehouse_credits: float | None = None,
) -> str:
    """Bytes and wall time are not billed credits. Do not recommend targeted repair from running alone."""
    del targeted_ran
    if full_rebuild_recommended:
        return FULL_REBUILD_RECOMMENDED
    if frontier_bytes is not None and full_comparison_bytes is not None:
        if frontier_bytes >= full_comparison_bytes:
            return FULL_REBUILD_RECOMMENDED
        if warehouse_credits is None:
            return ECONOMICS_NOT_EVALUATED
        return TARGETED_REPAIR_RECOMMENDED
    return ECONOMICS_NOT_EVALUATED


def execution_status(
    *,
    execution_failed: bool = False,
    execution_ran: bool = False,
) -> str:
    if execution_failed:
        return EXECUTION_FAILED
    if execution_ran:
        return EXECUTION_SUCCEEDED
    return EXECUTION_PENDING


def build_assessment_dimensions(
    *,
    snapshot: SourceSnapshot | None,
    static_certified: bool = False,
    contract_certified: bool = False,
    execution_failed: bool = False,
    execution_ran: bool = False,
    full_rebuild_recommended: bool = False,
    targeted_ran: bool = False,
    frontier_bytes: int | None = None,
    full_comparison_bytes: int | None = None,
    warehouse_credits: float | None = None,
    candidates_confirmed: bool = False,
    confirmation_failed: bool = False,
    full_reference_validated: bool = False,
    full_reference_failed: bool = False,
    failure_phase: str | None = None,
    failure_code: str | None = None,
    failure_reason: str | None = None,
) -> dict[str, Any]:
    cert_status, cert_code = certification_status(
        snapshot,
        static_certified=static_certified,
        contract_certified=contract_certified,
    )
    if cert_status == SQL_CERTIFIED and (snapshot is None or not snapshot.allows_sql_certified()):
        cert_status = UNCERTIFIED
        cert_code = SOURCE_SNAPSHOT_NOT_PINNED
    certification: dict[str, Any] = {
        "status": cert_status,
        "ruleSetVersion": RULE_SET_VERSION,
    }
    if cert_code:
        certification["failureCode"] = cert_code
    source = snapshot or unpinned_snapshot()
    upload = source.to_upload_payload()
    economics = {
        "decision": economics_decision(
            full_rebuild_recommended=full_rebuild_recommended,
            targeted_ran=targeted_ran,
            frontier_bytes=frontier_bytes,
            full_comparison_bytes=full_comparison_bytes,
            warehouse_credits=warehouse_credits,
        )
    }
    if frontier_bytes is not None:
        economics["frontierBytesScanned"] = int(frontier_bytes)
    if full_comparison_bytes is not None:
        economics["fullComparisonBytesScanned"] = int(full_comparison_bytes)
    execution: dict[str, Any] = {
        "status": execution_status(
            execution_failed=execution_failed,
            execution_ran=execution_ran,
        )
    }
    if failure_phase:
        execution["failurePhase"] = failure_phase[:64]
    if failure_code:
        execution["failureCode"] = failure_code[:64]
    if failure_reason:
        execution["failureReason"] = failure_reason[:512]
    return {
        "certification": certification,
        "validation": {"status": validation_status(
            full_reference_validated=full_reference_validated,
            full_reference_failed=full_reference_failed,
            candidates_confirmed=candidates_confirmed,
            confirmation_failed=confirmation_failed,
        )},
        "economics": economics,
        "execution": execution,
        "sourceSnapshot": upload,
        "baselineBoundary": dict(BASELINE_BOUNDARY),
    }


BASELINE_BOUNDARY = {
    "comparesSqlVersionsAtPinnedSnapshot": True,
    "existingMaterializedMartRepresentsSnapshot": False,
    "safeInPlaceProductionRepair": False,
}


def filter_v1_sql_certified(
    *,
    eligible: bool,
    compiled: bool,
    confirmed: bool,
    execution_failed: bool,
) -> bool:
    """All four 21C gates except snapshot, which certification_status still checks."""
    return bool(eligible and compiled and confirmed and not execution_failed)


def enrich_certification_record(
    dimensions: dict[str, Any],
    *,
    eligibility: dict[str, Any] | None,
    snapshot: SourceSnapshot | None,
    candidate_fingerprint: str | None = None,
    execution_failed: bool = False,
) -> dict[str, Any]:
    """Attach plan, taint, assumption, and snapshot identity to certification."""
    certification = dict(dimensions.get("certification") or {})
    eligibility = eligibility or {}
    for src, dest in (
        ("oldPlanFingerprint", "oldPlanFingerprint"),
        ("newPlanFingerprint", "newPlanFingerprint"),
        ("changedFilterNodeId", "changedFilterNodeId"),
        ("coveredNodeIds", "coveredNodeIds"),
        ("operators", "operators"),
        ("joinTaint", "joinTaint"),
        ("checkedAssumptions", "checkedAssumptions"),
        ("manifestDependencies", "manifestDependencies"),
        ("candidateFingerprint", "candidateQueryFingerprint"),
    ):
        value = eligibility.get(src)
        if value:
            certification[dest] = value
    if candidate_fingerprint:
        certification["candidateQueryFingerprint"] = candidate_fingerprint
    source = snapshot or unpinned_snapshot()
    if source.identifier:
        certification["snapshotIdentifier"] = source.identifier
    if eligibility.get("eligible") is False:
        certification["status"] = UNCERTIFIED
        certification["failureCode"] = eligibility.get("reasonCode") or UNCERTIFIED
    elif execution_failed:
        certification["status"] = UNCERTIFIED
        if snapshot is not None and (
            snapshot.assurance == ASSURANCE_ADAPTER or snapshot.allows_sql_certified()
        ):
            certification["failureCode"] = EXECUTION_FAILED
    dimensions["certification"] = certification
    dimensions["baselineBoundary"] = dict(BASELINE_BOUNDARY)
    return dimensions


def display_evidence_level(evidence_level: str, validation: dict[str, Any] | None) -> str:
    """Never label empirically validated unless full-reference actually ran."""
    if (
        validation
        and validation.get("status") != FULL_REFERENCE_VALIDATED
        and evidence_level == "empirically_validated"
    ):
        return "aggregates"
    return evidence_level
