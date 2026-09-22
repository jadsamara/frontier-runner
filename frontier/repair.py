"""Mart baseline verification and disposable delete-and-insert repair.

Baseline compares the existing materialized target with complete old SQL
at one adapter-verified snapshot. Disposable repair copies that mart,
deletes certified candidate keys, inserts targeted head rows, and
compares the repaired copy with the complete head result. The real mart
is never mutated.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from frontier.config import ConfigError
from frontier.mutation import MUTATION_REJECTED, canonicalize_relation
from frontier.snapshot import (
    ASSURANCE_ADAPTER,
    TIME_TRAVEL_TYPES,
    SOURCE_SNAPSHOT_NOT_PINNED,
    SnapshotError,
)

MART_BASELINE_NOT_RUN = "NOT_RUN"
MART_BASELINE_MATCHED = "MATCHED"
MART_BASELINE_MISMATCHED = "MISMATCHED"
MART_BASELINE_FAILED = "FAILED"

REPAIR_NOT_RUN = "NOT_RUN"
REPAIR_SUCCEEDED = "SUCCEEDED"
REPAIR_FAILED = "FAILED"

PRODUCTION_NOT_REQUESTED = "NOT_REQUESTED"
PRODUCTION_NOT_RUN = "NOT_RUN"
PRODUCTION_SUCCEEDED = "SUCCEEDED"
PRODUCTION_FAILED = "FAILED"

TARGET_SNAPSHOT_NOT_PINNED = "TARGET_SNAPSHOT_NOT_PINNED"
GRAIN_VIOLATED = "GRAIN_VIOLATED"
SCHEMA_MISALIGNED = "SCHEMA_MISALIGNED"
SNAPSHOT_IDENTIFIER_MISMATCH = "SNAPSHOT_IDENTIFIER_MISMATCH"
PREREQUISITES_NOT_MET = "PREREQUISITES_NOT_MET"
CLEANUP_FAILED = "CLEANUP_FAILED"
WAREHOUSE_EXECUTION_FAILED = "WAREHOUSE_EXECUTION_FAILED"

MEASUREMENT_CREDITS = "WAREHOUSE_CREDITS"
MEASUREMENT_BYTES_TIME = "BYTES_AND_TIME_ONLY"
MEASUREMENT_NONE = "NONE"

TARGETED_ECONOMICS_PHASES = (
    "candidate discovery",
    "candidate materialization",
    "baseline verification",
    "disposable mart creation",
    "candidate-key delete",
    "targeted head computation",
    "targeted insert",
    "confirmation",
    "complete repaired-table validation",
    "cleanup",
)
FULL_ECONOMICS_PHASES = (
    "complete head computation",
    "complete output write",
)

SQL_CERTIFIED = "SQL_CERTIFIED"
CONTRACT_CERTIFIED = "CONTRACT_CERTIFIED"
CANDIDATES_CONFIRMED = "CANDIDATES_CONFIRMED"
FULL_REFERENCE_VALIDATED = "FULL_REFERENCE_VALIDATED"


def empty_mart_baseline() -> dict[str, Any]:
    return {
        "status": MART_BASELINE_NOT_RUN,
        "missingRows": None,
        "extraRows": None,
        "mismatchedRows": None,
        "duplicateEntityKeys": None,
        "snapshotIdentifier": None,
        "warehouseQueryIds": [],
        "failurePhase": None,
        "reasonCode": None,
        "reason": None,
    }


def empty_repair_validation() -> dict[str, Any]:
    return {
        "status": REPAIR_NOT_RUN,
        "mode": "DISPOSABLE_TABLE",
        "candidateCount": None,
        "deletedRows": None,
        "insertedRows": None,
        "missingRows": None,
        "extraRows": None,
        "mismatchedRows": None,
        "duplicateEntityKeys": None,
        "snapshotIdentifier": None,
        "warehouseQueryIds": [],
        "disposableResourcesCleaned": None,
        "failurePhase": None,
        "reasonCode": None,
        "reason": None,
    }


def empty_production_apply() -> dict[str, Any]:
    return {"status": PRODUCTION_NOT_REQUESTED}


def _nullsafe_equal(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return left == right


def _row_equal(left: tuple[Any, ...], right: tuple[Any, ...]) -> bool:
    if len(left) != len(right):
        return False
    return all(_nullsafe_equal(a, b) for a, b in zip(left, right))


@dataclass
class CompleteComparison:
    missing_rows: int = 0
    extra_rows: int = 0
    mismatched_rows: int = 0
    duplicate_entity_keys: int = 0
    reason_code: str | None = None
    reason: str | None = None

    @property
    def ok(self) -> bool:
        return (
            self.reason_code is None
            and self.missing_rows == 0
            and self.extra_rows == 0
            and self.mismatched_rows == 0
            and self.duplicate_entity_keys == 0
        )


def compare_complete_results(
    actual: Iterable[tuple[Any, ...]],
    reference: Iterable[tuple[Any, ...]],
    *,
    entity_key_index: int = 0,
    expected_width: int | None = None,
    one_row_per_entity: bool = True,
) -> CompleteComparison:
    """NULL-safe, order-independent, multiplicity-aware equality over every column."""
    left = [tuple(row) for row in actual]
    right = [tuple(row) for row in reference]
    widths = {len(row) for row in (*left, *right)}
    if expected_width is not None:
        widths.add(expected_width)
    if len(widths) > 1:
        return CompleteComparison(
            reason_code=SCHEMA_MISALIGNED,
            reason="output schemas cannot be aligned unambiguously",
        )
    if left and right and entity_key_index >= len(left[0]):
        return CompleteComparison(
            reason_code=SCHEMA_MISALIGNED,
            reason="entity key is not present in comparable output columns",
        )

    def key_of(row: tuple[Any, ...]) -> Any:
        return row[entity_key_index]

    from collections import Counter

    left_keys = Counter(key_of(row) for row in left)
    right_keys = Counter(key_of(row) for row in right)
    duplicate = sum(1 for count in (*left_keys.values(), *right_keys.values()) if count > 1)
    if one_row_per_entity and duplicate:
        dup_keys = {
            key
            for key, count in {**left_keys, **right_keys}.items()
            if left_keys[key] > 1 or right_keys[key] > 1
        }
        return CompleteComparison(
            duplicate_entity_keys=len(dup_keys),
            reason_code=GRAIN_VIOLATED,
            reason="declared one-row-per-entity grain is violated",
        )

    left_map = {key_of(row): row for row in left}
    right_map = {key_of(row): row for row in right}
    missing = len(set(right_map) - set(left_map))
    extra = len(set(left_map) - set(right_map))
    mismatched = sum(
        1
        for key in set(left_map) & set(right_map)
        if not _row_equal(left_map[key], right_map[key])
    )
    return CompleteComparison(
        missing_rows=missing,
        extra_rows=extra,
        mismatched_rows=mismatched,
        duplicate_entity_keys=0,
    )


def snapshot_identifiers_consistent(snapshot: Any, extra: Iterable[str] = ()) -> bool:
    if snapshot is None:
        return False
    identifier = getattr(snapshot, "identifier", None)
    if not identifier:
        return False
    phases = dict(getattr(snapshot, "phases", {}) or {})
    values = {identifier, *phases.values(), *extra}
    values.discard("")
    values.discard(None)
    return len(values) == 1


def target_binding_is_usable(snapshot: Any, target_relation: str) -> tuple[bool, str | None, str | None]:
    if snapshot is None or getattr(snapshot, "assurance", None) != ASSURANCE_ADAPTER:
        return False, TARGET_SNAPSHOT_NOT_PINNED, "existing target cannot be read at a pinned adapter snapshot"
    binding = snapshot.binding_for(target_relation) if hasattr(snapshot, "binding_for") else None
    if binding is None:
        return False, TARGET_SNAPSHOT_NOT_PINNED, "existing target is not in the snapshot inventory"
    if not getattr(binding, "supported", False):
        return False, TARGET_SNAPSHOT_NOT_PINNED, "existing target is not a supported physical table"
    relation_type = getattr(binding, "relation_type", None)
    if relation_type not in TIME_TRAVEL_TYPES:
        return False, TARGET_SNAPSHOT_NOT_PINNED, "existing target does not have usable snapshot retention"
    return True, None, None


def baseline_boundary_from(
    *,
    mart_baseline: dict[str, Any] | None,
    repair_validation: dict[str, Any] | None,
    certification_status: str | None,
    validation_status: str | None,
    snapshot: Any = None,
    cleanup_ok: bool | None = None,
    execution_failed: bool = False,
) -> dict[str, Any]:
    matched = (mart_baseline or {}).get("status") == MART_BASELINE_MATCHED
    repair_ok = (repair_validation or {}).get("status") == REPAIR_SUCCEEDED
    cleaned = (repair_validation or {}).get("disposableResourcesCleaned") is True
    if cleanup_ok is False:
        cleaned = False
    certified = certification_status in {SQL_CERTIFIED, CONTRACT_CERTIFIED}
    confirmed = validation_status in {CANDIDATES_CONFIRMED, FULL_REFERENCE_VALIDATED}
    snapshot_ok = bool(
        snapshot is not None
        and getattr(snapshot, "assurance", None) == ASSURANCE_ADAPTER
        and snapshot_identifiers_consistent(snapshot)
    )
    zero_diff = (
        repair_ok
        and (repair_validation or {}).get("missingRows") == 0
        and (repair_validation or {}).get("extraRows") == 0
        and (repair_validation or {}).get("mismatchedRows") == 0
        and (repair_validation or {}).get("duplicateEntityKeys") == 0
    )
    deleted = (repair_validation or {}).get("deletedRows")
    inserted = (repair_validation or {}).get("insertedRows")
    dml_ran = deleted is not None and inserted is not None
    safe = bool(
        certified
        and confirmed
        and snapshot_ok
        and matched
        and repair_ok
        and zero_diff
        and dml_ran
        and cleaned
        and not execution_failed
    )
    return {
        "comparesSqlVersionsAtPinnedSnapshot": True,
        "existingMaterializedMartRepresentsSnapshot": matched,
        "safeInPlaceProductionRepair": safe,
    }


def economics_from_jobs(
    job_metrics: Iterable[dict[str, Any]],
    *,
    phase_timings: dict[str, int] | None = None,
    warehouse_credits: float | None = None,
    full_credits: float | None = None,
    full_bytes: int | None = None,
    full_elapsed_ms: int | None = None,
    include_full_reference: bool = False,
) -> dict[str, Any]:
    jobs = list(job_metrics)
    timings = dict(phase_timings or {})
    targeted_phases: list[str] = []
    targeted_bytes = 0
    targeted_elapsed = 0
    saw_bytes = False
    saw_time = False
    for job in jobs:
        phase = str(job.get("phase") or "")
        if not phase:
            continue
        lowered = phase.lower()
        if any(token in lowered for token in ("full head", "complete head", "full comparison", "full reference")):
            continue
        targeted_phases.append(phase)
        bytes_processed = job.get("total_bytes_processed")
        if bytes_processed is None:
            bytes_processed = job.get("bytes_scanned")
        if bytes_processed is not None:
            targeted_bytes += int(bytes_processed)
            saw_bytes = True
        elapsed = job.get("elapsed_ms")
        if elapsed is not None:
            targeted_elapsed += int(elapsed)
            saw_time = True
        elif phase in timings:
            targeted_elapsed += int(timings[phase])
            saw_time = True
    if not targeted_phases:
        for phase, duration in timings.items():
            targeted_phases.append(phase)
            targeted_elapsed += int(duration)
            saw_time = True
    full_phases = list(FULL_ECONOMICS_PHASES) if include_full_reference or full_bytes is not None else []
    basis = MEASUREMENT_NONE
    if warehouse_credits is not None and full_credits is not None:
        basis = MEASUREMENT_CREDITS
    elif saw_bytes and saw_time and full_bytes is not None and full_elapsed_ms is not None:
        basis = MEASUREMENT_BYTES_TIME
    elif saw_bytes and full_bytes is not None:
        basis = MEASUREMENT_BYTES_TIME
    decision = "NOT_EVALUATED"
    reason = None
    targeted_cost: float | None = None
    full_cost: float | None = None
    if basis == MEASUREMENT_CREDITS:
        targeted_cost = float(warehouse_credits or 0)
        full_cost = float(full_credits or 0)
        if targeted_cost < full_cost:
            decision = "TARGETED_REPAIR_RECOMMENDED"
        else:
            decision = "FULL_REBUILD_RECOMMENDED"
            reason = "targeted credits are equal or greater than a full rebuild"
    elif basis == MEASUREMENT_BYTES_TIME:
        bytes_disclaimer = "bytes processed are not billed cost"
        if warehouse_credits is not None or full_credits is not None:
            reason = f"{bytes_disclaimer}; warehouse credits were incomplete"
        if saw_bytes and saw_time and full_bytes is not None and full_elapsed_ms is not None:
            cheaper = targeted_bytes < full_bytes and targeted_elapsed < full_elapsed_ms
            if cheaper:
                decision = "TARGETED_REPAIR_RECOMMENDED"
                reason = bytes_disclaimer if reason is None else f"{reason}; {bytes_disclaimer}"
            else:
                decision = "FULL_REBUILD_RECOMMENDED"
                reason = "targeted bytes or elapsed time are equal or greater than the full path; bytes processed are not billed cost"
        else:
            decision = "NOT_EVALUATED"
            reason = "bytes-only evidence is not billed cost and is not a complete comparable path"
    else:
        reason = "targeted and full paths are not comparable"
    if include_full_reference:
        reason = (reason + "; " if reason else "") + (
            "full-reference validation adds trust-building cost beyond a normal production repair"
        )
    payload = {
        "decision": decision,
        "targetedBytesProcessed": targeted_bytes if saw_bytes else None,
        "fullBytesProcessed": full_bytes,
        "targetedElapsedMs": targeted_elapsed if saw_time else None,
        "fullElapsedMs": full_elapsed_ms,
        "targetedCredits": warehouse_credits,
        "fullCredits": full_credits,
        "measurementBasis": basis,
        "includedPhases": {
            "targeted": targeted_phases or list(TARGETED_ECONOMICS_PHASES[: len(timings) or 0]),
            "full": full_phases,
        },
        "reason": reason,
    }
    if saw_bytes:
        payload["frontierBytesScanned"] = targeted_bytes
    if full_bytes is not None:
        payload["fullComparisonBytesScanned"] = full_bytes
    if saw_time:
        payload["frontierElapsedMs"] = targeted_elapsed
    if full_elapsed_ms is not None:
        payload["fullComparisonElapsedMs"] = full_elapsed_ms
    if warehouse_credits is not None:
        payload["warehouseCredits"] = warehouse_credits
    return payload
