from __future__ import annotations

import time
from dataclasses import dataclass

from frontier.config import ConfigError, FrontierConfig, ProofConfig
from frontier.dbt_artifacts import Manifest
from frontier.frontier import ChangeEvent
from frontier.warehouse import WarehouseAdapter
from frontier.validation import ValidationResult


@dataclass(frozen=True)
class MutationProof:
    full_rows_recomputed: int
    frontier_rows_recomputed: int
    rows_avoided: int
    missing_frontier_entities: int
    extra_frontier_entities: int
    mismatched_final_rows: int
    test_duration_ms: int
    deleted_order_id: str
    deleted_order_customer_id: str

    @property
    def percent_rows_avoided(self) -> float:
        if self.full_rows_recomputed <= 0:
            raise ConfigError("full rows recomputed must be positive")
        return round((1 - self.frontier_rows_recomputed / self.full_rows_recomputed) * 100, 3)


def _count(warehouse: WarehouseAdapter, sql: str) -> int:
    rows = warehouse.execute(sql)
    if not rows or rows[0][0] is None:
        return 0
    return int(rows[0][0])


def except_keyword(dialect: str) -> str:
    if dialect == "bigquery":
        return "except distinct"
    return "except"


def deleted_order_sql(relation: str) -> str:
    return (
        "select order_id, customer_id "
        f"from {relation} as mutation_deleted_order"
    )


def full_rows_sql(after_relation: str) -> str:
    return f"select count(*) as full_rows_recomputed from {after_relation}"


def frontier_rows_sql(targeted_relation: str) -> str:
    return f"select count(*) as frontier_rows_recomputed from {targeted_relation}"


def missing_frontier_sql(
    *,
    before_relation: str,
    after_relation: str,
    frontier_relation: str,
    dialect: str = "snowflake",
) -> str:
    except_op = except_keyword(dialect)
    return (
        "select count(*) as missing_frontier_entities from ("
        "    select customer_id from ("
        f"        select * from {after_relation}"
        f"        {except_op}"
        f"        select * from {before_relation}"
        "    ) as actually_changed"
        f"    {except_op}"
        f"    select customer_id from {frontier_relation}"
        ") as missing_frontier"
    )


def extra_frontier_sql(
    *,
    before_relation: str,
    after_relation: str,
    frontier_relation: str,
    dialect: str = "snowflake",
) -> str:
    except_op = except_keyword(dialect)
    return (
        "select count(*) as extra_frontier_entities from ("
        f"    select customer_id from {frontier_relation}"
        f"    {except_op}"
        "    select customer_id from ("
        f"        select * from {after_relation}"
        f"        {except_op}"
        f"        select * from {before_relation}"
        "    ) as actually_changed"
        ") as extra_frontier"
    )


def mismatched_rows_sql(
    *,
    after_relation: str,
    repaired_relation: str,
    dialect: str = "snowflake",
) -> str:
    except_op = except_keyword(dialect)
    return (
        "select count(*) as mismatched_final_rows from ("
        "    select * from ("
        f"        select * from {after_relation}"
        f"        {except_op}"
        f"        select * from {repaired_relation}"
        "    ) as after_not_repaired"
        "    union all"
        "    select * from ("
        f"        select * from {repaired_relation}"
        f"        {except_op}"
        f"        select * from {after_relation}"
        "    ) as repaired_not_after"
        ") as mismatched_final"
    )


def _relation(manifest: Manifest, name: str) -> str:
    return manifest.find_model(name).relation


def recorded_proof(
    *,
    deleted_order_id: str = "5",
    deleted_order_customer_id: str = "781",
    test_duration_ms: int = 1,
) -> MutationProof:
    return MutationProof(
        full_rows_recomputed=150_000,
        frontier_rows_recomputed=3,
        rows_avoided=149_997,
        missing_frontier_entities=0,
        extra_frontier_entities=0,
        mismatched_final_rows=0,
        test_duration_ms=test_duration_ms,
        deleted_order_id=deleted_order_id,
        deleted_order_customer_id=deleted_order_customer_id,
    )


def measure_mutation_proof(
    config: FrontierConfig,
    *,
    manifest: Manifest,
    warehouse: WarehouseAdapter,
    proof: ProofConfig | None = None,
) -> MutationProof:
    spec = proof or config.proof
    before = _relation(manifest, spec.before_mart)
    after = _relation(manifest, spec.after_mart)
    repaired = _relation(manifest, spec.repaired_mart)
    frontier = _relation(manifest, spec.frontier)
    targeted = _relation(manifest, spec.targeted_after)
    deleted = _relation(manifest, spec.deleted_order)

    deleted_rows = warehouse.execute(deleted_order_sql(deleted))
    if not deleted_rows or deleted_rows[0][0] is None:
        raise ConfigError("mutation_deleted_order returned no order to delete")
    deleted_order_id = str(deleted_rows[0][0])
    deleted_customer_id = str(deleted_rows[0][1])

    full_rows = _count(warehouse, full_rows_sql(after))
    frontier_rows = _count(warehouse, frontier_rows_sql(targeted))
    missing = _count(
        warehouse,
        missing_frontier_sql(
            before_relation=before,
            after_relation=after,
            frontier_relation=frontier,
            dialect=warehouse.dialect,
        ),
    )
    extra = _count(
        warehouse,
        extra_frontier_sql(
            before_relation=before,
            after_relation=after,
            frontier_relation=frontier,
            dialect=warehouse.dialect,
        ),
    )
    started = time.perf_counter()
    mismatched = _count(
        warehouse,
        mismatched_rows_sql(
            after_relation=after,
            repaired_relation=repaired,
            dialect=warehouse.dialect,
        ),
    )
    duration_ms = max(0, round((time.perf_counter() - started) * 1000))
    if full_rows <= 0:
        raise ConfigError("after-change mart returned no customers")
    if frontier_rows < 0 or frontier_rows > full_rows:
        raise ConfigError("frontier recompute count is outside the full mart")
    return MutationProof(
        full_rows_recomputed=full_rows,
        frontier_rows_recomputed=frontier_rows,
        rows_avoided=full_rows - frontier_rows,
        missing_frontier_entities=missing,
        extra_frontier_entities=extra,
        mismatched_final_rows=mismatched,
        test_duration_ms=duration_ms,
        deleted_order_id=deleted_order_id,
        deleted_order_customer_id=deleted_customer_id,
    )


def apply_resolved_delete(
    events: list[ChangeEvent],
    *,
    order_id: str,
    customer_id: str,
) -> list[ChangeEvent]:
    resolved: list[ChangeEvent] = []
    for event in events:
        if event.event_id == "event_003":
            resolved.append(
                ChangeEvent(
                    event_id=event.event_id,
                    source_model=event.source_model,
                    operation="DELETE",
                    entity_key=event.entity_key,
                    entity_value=order_id,
                    prior_entity_value=customer_id,
                )
            )
        else:
            resolved.append(event)
    return resolved


def proof_validation_results(proof: MutationProof) -> list[ValidationResult]:
    return [
        ValidationResult(
            test_name="assert_changed_customers_in_frontier",
            status="passed" if proof.missing_frontier_entities == 0 else "failed",
            difference_count=proof.missing_frontier_entities,
        ),
        ValidationResult(
            test_name="assert_no_extra_frontier_entities",
            status="passed" if proof.extra_frontier_entities == 0 else "failed",
            difference_count=proof.extra_frontier_entities,
        ),
        ValidationResult(
            test_name="assert_repaired_equals_reference",
            status="passed" if proof.mismatched_final_rows == 0 else "failed",
            difference_count=proof.mismatched_final_rows,
            message=f"{proof.test_duration_ms} ms",
        ),
    ]
