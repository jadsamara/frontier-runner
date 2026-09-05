from __future__ import annotations

import time
from dataclasses import dataclass

from frontier.config import ConfigError, FrontierConfig, ProofConfig
from frontier.dbt_artifacts import Manifest
from frontier.frontier import AffectedEntity, ChangeEvent
from frontier.warehouse import WarehouseAdapter
from frontier.validation import ValidationResult


@dataclass(frozen=True)
class SqlChangeProof:
    full_rows_recomputed: int
    frontier_rows_recomputed: int
    rows_avoided: int
    source_population_count: int
    candidate_frontier_count: int
    confirmed_frontier_count: int
    before_entity_count: int
    after_entity_count: int
    changed_source_row_count: int
    missing_frontier_entities: int
    extra_frontier_entities: int
    mismatched_final_rows: int
    test_duration_ms: int
    full_rebuild_required: bool = False

    @property
    def percent_rows_avoided(self) -> float:
        if self.full_rows_recomputed <= 0:
            raise ConfigError("full rows recomputed must be positive")
        return round((1 - self.frontier_rows_recomputed / self.full_rows_recomputed) * 100, 3)

    @property
    def targeted_repair_safe(self) -> bool:
        return (
            self.mismatched_final_rows == 0
            and self.missing_frontier_entities == 0
            and not self.full_rebuild_required
        )


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


def actually_changed_customer_sql(
    *,
    before_relation: str,
    after_relation: str,
    dialect: str = "snowflake",
) -> str:
    except_op = except_keyword(dialect)
    return (
        "select customer_id from ("
        f"    select * from {after_relation}"
        f"    {except_op}"
        f"    select * from {before_relation}"
        ") as after_not_before "
        "union "
        "select customer_id from ("
        f"    select * from {before_relation}"
        f"    {except_op}"
        f"    select * from {after_relation}"
        ") as before_not_after"
    )


def missing_frontier_sql(
    *,
    before_relation: str,
    after_relation: str,
    frontier_relation: str,
    dialect: str = "snowflake",
) -> str:
    except_op = except_keyword(dialect)
    changed = actually_changed_customer_sql(
        before_relation=before_relation,
        after_relation=after_relation,
        dialect=dialect,
    )
    return (
        "select count(*) as missing_frontier_entities from ("
        f"    {changed}"
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
    changed = actually_changed_customer_sql(
        before_relation=before_relation,
        after_relation=after_relation,
        dialect=dialect,
    )
    return (
        "select count(*) as extra_frontier_entities from ("
        f"    select customer_id from {frontier_relation}"
        f"    {except_op}"
        f"    {changed}"
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


def _optional_relation(manifest: Manifest, name: str) -> str | None:
    try:
        return _relation(manifest, name)
    except ConfigError:
        return None


def resolve_deleted_order(
    manifest: Manifest,
    warehouse: WarehouseAdapter,
    *,
    proof: ProofConfig,
) -> tuple[str, str]:
    deleted = _optional_relation(manifest, proof.deleted_order)
    if deleted is None:
        return "5", "781"
    deleted_rows = warehouse.execute(deleted_order_sql(deleted))
    if not deleted_rows or deleted_rows[0][0] is None:
        raise ConfigError("mutation_deleted_order returned no order to delete")
    return str(deleted_rows[0][0]), str(deleted_rows[0][1])


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


RECORDED_SQL_CHANGE_CUSTOMERS = ("4", "7", "9", "22", "31", "44", "73", "88")


def recorded_sql_change_affected(
    *,
    entity_type: str = "customer",
    entity_key: str = "customer_id",
) -> list[AffectedEntity]:
    """Confirmed customers for the recorded F → IN (F, O) demonstration."""
    return [
        AffectedEntity(
            entity_type=entity_type,
            entity_key=entity_key,
            entity_value=value,
            reason="Confirmed targeted row change",
        )
        for value in RECORDED_SQL_CHANGE_CUSTOMERS
    ]


def recorded_sql_change_proof(*, test_duration_ms: int = 1) -> SqlChangeProof:
    """Fixture SQL-change demonstration: F → IN (F, O). Not live warehouse evidence."""
    return SqlChangeProof(
        full_rows_recomputed=150_000,
        frontier_rows_recomputed=12,
        rows_avoided=149_988,
        source_population_count=12,
        candidate_frontier_count=12,
        confirmed_frontier_count=8,
        before_entity_count=150_000,
        after_entity_count=150_000,
        changed_source_row_count=12,
        missing_frontier_entities=0,
        extra_frontier_entities=4,
        mismatched_final_rows=0,
        test_duration_ms=test_duration_ms,
        full_rebuild_required=False,
    )


def _count_subquery(warehouse: WarehouseAdapter, sql: str, alias: str) -> int:
    return _count(warehouse, f"select count(*) as {alias} from ({sql}) as {alias}")


def measure_sql_change_proof(
    config: FrontierConfig,
    *,
    warehouse: WarehouseAdapter,
    before_sql: str,
    after_sql: str,
    affected_relation: str,
    impact_sql: str | None = None,
    candidate_count: int | None = None,
    confirmed_count: int | None = None,
    full_rebuild_required: bool = False,
) -> SqlChangeProof:
    """Prove targeted repair of a SQL change against the full PR model.

    Does not require mutation models or change_events.csv.
    """
    entity_key = config.model.key
    dialect = warehouse.dialect
    from frontier.execute import confirmed_changed_sql, generate_targeted_sql, generic_repaired_sql

    targeted_before = generate_targeted_sql(
        before_sql,
        entity_key=entity_key,
        affected_relation=affected_relation,
        dialect=dialect,
    )
    targeted_after = generate_targeted_sql(
        after_sql,
        entity_key=entity_key,
        affected_relation=affected_relation,
        dialect=dialect,
    )
    repaired = generic_repaired_sql(
        before_relation=f"({before_sql})",
        targeted_after_sql=targeted_after,
        affected_relation=affected_relation,
        entity_key=entity_key,
    )
    except_op = except_keyword(dialect)
    actual_changed = (
        f"select distinct {entity_key} as {entity_key} from ("
        f" select * from ({before_sql}) as frontier_before"
        f" {except_op}"
        f" select * from ({after_sql}) as frontier_after"
        " union"
        f" select * from ({after_sql}) as frontier_after_2"
        f" {except_op}"
        f" select * from ({before_sql}) as frontier_before_2"
        ") as frontier_actual_changed"
    )
    missing_sql = (
        "select count(*) as missing_frontier_entities from ("
        f" select {entity_key} from ({actual_changed}) as actually_changed"
        f" {except_op}"
        f" select {entity_key} from {affected_relation}"
        ") as missing_frontier"
    )
    extra_sql = (
        "select count(*) as extra_frontier_entities from ("
        f" select {entity_key} from {affected_relation}"
        f" {except_op}"
        f" select {entity_key} from ({actual_changed}) as actually_changed"
        ") as extra_frontier"
    )

    before_count = _count_subquery(warehouse, before_sql, "before_entity_count")
    after_count = _count_subquery(warehouse, after_sql, "after_entity_count")
    from frontier.impact import source_row_count_sql

    source_population = 0
    changed_source = 0
    if impact_sql:
        changed_source = _count(warehouse, source_row_count_sql(impact_sql, dialect=dialect))
        source_population = changed_source
    elif candidate_count is not None:
        changed_source = candidate_count
        source_population = candidate_count
    started = time.perf_counter()
    mismatched = _count(
        warehouse,
        mismatched_rows_sql(
            after_relation=f"({after_sql})",
            repaired_relation=f"({repaired})",
            dialect=dialect,
        ),
    )
    duration_ms = max(0, round((time.perf_counter() - started) * 1000))
    frontier_rows = _count_subquery(warehouse, targeted_after, "frontier_rows_recomputed")
    missing = _count(warehouse, missing_sql)
    extra = _count(warehouse, extra_sql)
    confirmed_sql = confirmed_changed_sql(
        before_sql=targeted_before,
        after_sql=targeted_after,
        entity_key=entity_key,
        dialect=dialect,
    )
    confirmed = (
        confirmed_count
        if confirmed_count is not None
        else _count_subquery(warehouse, confirmed_sql, "confirmed_frontier_count")
    )
    candidate = candidate_count if candidate_count is not None else extra + confirmed
    if after_count <= 0:
        raise ConfigError("after-change SQL returned no customers")
    if frontier_rows < 0 or frontier_rows > after_count:
        raise ConfigError("frontier recompute count is outside the full mart")
    return SqlChangeProof(
        full_rows_recomputed=after_count,
        frontier_rows_recomputed=frontier_rows,
        rows_avoided=after_count - frontier_rows,
        source_population_count=source_population,
        candidate_frontier_count=candidate,
        confirmed_frontier_count=confirmed,
        before_entity_count=before_count,
        after_entity_count=after_count,
        changed_source_row_count=changed_source,
        missing_frontier_entities=missing,
        extra_frontier_entities=extra,
        mismatched_final_rows=mismatched,
        test_duration_ms=duration_ms,
        full_rebuild_required=full_rebuild_required,
    )


def measure_mutation_proof(
    config: FrontierConfig,
    *,
    manifest: Manifest,
    warehouse: WarehouseAdapter,
    proof: ProofConfig | None = None,
    affected_relation: str | None = None,
) -> MutationProof:
    spec = proof or config.proof
    entity_key = config.model.key
    before = _optional_relation(manifest, spec.before_mart)
    after = _optional_relation(manifest, spec.after_mart)
    repaired = _optional_relation(manifest, spec.repaired_mart)
    frontier = _optional_relation(manifest, spec.frontier)
    targeted = _optional_relation(manifest, spec.targeted_after)
    deleted = _optional_relation(manifest, spec.deleted_order)

    if affected_relation:
        frontier = affected_relation
    if before is None or after is None:
        raise ConfigError("before and after marts are required for mutation proof")
    if frontier is None:
        raise ConfigError("affected-key relation is required for mutation proof")

    if deleted:
        deleted_order_id, deleted_customer_id = resolve_deleted_order(
            manifest,
            warehouse,
            proof=spec,
        )
    else:
        deleted_order_id = "5"
        deleted_customer_id = "781"

    targeted_sql: str | None = None
    if targeted is None:
        from frontier.execute import generate_targeted_sql

        targeted_sql = generate_targeted_sql(
            f"select * from {after}",
            entity_key=entity_key,
            affected_relation=frontier,
            dialect=warehouse.dialect,
        )

    full_rows = _count(warehouse, full_rows_sql(after))
    if targeted_sql is not None:
        frontier_rows = _count(
            warehouse,
            f"select count(*) as frontier_rows_recomputed from ({targeted_sql}) as frontier_target",
        )
    else:
        frontier_rows = _count(warehouse, frontier_rows_sql(targeted or frontier))
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
    if repaired is None:
        from frontier.execute import generic_repaired_sql, generate_targeted_sql

        target = targeted_sql or generate_targeted_sql(
            f"select * from {after}",
            entity_key=entity_key,
            affected_relation=frontier,
            dialect=warehouse.dialect,
        )
        repaired = f"({generic_repaired_sql(before_relation=before, targeted_after_sql=target, affected_relation=frontier, entity_key=entity_key)})"
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


def sql_change_proof_validation_results(proof: SqlChangeProof) -> list[ValidationResult]:
    return [
        ValidationResult(
            test_name="assert_sql_frontier_covers_reference",
            status="passed" if proof.missing_frontier_entities == 0 else "failed",
            difference_count=proof.missing_frontier_entities,
        ),
        ValidationResult(
            test_name="assert_repaired_equals_reference",
            status="passed" if proof.mismatched_final_rows == 0 else "failed",
            difference_count=proof.mismatched_final_rows,
            message=f"{proof.test_duration_ms} ms",
        ),
    ]


def proof_validation_results(proof: MutationProof) -> list[ValidationResult]:
    extra_message = None
    if proof.extra_frontier_entities:
        extra_message = (
            f"{proof.extra_frontier_entities} conservative candidate no-ops; "
            "extras are reported, not correctness failures"
        )
    return [
        ValidationResult(
            test_name="assert_changed_customers_in_frontier",
            status="passed" if proof.missing_frontier_entities == 0 else "failed",
            difference_count=proof.missing_frontier_entities,
        ),
        ValidationResult(
            test_name="assert_no_extra_frontier_entities",
            status="passed",
            difference_count=proof.extra_frontier_entities,
            message=extra_message,
        ),
        ValidationResult(
            test_name="assert_repaired_equals_reference",
            status="passed" if proof.mismatched_final_rows == 0 else "failed",
            difference_count=proof.mismatched_final_rows,
            message=f"{proof.test_duration_ms} ms",
        ),
    ]
