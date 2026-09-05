from __future__ import annotations

from pathlib import Path

import pytest

from frontier.cdc.config import load_cdc_config
from frontier.cdc.consume import consume_all
from frontier.cdc.prove import create_cdc_affected_keys_sql, prove_batch
from frontier.cdc.store import FakeCdcStore
from frontier.config import ConfigError, load_frontier_config
from frontier.dbt_artifacts import DbtNode, Manifest
from frontier.warehouse import FakeWarehouse
from tests.conftest import FIXTURES

ORDERS = "DATA_AGENT_DEV.FRONTIER_CDC.ORDERS_STREAM"
CUSTOMERS = "DATA_AGENT_DEV.FRONTIER_CDC.CUSTOMER_STREAM"

TARGET_SQL = """
with customers as (
    select c.* from stg_customers c
),
orders as (
    select o.* from stg_orders o
)
select
    c.customer_id,
    c.customer_name,
    count(o.order_id) as total_orders
from customers c
left join orders o on c.customer_id = o.customer_id
group by c.customer_id, c.customer_name
"""

UNION_SQL = "select customer_id from a union select customer_id from b"


def _cdc_config():
    return load_cdc_config(FIXTURES / "frontier-cdc.yml")


def _frontier_config():
    return load_frontier_config(FIXTURES / "frontier.yml")


def _update_pair(*, customer_before: str = "370", customer_after: str = "370") -> list[dict]:
    return [
        {
            "action": "DELETE",
            "is_update": True,
            "row_id": "rid-1",
            "primary_key": "1",
            "target_key": customer_before,
        },
        {
            "action": "INSERT",
            "is_update": True,
            "row_id": "rid-1",
            "primary_key": "1",
            "target_key": customer_after,
        },
    ]


def _manifest(*, sql: str = TARGET_SQL) -> Manifest:
    summary = DbtNode(
        unique_id="model.jaffle_shop.customer_summary",
        name="customer_summary",
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name="DATA_AGENT_DEV.DBT_DEV.customer_summary",
        depends_on=("model.jaffle_shop.int_customer_orders",),
        original_file_path="models/marts/customer_summary.sql",
        compiled_code=sql,
        package_name="jaffle_shop",
    )
    intermediate = DbtNode(
        unique_id="model.jaffle_shop.int_customer_orders",
        name="int_customer_orders",
        resource_type="model",
        database="DATA_AGENT_DEV",
        schema="DBT_DEV",
        relation_name="DATA_AGENT_DEV.DBT_DEV.int_customer_orders",
        depends_on=("model.jaffle_shop.stg_customers", "model.jaffle_shop.stg_orders"),
        original_file_path="models/intermediate/int_customer_orders.sql",
        compiled_code=sql,
        package_name="jaffle_shop",
    )
    return Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={
            summary.unique_id: summary,
            intermediate.unique_id: intermediate,
        },
        sources={},
    )


def _warehouse(*, confirmed: int = 0, targeted: int = 1, missing: int = 0, mismatched: int = 0):
    return FakeWarehouse(
        {
            "frontier_cdc_targeted_count": [(targeted,)],
            "frontier_cdc_confirmed": [(confirmed,)],
            "frontier_cdc_missing_candidates": [(missing,)],
            "frontier_cdc_repair_check": [(mismatched,)],
        }
    )


def _captured(rows: list[dict] | None = None) -> FakeCdcStore:
    store = FakeCdcStore({ORDERS: rows or _update_pair(), CUSTOMERS: []})
    consume_all(_cdc_config(), store=store, project_name="jaffle_shop")
    return store


def _prove(store: FakeCdcStore, warehouse: FakeWarehouse, tmp_path: Path, **kwargs):
    return prove_batch(
        store=store,
        warehouse=warehouse,
        cdc_config=_cdc_config(),
        frontier_config=_frontier_config(),
        manifest=_manifest(sql=kwargs.pop("sql", TARGET_SQL)),
        compiled_root=None,
        project_name="jaffle_shop",
        output_dir=tmp_path,
        **kwargs,
    )


def test_update_same_ownership_one_candidate(tmp_path: Path) -> None:
    store = _captured(_update_pair())
    result = _prove(store, _warehouse(confirmed=0, targeted=1), tmp_path)
    assert result.logical_event_count == 1
    assert result.event_candidate_count == 1
    assert result.sql_change_candidate_count == 0
    assert result.union_candidate_count == 1
    assert result.status == "COMPLETED"
    sql = create_cdc_affected_keys_sql(
        "DATA_AGENT_DEV.DBT_DEV.FRONTIER_TEST_AFFECTED_KEYS",
        "customer_id",
        {"370": 1},
    )
    assert "entity_key" in sql
    assert "event_count" in sql
    assert "'event'" in sql or "origin" in sql


def test_ownership_change_two_candidates(tmp_path: Path) -> None:
    store = _captured(_update_pair(customer_before="370", customer_after="781"))
    result = _prove(store, _warehouse(confirmed=0, targeted=2), tmp_path)
    assert result.union_candidate_count == 2
    assert result.event_candidate_count == 2
    assert result.sql_change_candidate_count == 0


def test_candidate_no_op(tmp_path: Path) -> None:
    store = _captured()
    result = _prove(store, _warehouse(confirmed=0, targeted=1), tmp_path)
    assert result.confirmed_change_count == 0
    assert result.no_op_count == 1
    assert result.validation == "passed"


def test_candidate_confirmed_change(tmp_path: Path) -> None:
    store = _captured()
    result = _prove(store, _warehouse(confirmed=1, targeted=1), tmp_path)
    assert result.confirmed_change_count == 1
    assert result.no_op_count == 0
    assert result.validation == "passed"


def test_unresolved_before_key_fails(tmp_path: Path) -> None:
    store = _captured()
    event = store.events[0][1]
    object.__setattr__(event, "target_key_before", None)
    object.__setattr__(event, "operation", "DELETE")
    object.__setattr__(event, "target_key_after", None)
    with pytest.raises(ConfigError, match="before target key"):
        _prove(store, _warehouse(), tmp_path)
    assert store.batches[0].status == "FAILED"
    assert store.events


def test_targeted_sql_generation_failure(tmp_path: Path) -> None:
    store = _captured()
    with pytest.raises(ConfigError, match="cannot be safely filtered"):
        _prove(store, _warehouse(), tmp_path, sql=UNION_SQL)
    assert store.batches[0].status == "FAILED"
    assert len(store.events) == 1


def test_failed_batch_remains_retryable(tmp_path: Path) -> None:
    store = _captured()

    class Flaky(FakeWarehouse):
        def __init__(self) -> None:
            super().__init__(
                {
                    "frontier_cdc_targeted_count": [(1,)],
                    "frontier_cdc_confirmed": [(0,)],
                    "frontier_cdc_missing_candidates": [(0,)],
                    "frontier_cdc_repair_check": [(0,)],
                }
            )
            self.failed = False

        def execute(self, sql: str):
            if (not self.failed) and "frontier_cdc_targeted_count" in sql.lower():
                self.failed = True
                raise ConfigError("warehouse failure")
            return super().execute(sql)

    with pytest.raises(ConfigError, match="warehouse failure"):
        _prove(store, Flaky(), tmp_path)
    assert store.batches[0].status == "FAILED"
    assert store.events
    result = _prove(store, _warehouse(confirmed=0, targeted=1), tmp_path)
    assert result.status == "COMPLETED"
    assert store.batches[0].status == "COMPLETED"


def test_successful_batch_becomes_completed(tmp_path: Path) -> None:
    store = _captured()
    result = _prove(store, _warehouse(), tmp_path)
    assert result.status == "COMPLETED"
    assert store.batches[0].status == "COMPLETED"
    assert store.get_proof(result.batch_id or "") is not None


def test_completed_batch_cannot_be_reprocessed(tmp_path: Path) -> None:
    store = _captured()
    first = _prove(store, _warehouse(), tmp_path)
    with pytest.raises(ConfigError, match="cannot be reprocessed"):
        _prove(store, _warehouse(), tmp_path, batch_id=first.batch_id)
    second = _prove(store, _warehouse(), tmp_path)
    assert second.status == "NO_BATCH"
    assert len([item for item in store.proofs.values()]) == 1


def test_concurrent_proof_claim(tmp_path: Path) -> None:
    store = _captured()
    batch_id = store.batches[0].batch_id
    assert store.claim_proof(batch_id) is not None
    with pytest.raises(ConfigError, match="already being proved"):
        _prove(store, _warehouse(), tmp_path, batch_id=batch_id)
    assert store.batches[0].status == "PROCESSING"
    assert len(store.events) == 1


def test_no_change_events_csv_dependency(tmp_path: Path, monkeypatch) -> None:
    def boom(*_args, **_kwargs):
        raise AssertionError("change_events.csv should not be read")

    monkeypatch.setattr("frontier.frontier.load_change_events_csv", boom)
    monkeypatch.setattr("frontier.cli.load_change_events_csv", boom)
    store = _captured()
    result = _prove(store, _warehouse(), tmp_path)
    assert result.status == "COMPLETED"


def test_no_frontier_affected_customers_dependency(tmp_path: Path) -> None:
    store = _captured()
    warehouse = _warehouse()
    _prove(store, warehouse, tmp_path)
    joined = "\n".join(warehouse.executed).lower()
    assert "frontier_affected_customers" not in joined
    assert "change_events" not in joined
    assert "stg_orders_mutated" not in joined
    assert "inner join" in joined
    assert "frontier_keys" in joined


def test_entity_ids_absent_from_logs(tmp_path: Path, capsys) -> None:
    store = _captured()
    _prove(store, _warehouse(), tmp_path)
    output = capsys.readouterr().out
    assert "370" not in output
    assert "781" not in output
    assert "O_ORDERKEY" not in output
    assert "customer_id=" not in output
    assert "cdc: batch claim started" in output
    assert "cdc: event routing started" in output
    assert "cdc: candidate materialization started" in output
    assert "cdc: targeted execution started" in output
    assert "cdc: confirmation started" in output
    assert "cdc: validation started" in output
    assert "cdc: cleanup started" in output
    assert "cdc: batch completion completed" in output
    artifact = next(tmp_path.glob("frontier-cdc-repair-*.json"))
    text = artifact.read_text()
    assert "370" not in text
    assert "781" not in text
    assert "delete-insert-candidates" in text
