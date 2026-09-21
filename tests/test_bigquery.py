from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from frontier.adapters.bigquery import (
    BigQueryAdapter,
    BigQueryConnectConfig,
    load_bigquery_config,
    open_bigquery_adapter,
)
from frontier.config import ConfigError
from frontier.execute import (
    IsolatedRun,
    affected_keys_relation,
    create_schema_sql,
    isolated_location,
    open_isolated_run,
    qualify_relation,
)
from frontier.frontier import run_frontier
from frontier.impact import compile_impact_query
from frontier.snowflake_sql import classify_sql_change, parse_snowflake_sql
from frontier.warehouse import FakeWarehouse


RUNNER_ROOT = Path(__file__).resolve().parents[1]


class FakeRow:
    def __init__(self, values: tuple[object, ...]) -> None:
        self._values = values

    def values(self) -> tuple[object, ...]:
        return self._values


class FakeQueryJob:
    def __init__(
        self,
        rows: list[tuple[object, ...]] | None = None,
        *,
        job_id: str = "bq-job-1",
        error: Exception | None = None,
        bytes_processed: int = 2048,
        location: str = "US",
    ) -> None:
        self._rows = rows or []
        self.job_id = job_id
        self.error = error
        self.total_bytes_processed = bytes_processed
        self.total_bytes_billed = bytes_processed
        self.slot_millis = 11
        self.location = location
        self.cache_hit = False
        self.state = "DONE"

    def result(self, timeout: object = None) -> list[FakeRow]:
        del timeout
        if self.error is not None:
            raise self.error
        return [FakeRow(row) for row in self._rows]


class FakeBigQueryClient:
    def __init__(
        self,
        *,
        rows: list[tuple[object, ...]] | None = None,
        error: Exception | None = None,
        location: str = "US",
    ) -> None:
        self.rows = rows if rows is not None else [(1,)]
        self.error = error
        self.location = location
        self.executed: list[dict[str, object]] = []
        self.n = 0
        self.closed = False

    def query(self, sql: str, job_config: object = None, location: str | None = None) -> FakeQueryJob:
        del job_config
        self.executed.append({"sql": sql, "location": location})
        self.n += 1
        return FakeQueryJob(
            [] if sql.lower().lstrip().startswith(("create ", "drop ")) else list(self.rows),
            job_id=f"bq-job-{self.n}",
            error=self.error,
            location=location or self.location,
        )

    def close(self) -> None:
        self.closed = True


def test_pyproject_exposes_bigquery_extra_without_removing_snowflake() -> None:
    text = (RUNNER_ROOT / "pyproject.toml").read_text()
    assert 'snowflake = ["snowflake-connector-python>=3.12,<4"]' in text
    assert 'bigquery = ["google-cloud-bigquery>=3.25,<4"]' in text


def test_bigquery_module_does_not_import_snowflake() -> None:
    source = (RUNNER_ROOT / "frontier" / "adapters" / "bigquery.py").read_text()
    assert "snowflake.connector" not in source
    assert "from frontier.adapters.snowflake" not in source
    assert "SNOWFLAKE_" not in source


def test_quote_and_qualify_hyphenated_project() -> None:
    adapter = BigQueryAdapter(project="acme-analytics", dataset="dbt_ci", location="EU")
    assert adapter.quote_identifier("order_id") == "`order_id`"
    assert adapter.last_query_id is None
    relation = qualify_relation(
        "acme-analytics",
        "dbt_ci",
        "FRONTIER_PR1_AFFECTED_KEYS",
        dialect="bigquery",
    )
    assert relation == "`acme-analytics`.`dbt_ci`.`FRONTIER_PR1_AFFECTED_KEYS`"
    sql = create_schema_sql("acme-analytics", "dbt_ci", dialect="bigquery")
    assert sql == "create schema if not exists `acme-analytics.dbt_ci`"


def test_isolated_location_allows_hyphenated_project() -> None:
    database, schema = isolated_location(
        model_database="acme-analytics",
        model_schema="dbt_ci",
        dialect="bigquery",
    )
    assert database == "acme-analytics"
    assert schema == "dbt_ci"
    first = affected_keys_relation(
        "jaffle_shop-pr-1",
        database=database,
        schema=schema,
        dialect="bigquery",
    )
    second = affected_keys_relation(
        "jaffle_shop-pr-2",
        database=database,
        schema=schema,
        dialect="bigquery",
    )
    assert first != second
    assert first.startswith("`acme-analytics`.`dbt_ci`.`FRONTIER_")
    with pytest.raises(ConfigError, match="not a safe identifier"):
        isolated_location(
            model_database="acme-analytics",
            model_schema="dbt-ci",
            dialect="bigquery",
        )


def test_adapter_contract_shared_by_fake_warehouses() -> None:
    for kind in ("snowflake", "bigquery", "redshift"):
        warehouse = FakeWarehouse({"select 1": [(1,)]}, warehouse_type=kind)
        assert warehouse.warehouse_type == kind
        assert warehouse.dialect == kind
        rows = warehouse.execute("select 1")
        assert rows == [(1,)]
        assert warehouse.last_query_id
        profile = warehouse.get_query_profile(warehouse.last_query_id)
        assert profile["query_id"] == warehouse.last_query_id
        warehouse.close()


def test_concurrent_isolated_runs_do_not_share_key_tables() -> None:
    warehouse = FakeWarehouse(
        {"full_entity_count": [(10,)]},
        warehouse_type="bigquery",
    )
    first = open_isolated_run(
        warehouse,
        run_id="jaffle_shop-aaa",
        entity_key="customer_id",
        model_database="acme_analytics",
        model_schema="dbt_ci",
    )
    second = open_isolated_run(
        warehouse,
        run_id="jaffle_shop-bbb",
        entity_key="customer_id",
        model_database="acme_analytics",
        model_schema="dbt_ci",
    )
    assert first.relation != second.relation
    first.materialize(["1"])
    second.materialize(["2"])
    first.cleanup()
    second.cleanup()
    dropped = [sql for sql in warehouse.executed if sql.lower().startswith("drop table")]
    assert any(first.relation in sql for sql in dropped)
    assert any(second.relation in sql for sql in dropped)


def test_bigquery_execute_records_job_id_and_bytes_not_cost() -> None:
    client = FakeBigQueryClient(rows=[(3,)])
    adapter = BigQueryAdapter(
        client,
        project="acme-analytics",
        dataset="dbt_ci",
        location="EU",
    )
    rows = adapter.execute("select count(*) from `acme-analytics.dbt_ci.orders`")
    assert rows == [(3,)]
    assert adapter.last_query_id == "bq-job-1"
    profile = adapter.get_query_profile(adapter.last_query_id)
    assert profile["job_id"] == "bq-job-1"
    assert profile["total_bytes_processed"] == 2048
    assert profile["bytes_scanned"] == 2048
    assert "cost" not in profile
    assert "savings" not in profile
    assert client.executed[0]["location"] == "EU"
    dumped = str(adapter.describe())
    assert "password" not in dumped
    assert "private_key" not in dumped


def test_bigquery_job_failure_raises_and_keeps_job_id() -> None:
    client = FakeBigQueryClient(error=RuntimeError("Access Denied: dataset dbt_prod"))
    adapter = BigQueryAdapter(client, project="acme-analytics", dataset="dbt_ci")
    with pytest.raises(RuntimeError, match="Access Denied"):
        adapter.execute("select 1")
    assert adapter.last_query_id == "bq-job-1"
    profile = adapter.get_query_profile("bq-job-1")
    assert profile["error"] == "RuntimeError"


def test_permission_failure_is_execution_failed_not_empty_pass() -> None:
    class FailingWarehouse(FakeWarehouse):
        def execute(self, sql: str):
            lowered = sql.lower()
            if "create or replace table" in lowered and "target_base" in lowered:
                raise RuntimeError("403 Access Denied: dataset dbt_ci")
            return super().execute(sql)

    warehouse = FailingWarehouse(
        {
            "full_entity_count": [(150_000,)],
            "frontier_origin_counts": [(0, 3, 3)],
        },
        warehouse_type="bigquery",
    )
    from frontier.config import FrontierConfig, ModelConfig
    from frontier.dbt_artifacts import DbtNode, Manifest

    model = DbtNode(
        unique_id="model.jaffle_shop.customer_summary",
        name="customer_summary",
        resource_type="model",
        database="acme_analytics",
        schema="dbt_ci",
        relation_name="acme_analytics.dbt_ci.customer_summary",
        depends_on=(),
        compiled_code="select customer_id from orders",
        columns=("customer_id",),
    )
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="bigquery",
        nodes={model.unique_id: model},
        sources={},
    )
    config = FrontierConfig(
        project="jaffle_shop",
        environment="ci",
        model=ModelConfig(name="customer_summary", entity="customer", key="customer_id", grain="customer"),
        relations={},
    )
    session = open_isolated_run(
        warehouse,
        run_id="jaffle_shop-fail",
        entity_key="customer_id",
        model_database="acme_analytics",
        model_schema="dbt_ci",
    )
    result = run_frontier(
        config,
        manifest=manifest,
        events=[],
        warehouse=warehouse,
        persist=True,
        confirm=True,
        isolated_run=session,
        extra_keys=[],
        sql_change_queries=["select customer_id from orders where status = 'paid'"],
        sql_change_required=True,
        before_sql="select customer_id from orders where status = 'paid'",
        after_sql="select customer_id from orders where status = 'paid' and amount > 10",
        run_id="jaffle_shop-fail",
    )
    assert result.execution_failed is True
    assert result.proof_status == "EXECUTION_FAILED"
    assert result.sql_change_candidate_count == 3
    assert result.union_candidate_count == 3
    assert result.frontier_entity_count == 3
    dumped = str(result.failure_reason or "")
    assert "customer_id" not in dumped or "Access Denied" in dumped
    session.cleanup()


def test_unsupported_bigquery_sql_is_full_rebuild() -> None:
    change = classify_sql_change(
        "select customer_id from orders",
        "select my_udf(customer_id) as customer_id from orders",
        dialect="bigquery",
    )
    assert "UNSUPPORTED" in change.kinds
    compiled = compile_impact_query(
        "select customer_id from orders",
        "select my_udf(customer_id) as customer_id from orders",
        entity_key="customer_id",
        dialect="bigquery",
    )
    assert compiled.status == "FULL_REBUILD_REQUIRED"
    assert compiled.candidate_sql is None


def test_bigquery_filter_change_compiles_backtick_sql() -> None:
    base = "select customer_id from `acme-analytics.dbt_ci.orders` where status = 'paid'"
    pr = "select customer_id from `acme-analytics.dbt_ci.orders` where status = 'paid' and amount > 10"
    parsed = parse_snowflake_sql(pr, dialect="bigquery")
    assert parsed.ok
    change = classify_sql_change(base, pr, dialect="bigquery")
    assert "FILTER_CHANGED" in change.kinds
    compiled = compile_impact_query(base, pr, entity_key="customer_id", dialect="bigquery")
    assert compiled.status == "COMPILED"
    assert compiled.candidate_sql
    assert "except" not in compiled.candidate_sql.lower() or "except distinct" in compiled.candidate_sql.lower()


def test_open_bigquery_requires_extra_and_does_not_need_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.setenv("BIGQUERY_PROJECT", "acme-analytics")
    monkeypatch.setenv("BIGQUERY_DATASET", "dbt_ci")
    monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
    config = load_bigquery_config({})
    assert config.project == "acme-analytics"
    assert config.dataset == "dbt_ci"
    assert config.location == "EU"
    assert "super-secret" not in repr(config)

    class DummyClient:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def close(self) -> None:
            return None

    try:
        from google.cloud import bigquery as real_bq

        monkeypatch.setattr(real_bq, "Client", DummyClient)
    except ImportError:
        fake = ModuleType("google.cloud.bigquery")
        fake.Client = DummyClient
        fake.QueryJobConfig = object
        monkeypatch.setitem(sys.modules, "google.cloud.bigquery", fake)
        monkeypatch.setitem(sys.modules, "google.cloud", SimpleNamespace(bigquery=fake))
        monkeypatch.setitem(sys.modules, "google", SimpleNamespace(cloud=SimpleNamespace(bigquery=fake)))
    adapter = open_bigquery_adapter({})
    assert adapter.warehouse_type == "bigquery"
    assert adapter.project == "acme-analytics"
    assert adapter.dataset == "dbt_ci"
    assert "SNOWFLAKE" not in str(adapter.describe())


def test_connect_config_hides_keyfile() -> None:
    config = BigQueryConnectConfig(
        project="acme-analytics",
        dataset="dbt_ci",
        keyfile="/tmp/sa.json",
    )
    assert "sa.json" not in repr(config)
    assert "********" in repr(config)


def test_isolated_run_cleanup_and_no_prod_writes() -> None:
    warehouse = FakeWarehouse(warehouse_type="bigquery")
    with pytest.raises(ConfigError, match="PROD"):
        open_isolated_run(
            warehouse,
            run_id="jaffle_shop-prod",
            entity_key="customer_id",
            model_database="acme_analytics",
            model_schema="DBT_PROD",
        )
    session = IsolatedRun(
        warehouse=warehouse,
        relation="`acme_analytics`.`dbt_ci`.`FRONTIER_RUN1_AFFECTED_KEYS`",
        database="acme_analytics",
        schema="dbt_ci",
        run_id="run1",
        entity_key="customer_id",
    )
    session.materialize(["1", "2"])
    assert any("create schema if not exists `acme_analytics.dbt_ci`" in sql for sql in warehouse.executed)
    session.cleanup()
    assert session._cleaned is True
