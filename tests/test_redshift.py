from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from frontier.adapters.redshift import (
    RedshiftAdapter,
    RedshiftConnectConfig,
    load_redshift_config,
    open_redshift_adapter,
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


class FakeRedshiftCursor:
    def __init__(self, connection: "FakeRedshiftConnection") -> None:
        self.connection = connection
        self.description = None
        self.query_id: str | None = None
        self._rows: list[tuple[object, ...]] = []

    def execute(self, sql: str, params: object = None) -> None:
        lowered = sql.lower()
        self.connection.executed.append({"sql": sql, "params": params})
        if "pg_last_query_id" in lowered:
            self.description = [("pg_last_query_id",)]
            self._rows = [(self.connection.last_qid,)]
            return
        if self.connection.error is not None and "stl_" not in lowered and "svl_" not in lowered:
            self.connection.n += 1
            self.connection.last_qid = self.connection.n
            self.query_id = str(self.connection.n)
            raise self.connection.error
        if "stl_query" in lowered or "stl_wlm" in lowered or "svl_query_summary" in lowered:
            self.description = [("query",), ("status",)]
            qid = params[0] if isinstance(params, tuple) and params else 11
            if self.connection.history_error is not None:
                raise self.connection.history_error
            if self.connection.history_rows is not None:
                self._rows = list(self.connection.history_rows)
            else:
                self._rows = [(str(qid), "completed", 42, 5, 30, 4096)]
            return
        self.connection.n += 1
        self.connection.last_qid = self.connection.n
        self.query_id = str(self.connection.n)
        if lowered.lstrip().startswith(("create ", "drop ", "set ", "insert ", "delete ")):
            self.description = None
            self._rows = []
            return
        self.description = [("col",)]
        self._rows = list(self.connection.rows)

    def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)

    def fetchone(self) -> tuple[object, ...] | None:
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        return None


class FakeRedshiftConnection:
    def __init__(
        self,
        *,
        rows: list[tuple[object, ...]] | None = None,
        error: Exception | None = None,
        history_error: Exception | None = None,
        history_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.rows = rows if rows is not None else [(1,)]
        self.error = error
        self.history_error = history_error
        self.history_rows = history_rows
        self.executed: list[dict[str, object]] = []
        self.n = 0
        self.last_qid = 0
        self.closed = False

    def cursor(self) -> FakeRedshiftCursor:
        return FakeRedshiftCursor(self)

    def close(self) -> None:
        self.closed = True


def test_pyproject_exposes_redshift_extra_without_removing_others() -> None:
    text = (RUNNER_ROOT / "pyproject.toml").read_text()
    assert 'snowflake = ["snowflake-connector-python>=3.12,<4"]' in text
    assert 'bigquery = ["google-cloud-bigquery>=3.25,<4"]' in text
    assert 'redshift = ["redshift-connector>=2.1.5,<3"]' in text


def test_redshift_module_does_not_import_other_warehouses() -> None:
    source = (RUNNER_ROOT / "frontier" / "adapters" / "redshift.py").read_text()
    assert "snowflake.connector" not in source
    assert "from frontier.adapters.snowflake" not in source
    assert "google.cloud" not in source
    assert "from frontier.adapters.bigquery" not in source
    assert "SNOWFLAKE_" not in source
    assert "BIGQUERY_" not in source


def test_quote_schema_ddl_and_isolated_tables() -> None:
    adapter = RedshiftAdapter(host="example.region.redshift.amazonaws.com", database="analytics", schema="dbt_ci")
    assert adapter.quote_identifier("order_id") == '"order_id"'
    relation = qualify_relation("analytics", "dbt_ci", "FRONTIER_PR1_AFFECTED_KEYS", dialect="redshift")
    assert relation == "analytics.dbt_ci.FRONTIER_PR1_AFFECTED_KEYS"
    sql = create_schema_sql("analytics", "dbt_ci", dialect="redshift")
    assert sql == "create schema if not exists dbt_ci"
    first = affected_keys_relation("jaffle_shop-pr-1", database="analytics", schema="dbt_ci", dialect="redshift")
    second = affected_keys_relation("jaffle_shop-pr-2", database="analytics", schema="dbt_ci", dialect="redshift")
    assert first != second
    database, schema = isolated_location(
        model_database="analytics",
        model_schema="dbt_ci",
        dialect="redshift",
    )
    assert database == "analytics"
    assert schema == "dbt_ci"


def test_adapter_contract_includes_redshift() -> None:
    warehouse = FakeWarehouse({"select 1": [(1,)]}, warehouse_type="redshift")
    assert warehouse.warehouse_type == "redshift"
    assert warehouse.dialect == "redshift"
    rows = warehouse.execute("select 1")
    assert rows == [(1,)]
    assert warehouse.last_query_id
    profile = warehouse.get_query_profile(warehouse.last_query_id)
    assert profile["query_id"] == warehouse.last_query_id
    warehouse.close()


def test_concurrent_isolated_runs_use_distinct_key_tables() -> None:
    warehouse = FakeWarehouse({"full_entity_count": [(10,)]}, warehouse_type="redshift")
    first = open_isolated_run(
        warehouse,
        run_id="jaffle_shop-aaa",
        entity_key="customer_id",
        model_database="analytics",
        model_schema="dbt_ci",
    )
    second = open_isolated_run(
        warehouse,
        run_id="jaffle_shop-bbb",
        entity_key="customer_id",
        model_database="analytics",
        model_schema="dbt_ci",
    )
    assert first.relation != second.relation
    first.materialize(["1"])
    second.materialize(["2"])
    created = [sql for sql in warehouse.executed if "create table" in sql.lower() and "or replace" not in sql.lower()]
    assert created
    assert all("create or replace table" not in sql.lower() for sql in warehouse.executed)
    first.cleanup()
    second.cleanup()
    dropped = [sql for sql in warehouse.executed if sql.lower().startswith("drop table")]
    assert any(first.relation in sql for sql in dropped)
    assert any(second.relation in sql for sql in dropped)


def test_redshift_execute_records_query_id_and_history_metrics() -> None:
    connection = FakeRedshiftConnection(rows=[(3,)])
    adapter = RedshiftAdapter(
        connection,
        host="example.region.redshift.amazonaws.com",
        database="analytics",
        schema="dbt_ci",
        user="ci_user",
    )
    rows = adapter.execute("select 1")
    assert rows == [(3,)]
    assert adapter.last_query_id == "1"
    profile = adapter.get_query_profile(adapter.last_query_id)
    assert profile["query_id"] == "1"
    assert profile["status"] == "completed"
    assert profile["queue_ms"] == 5
    assert profile["execution_ms"] == 30
    assert profile["bytes_scanned"] == 4096
    assert "cost" not in profile
    bound = [item for item in connection.executed if item["params"]]
    assert bound
    dumped = str(adapter.describe())
    assert "password" not in dumped.lower() or "********" in dumped


def test_history_permission_failure_marks_metrics_unavailable() -> None:
    connection = FakeRedshiftConnection(
        rows=[(1,)],
        history_error=RuntimeError("permission denied for relation stl_query"),
    )
    adapter = RedshiftAdapter(connection, database="analytics", schema="dbt_ci")
    adapter.execute("select 1")
    profile = adapter.get_query_profile(adapter.last_query_id or "1")
    assert profile["query_id"] == "1"
    assert profile["metrics_available"] is False
    assert profile["bytes_scanned"] is None


def test_redshift_query_failure_raises_and_keeps_query_id() -> None:
    connection = FakeRedshiftConnection(error=RuntimeError("permission denied for schema dbt_ci"))
    adapter = RedshiftAdapter(connection, database="analytics", schema="dbt_ci")
    with pytest.raises(RuntimeError, match="permission denied"):
        adapter.execute("select 1")
    assert adapter.last_query_id == "1"
    profile = adapter.get_query_profile("1")
    assert profile["error"] == "RuntimeError"


def test_retries_retryable_connection_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = FakeRedshiftConnection(rows=[(1,)])
    adapter = RedshiftAdapter(connection, database="analytics", schema="dbt_ci")
    attempts = {"n": 0}

    def flaky(sql: str, *, params=None, capture_id: bool = True):
        del params, capture_id
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("connection timed out")
        adapter.last_query_id = "9"
        return [(1,)]

    monkeypatch.setattr(adapter, "_execute_once", flaky)
    monkeypatch.setattr("frontier.adapters.redshift.time.sleep", lambda _seconds: None)
    assert adapter.execute("select 1") == [(1,)]
    assert attempts["n"] == 3


def test_permission_errors_are_not_retried() -> None:
    connection = FakeRedshiftConnection(error=RuntimeError("permission denied for schema dbt_prod"))
    adapter = RedshiftAdapter(connection, database="analytics", schema="dbt_ci")
    with pytest.raises(RuntimeError, match="permission denied"):
        adapter.execute("select 1")
    selects = [item for item in connection.executed if str(item["sql"]).lower().startswith("select 1")]
    assert len(selects) == 1


def test_permission_failure_is_execution_failed_not_empty_pass() -> None:
    class FailingWarehouse(FakeWarehouse):
        def execute(self, sql: str):
            lowered = sql.lower()
            if "create table" in lowered and "or replace" not in lowered and "target_base" in lowered:
                raise RuntimeError("permission denied for schema dbt_ci")
            return super().execute(sql)

    warehouse = FailingWarehouse(
        {
            "full_entity_count": [(150_000,)],
            "frontier_origin_counts": [(0, 3, 3)],
        },
        warehouse_type="redshift",
    )
    from frontier.config import FrontierConfig, ModelConfig
    from frontier.dbt_artifacts import DbtNode, Manifest

    model = DbtNode(
        unique_id="model.jaffle_shop.customer_summary",
        name="customer_summary",
        resource_type="model",
        database="analytics",
        schema="dbt_ci",
        relation_name="analytics.dbt_ci.customer_summary",
        depends_on=(),
        compiled_code="select customer_id from orders",
        columns=("customer_id",),
    )
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="redshift",
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
        model_database="analytics",
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
    assert "super-secret" not in dumped
    session.cleanup()


def test_unsupported_redshift_sql_is_full_rebuild() -> None:
    change = classify_sql_change(
        "select customer_id from orders",
        "select my_udf(customer_id) as customer_id from orders",
        dialect="redshift",
    )
    assert "UNSUPPORTED" in change.kinds
    compiled = compile_impact_query(
        "select customer_id from orders",
        "select my_udf(customer_id) as customer_id from orders",
        entity_key="customer_id",
        dialect="redshift",
    )
    assert compiled.status == "FULL_REBUILD_REQUIRED"
    assert compiled.candidate_sql is None


def test_grouping_change_fails_closed() -> None:
    compiled = compile_impact_query(
        "select customer_id, count(*) as orders from stg_orders group by customer_id",
        "select customer_id, status, count(*) as orders from stg_orders group by customer_id, status",
        entity_key="customer_id",
        dialect="redshift",
    )
    assert compiled.status == "FULL_REBUILD_REQUIRED"


def test_redshift_filter_change_compiles_quoted_sql() -> None:
    base = 'select customer_id from "analytics"."dbt_ci"."orders" where status = \'paid\''
    pr = (
        'select customer_id from "analytics"."dbt_ci"."orders" '
        "where status = 'paid' and amount is distinct from 0"
    )
    parsed = parse_snowflake_sql(pr, dialect="redshift")
    assert parsed.ok
    change = classify_sql_change(base, pr, dialect="redshift")
    assert "FILTER_CHANGED" in change.kinds
    compiled = compile_impact_query(base, pr, entity_key="customer_id", dialect="redshift")
    assert compiled.status == "COMPILED"
    assert compiled.candidate_sql
    lowered = compiled.candidate_sql.lower()
    assert "is distinct from" in lowered or "is not" in lowered
    snowflake = compile_impact_query(base, pr, entity_key="customer_id", dialect="snowflake")
    assert snowflake.status == "COMPILED"
    assert compiled.candidate_sql is not None
    assert snowflake.candidate_sql is not None


def test_open_redshift_requires_extra_and_does_not_need_snowflake(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("BIGQUERY_PROJECT", raising=False)
    monkeypatch.setenv("REDSHIFT_HOST", "example.region.redshift.amazonaws.com")
    monkeypatch.setenv("REDSHIFT_USER", "ci_user")
    monkeypatch.setenv("REDSHIFT_PASSWORD", "super-secret-password")
    monkeypatch.setenv("REDSHIFT_DATABASE", "analytics")
    monkeypatch.setenv("REDSHIFT_SCHEMA", "dbt_ci")
    config = load_redshift_config({})
    assert config.host == "example.region.redshift.amazonaws.com"
    assert config.database == "analytics"
    assert config.user == "ci_user"
    assert "super-secret-password" not in repr(config)
    assert "********" in repr(config)

    class DummyConnection:
        def cursor(self) -> FakeRedshiftCursor:
            return FakeRedshiftCursor(FakeRedshiftConnection())

        def close(self) -> None:
            return None

    def fake_connect(**kwargs: object) -> DummyConnection:
        assert "password" in kwargs
        assert kwargs["password"] == "super-secret-password"
        assert "snowflake" not in str(kwargs).lower()
        return DummyConnection()

    try:
        import redshift_connector as real_rs

        monkeypatch.setattr(real_rs, "connect", fake_connect)
    except ImportError:
        fake = ModuleType("redshift_connector")
        fake.connect = fake_connect
        monkeypatch.setitem(sys.modules, "redshift_connector", fake)
    adapter = open_redshift_adapter({})
    assert adapter.warehouse_type == "redshift"
    assert adapter.database == "analytics"
    dumped = str(adapter.describe())
    assert "super-secret-password" not in dumped
    assert "SNOWFLAKE" not in dumped
    assert "BIGQUERY" not in dumped


def test_iam_config_does_not_require_password(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REDSHIFT_PASSWORD", raising=False)
    monkeypatch.setenv("REDSHIFT_IAM", "1")
    monkeypatch.setenv("REDSHIFT_HOST", "example.region.redshift.amazonaws.com")
    monkeypatch.setenv("REDSHIFT_USER", "iam_user")
    monkeypatch.setenv("REDSHIFT_DATABASE", "analytics")
    monkeypatch.setenv("REDSHIFT_CLUSTER_ID", "analytics-cluster")
    monkeypatch.setenv("REDSHIFT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-token")
    config = load_redshift_config({"method": "iam"})
    assert config.method == "iam"
    kwargs = config.connect_kwargs()
    assert kwargs["iam"] is True
    assert kwargs["db_user"] == "iam_user"
    assert "password" not in kwargs
    dumped = repr(config) + str(redact_describe(config))
    assert "aws-secret-token" not in dumped


def redact_describe(config: RedshiftConnectConfig) -> dict[str, object]:
    adapter = RedshiftAdapter(config=config, database=config.database, schema=config.schema, user=config.user)
    return adapter.describe()


def test_connect_config_hides_password() -> None:
    config = RedshiftConnectConfig(
        host="example.region.redshift.amazonaws.com",
        database="analytics",
        user="ci_user",
        password="super-secret-password",
    )
    assert "super-secret-password" not in repr(config)
    assert "********" in repr(config)


def test_isolated_run_cleanup_and_no_prod_writes() -> None:
    warehouse = FakeWarehouse(warehouse_type="redshift")
    with pytest.raises(ConfigError, match="PROD"):
        open_isolated_run(
            warehouse,
            run_id="jaffle_shop-prod",
            entity_key="customer_id",
            model_database="analytics",
            model_schema="DBT_PROD",
        )
    session = IsolatedRun(
        warehouse=warehouse,
        relation="analytics.dbt_ci.FRONTIER_RUN1_AFFECTED_KEYS",
        database="analytics",
        schema="dbt_ci",
        run_id="run1",
        entity_key="customer_id",
    )
    session.materialize(["1", "2"])
    assert any("create schema if not exists dbt_ci" in sql for sql in warehouse.executed)
    assert any(sql.lower().startswith("drop table if exists") for sql in warehouse.executed)
    assert any(sql.lower().startswith("create table ") for sql in warehouse.executed)
    session.cleanup()
    assert session._cleaned is True


def test_failed_cleanup_does_not_raise() -> None:
    class CleanupFail(FakeWarehouse):
        def execute(self, sql: str):
            if sql.lower().startswith("drop table"):
                raise RuntimeError("could not drop")
            return super().execute(sql)

    warehouse = CleanupFail(warehouse_type="redshift")
    session = IsolatedRun(
        warehouse=warehouse,
        relation="analytics.dbt_ci.FRONTIER_RUN1_AFFECTED_KEYS",
        database="analytics",
        schema="dbt_ci",
        run_id="run1",
        entity_key="customer_id",
    )
    session.materialize(["1"])
    session.cleanup()
    assert session._cleaned is True
