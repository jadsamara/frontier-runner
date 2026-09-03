from __future__ import annotations

from pathlib import Path

import pytest

from frontier.adapters.bigquery import BigQueryAdapter
from frontier.adapters.databricks import DatabricksAdapter
from frontier.adapters.postgres import PostgresAdapter
from frontier.adapters.redshift import RedshiftAdapter
from frontier.adapters.snowflake import SnowflakeAdapter
from frontier.config import ConfigError
from frontier.proof import except_keyword, mismatched_rows_sql
from frontier.warehouse import (
    FakeWarehouse,
    WAREHOUSE_TYPES,
    normalize_warehouse_type,
    quote_identifier,
    split_relation_parts,
)

CORE_MODULES = ("frontier.py", "proof.py", "validation.py")


def test_supported_types_follow_adapter_order() -> None:
    assert WAREHOUSE_TYPES == (
        "snowflake",
        "bigquery",
        "databricks",
        "postgres",
        "redshift",
    )


def test_normalize_aliases() -> None:
    assert normalize_warehouse_type("PostgreSQL") == "postgres"
    assert normalize_warehouse_type("databricks-sql") == "databricks"
    with pytest.raises(ConfigError, match="Unsupported warehouse type"):
        normalize_warehouse_type("oracle")


def test_quote_identifier_styles() -> None:
    assert SnowflakeAdapter().quote_identifier("order_id") == '"order_id"'
    assert PostgresAdapter().quote_identifier("order_id") == '"order_id"'
    assert RedshiftAdapter().quote_identifier("order_id") == '"order_id"'
    assert BigQueryAdapter().quote_identifier("order_id") == "`order_id`"
    assert DatabricksAdapter().quote_identifier("order_id") == "`order_id`"
    assert quote_identifier("db.schema.table", '"') == '"db"."schema"."table"'


def test_split_quoted_relation() -> None:
    assert split_relation_parts('"DATA_AGENT_DEV"."DBT_DEV"."stg_orders"') == (
        "DATA_AGENT_DEV",
        "DBT_DEV",
        "stg_orders",
    )
    assert split_relation_parts("`project`.`dataset`.`table`") == (
        "project",
        "dataset",
        "table",
    )


def test_fake_warehouse_implements_adapter() -> None:
    warehouse = FakeWarehouse({"select 1": [(1,)]})
    assert warehouse.quote_identifier("customer_id") == '"customer_id"'
    assert warehouse.execute("select 1") == [(1,)]
    assert warehouse.relation_exists("db.schema.table") is True
    assert warehouse.estimate_query_cost("select 1")["estimated"] is False
    assert warehouse.get_query_history("run-1") == []
    warehouse.close()


def test_disconnected_adapters_do_not_import_vendor_clients() -> None:
    with pytest.raises(ConfigError, match="not connected"):
        SnowflakeAdapter().execute("select 1")
    with pytest.raises(ConfigError, match="not connected"):
        PostgresAdapter().execute("select 1")
    with pytest.raises(ConfigError, match="not connected"):
        DatabricksAdapter().execute("select 1")
    with pytest.raises(ConfigError, match="not connected"):
        RedshiftAdapter().execute("select 1")
    with pytest.raises(ConfigError, match="not connected"):
        BigQueryAdapter().execute("select 1")


def test_bigquery_except_uses_distinct() -> None:
    assert except_keyword("bigquery") == "except distinct"
    sql = mismatched_rows_sql(
        after_relation="after_mart",
        repaired_relation="repaired_mart",
        dialect="bigquery",
    )
    assert "except distinct" in sql
    assert sql.count("except") == 2


def test_core_modules_do_not_import_snowflake_client() -> None:
    root = Path(__file__).resolve().parents[1] / "frontier"
    for name in CORE_MODULES:
        text = (root / name).read_text()
        assert "from frontier.snowflake import" not in text
        assert "import frontier.snowflake\n" not in text
        assert "snowflake.connector" not in text
        assert "from frontier.warehouse import" in text
