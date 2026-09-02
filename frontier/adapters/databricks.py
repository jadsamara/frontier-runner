from __future__ import annotations

from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.warehouse import env_value, quote_identifier, sql_string


class DatabricksAdapter(CursorAdapter):
    warehouse_type = "databricks"
    dialect = "databricks"

    def __init__(
        self,
        connection: Any | None = None,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        host: str | None = None,
        http_path: str | None = None,
    ):
        self._connection = connection
        self.catalog = catalog
        self.schema = schema
        self.host = host
        self.http_path = http_path

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, "`")

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        tagged = sql_string(run_id)
        try:
            rows = self.execute(
                "select statement_id, status "
                "from system.query.history "
                f"where statement_text like '%' || {tagged} || '%' "
                "order by start_time desc limit 50"
            )
        except Exception:
            return []
        return [{"query_id": row[0], "status": row[1]} for row in rows]

    def describe(self) -> dict[str, Any]:
        return redact(
            {
                "warehouse_type": self.warehouse_type,
                "host": self.host,
                "http_path": self.http_path,
                "catalog": self.catalog,
                "schema": self.schema,
                "token": env_value("DATABRICKS_TOKEN"),
            }
        )


def open_databricks_adapter(settings: dict[str, Any]) -> DatabricksAdapter:
    try:
        from databricks import sql as databricks_sql
    except ImportError as error:
        raise ConfigError(
            "Install frontier-runner[databricks] to open a Databricks SQL session",
        ) from error
    host = env_value("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HOST") or settings.get(
        "host",
    ) or settings.get("server_hostname")
    http_path = env_value("DATABRICKS_HTTP_PATH") or settings.get("http_path")
    token = env_value("DATABRICKS_TOKEN") or settings.get("token")
    catalog = env_value("DATABRICKS_CATALOG") or settings.get("catalog")
    schema = env_value("DATABRICKS_SCHEMA") or settings.get("schema")
    if not host or not http_path or not token:
        raise ConfigError(
            "Databricks host, HTTP path, and token are required "
            "(DATABRICKS_HOST / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN)",
        )
    connection = databricks_sql.connect(
        server_hostname=str(host),
        http_path=str(http_path),
        access_token=str(token),
        catalog=str(catalog) if catalog else None,
        schema=str(schema) if schema else None,
    )
    return DatabricksAdapter(
        connection,
        catalog=str(catalog) if catalog else None,
        schema=str(schema) if schema else None,
        host=str(host),
        http_path=str(http_path),
    )
