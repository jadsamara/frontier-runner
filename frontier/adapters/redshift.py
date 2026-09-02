from __future__ import annotations

from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.warehouse import env_value, quote_identifier, sql_string


class RedshiftAdapter(CursorAdapter):
    warehouse_type = "redshift"
    dialect = "redshift"

    def __init__(
        self,
        connection: Any | None = None,
        *,
        host: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        user: str | None = None,
    ):
        self._connection = connection
        self.host = host
        self.database = database
        self.schema = schema
        self.user = user

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, '"')

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        tagged = sql_string(f"%{run_id}%")
        try:
            rows = self.execute(
                "select query::text, status "
                "from stl_query "
                f"where querytxt like {tagged} "
                "order by starttime desc limit 50"
            )
        except Exception:
            return []
        return [{"query_id": row[0], "status": row[1]} for row in rows]

    def describe(self) -> dict[str, Any]:
        return redact(
            {
                "warehouse_type": self.warehouse_type,
                "host": self.host,
                "database": self.database,
                "schema": self.schema,
                "user": self.user,
                "password": env_value("REDSHIFT_PASSWORD"),
            }
        )


def open_redshift_adapter(settings: dict[str, Any]) -> RedshiftAdapter:
    try:
        import redshift_connector
    except ImportError as error:
        raise ConfigError(
            "Install frontier-runner[redshift] to open a Redshift session",
        ) from error
    host = env_value("REDSHIFT_HOST") or settings.get("host")
    user = env_value("REDSHIFT_USER") or settings.get("user")
    database = env_value("REDSHIFT_DATABASE") or settings.get("dbname") or settings.get("database")
    port = env_value("REDSHIFT_PORT") or settings.get("port") or 5439
    password = env_value("REDSHIFT_PASSWORD") or settings.get("password")
    schema = env_value("REDSHIFT_SCHEMA") or settings.get("schema") or "public"
    if not host or not user or not database or not password:
        raise ConfigError(
            "Redshift host, user, database, and password are required "
            "(REDSHIFT_* or dbt profile)",
        )
    connection = redshift_connector.connect(
        host=str(host),
        database=str(database),
        user=str(user),
        password=str(password),
        port=int(port),
    )
    return RedshiftAdapter(
        connection,
        host=str(host),
        database=str(database),
        schema=str(schema),
        user=str(user),
    )
