from __future__ import annotations

from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.warehouse import env_value, quote_identifier, sql_string


class PostgresAdapter(CursorAdapter):
    warehouse_type = "postgres"
    dialect = "postgres"

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
                "select queryid::text, calls "
                "from pg_stat_statements "
                f"where query like {tagged} "
                "order by last_exec_at desc nulls last limit 50"
            )
        except Exception:
            return []
        return [{"query_id": row[0], "calls": row[1]} for row in rows]

    def describe(self) -> dict[str, Any]:
        return redact(
            {
                "warehouse_type": self.warehouse_type,
                "host": self.host,
                "database": self.database,
                "schema": self.schema,
                "user": self.user,
                "password": env_value("FRONTIER_POSTGRES_PASSWORD", "PGPASSWORD"),
            }
        )


def open_postgres_adapter(settings: dict[str, Any]) -> PostgresAdapter:
    try:
        import psycopg
    except ImportError as error:
        raise ConfigError(
            "Install frontier-runner[postgres] to open a PostgreSQL session",
        ) from error
    host = env_value("FRONTIER_POSTGRES_HOST", "PGHOST") or settings.get("host")
    user = env_value("FRONTIER_POSTGRES_USER", "PGUSER") or settings.get("user")
    database = (
        env_value("FRONTIER_POSTGRES_DB", "PGDATABASE")
        or settings.get("dbname")
        or settings.get("database")
    )
    port = env_value("FRONTIER_POSTGRES_PORT", "PGPORT") or settings.get("port") or 5432
    password = env_value("FRONTIER_POSTGRES_PASSWORD", "PGPASSWORD") or settings.get("password")
    schema = env_value("FRONTIER_POSTGRES_SCHEMA") or settings.get("schema") or "public"
    if not host or not user or not database:
        raise ConfigError(
            "PostgreSQL host, user, and database are required "
            "(FRONTIER_POSTGRES_* or dbt profile)",
        )
    connection = psycopg.connect(
        host=str(host),
        user=str(user),
        dbname=str(database),
        port=int(port),
        password=str(password) if password else None,
    )
    return PostgresAdapter(
        connection,
        host=str(host),
        database=str(database),
        schema=str(schema),
        user=str(user),
    )
