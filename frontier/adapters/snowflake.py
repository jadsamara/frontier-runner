from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.warehouse import (
    env_value,
    load_dbt_profile_output,
    quote_identifier,
    sql_string,
)


@dataclass
class SnowflakeConnectConfig:
    account: str
    user: str
    database: str
    schema: str
    warehouse: str | None = None
    role: str | None = None
    password: str | None = None
    authenticator: str | None = None

    def __repr__(self) -> str:
        return (
            "SnowflakeConnectConfig("
            f"account={self.account!r}, user={self.user!r}, "
            f"database={self.database!r}, schema={self.schema!r}, "
            f"warehouse={self.warehouse!r}, role={self.role!r}, "
            "password='********')"
        )

    def connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "schema": self.schema,
        }
        if self.warehouse:
            kwargs["warehouse"] = self.warehouse
        if self.role:
            kwargs["role"] = self.role
        if self.authenticator:
            kwargs["authenticator"] = self.authenticator
        if self.password:
            kwargs["password"] = self.password
        return kwargs


class SnowflakeAdapter(CursorAdapter):
    warehouse_type = "snowflake"
    dialect = "snowflake"

    def __init__(self, connection: Any | None = None, *, config: SnowflakeConnectConfig | None = None):
        self._connection = connection
        self._config = config

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, '"')

    def estimate_query_cost(self, sql: str) -> dict[str, Any]:
        try:
            rows = self.execute(f"explain using json {sql}")
        except Exception:
            return {"estimated": False, "warehouse_type": self.warehouse_type}
        return {
            "estimated": True,
            "warehouse_type": self.warehouse_type,
            "plan_rows": len(rows),
        }

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        tagged = sql_string(run_id)
        try:
            rows = self.execute(
                "select query_id, query_type, execution_status, rows_produced "
                "from table(information_schema.query_history()) "
                f"where query_text ilike '%' || {tagged} || '%' "
                "order by start_time desc limit 50"
            )
        except Exception:
            return []
        return [
            {
                "query_id": row[0],
                "query_type": row[1],
                "status": row[2],
                "rows": row[3] if len(row) > 3 else None,
            }
            for row in rows
        ]

    def describe(self) -> dict[str, Any]:
        if self._config is None:
            return {"warehouse_type": self.warehouse_type}
        return describe_connection(self._config)


def load_snowflake_config(
    project_dir: Path,
    *,
    profiles_path: Path | None = None,
    target: str | None = None,
) -> SnowflakeConnectConfig:
    """Load Snowflake settings from env, falling back to the dbt profile."""
    env_account = env_value("SNOWFLAKE_ACCOUNT")
    env_user = env_value("SNOWFLAKE_USER")
    env_database = env_value("SNOWFLAKE_DATABASE")
    env_schema = env_value("SNOWFLAKE_SCHEMA")

    profile_output: dict[str, Any] = {}
    if not all([env_account, env_user, env_database, env_schema]):
        profile_output = load_dbt_profile_output(
            project_dir,
            profiles_path=profiles_path,
            target=target,
        )
        profile_type = str(profile_output.get("type") or "").lower()
        if profile_type and profile_type != "snowflake":
            raise ConfigError(
                f"dbt profile type is '{profile_type}', not snowflake",
            )

    account = env_account or str(profile_output.get("account") or "")
    user = env_user or str(profile_output.get("user") or "")
    database = env_database or str(profile_output.get("database") or "")
    schema = env_schema or str(profile_output.get("schema") or "")
    if not all([account, user, database, schema]):
        raise ConfigError(
            "Snowflake account, user, database, and schema are required "
            "(env SNOWFLAKE_* or dbt profiles.yml)",
        )

    password = env_value("SNOWFLAKE_PASSWORD") or profile_output.get("password")
    return SnowflakeConnectConfig(
        account=account,
        user=user,
        database=database,
        schema=schema,
        warehouse=env_value("SNOWFLAKE_WAREHOUSE") or profile_output.get("warehouse"),
        role=env_value("SNOWFLAKE_ROLE") or profile_output.get("role"),
        password=str(password) if password else None,
        authenticator=env_value("SNOWFLAKE_AUTHENTICATOR") or profile_output.get("authenticator"),
    )


def open_warehouse(config: SnowflakeConnectConfig) -> SnowflakeAdapter:
    try:
        import snowflake.connector
    except ImportError as error:
        raise ConfigError(
            "Install frontier-runner[snowflake] to open a Snowflake session",
        ) from error
    connection = snowflake.connector.connect(**config.connect_kwargs())
    return SnowflakeAdapter(connection, config=config)


def open_snowflake_adapter(
    settings: dict[str, Any],
    *,
    project_dir: Path | None = None,
) -> SnowflakeAdapter:
    account = env_value("SNOWFLAKE_ACCOUNT") or settings.get("account")
    user = env_value("SNOWFLAKE_USER") or settings.get("user")
    database = env_value("SNOWFLAKE_DATABASE") or settings.get("database")
    schema = env_value("SNOWFLAKE_SCHEMA") or settings.get("schema")
    if not all([account, user, database, schema]) and project_dir is not None:
        return open_warehouse(load_snowflake_config(project_dir))
    if not all([account, user, database, schema]):
        raise ConfigError("Snowflake account, user, database, and schema are required")
    password = env_value("SNOWFLAKE_PASSWORD") or settings.get("password")
    config = SnowflakeConnectConfig(
        account=str(account),
        user=str(user),
        database=str(database),
        schema=str(schema),
        warehouse=env_value("SNOWFLAKE_WAREHOUSE") or settings.get("warehouse"),
        role=env_value("SNOWFLAKE_ROLE") or settings.get("role"),
        password=str(password) if password else None,
        authenticator=env_value("SNOWFLAKE_AUTHENTICATOR") or settings.get("authenticator"),
    )
    return open_warehouse(config)


def describe_connection(config: SnowflakeConnectConfig) -> dict[str, Any]:
    return redact(
        {
            "warehouse_type": "snowflake",
            "account": config.account,
            "user": config.user,
            "database": config.database,
            "schema": config.schema,
            "warehouse": config.warehouse,
            "role": config.role,
            "authenticator": config.authenticator,
            "password": config.password,
        }
    )


# Historical alias used by older runner imports.
SnowflakeWarehouse = SnowflakeAdapter
