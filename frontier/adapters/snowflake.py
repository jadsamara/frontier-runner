from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.progress import elapsed_ms, env_int, failure_status, log_step
from frontier.warehouse import (
    env_value,
    load_dbt_profile_output,
    quote_identifier,
    sql_string,
)


DEFAULT_LOGIN_TIMEOUT_SECONDS = 30
DEFAULT_NETWORK_TIMEOUT_SECONDS = 60
DEFAULT_QUERY_TIMEOUT_SECONDS = 300


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
        login_timeout = env_int("FRONTIER_SNOWFLAKE_LOGIN_TIMEOUT", DEFAULT_LOGIN_TIMEOUT_SECONDS)
        network_timeout = env_int(
            "FRONTIER_SNOWFLAKE_NETWORK_TIMEOUT",
            DEFAULT_NETWORK_TIMEOUT_SECONDS,
        )
        query_timeout = env_int(
            "FRONTIER_SNOWFLAKE_QUERY_TIMEOUT",
            DEFAULT_QUERY_TIMEOUT_SECONDS,
        )
        kwargs: dict[str, Any] = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "schema": self.schema,
            "login_timeout": login_timeout,
            "network_timeout": network_timeout,
            "socket_timeout": network_timeout,
            "session_parameters": {
                "STATEMENT_TIMEOUT_IN_SECONDS": query_timeout,
            },
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

    def get_query_profile(self, query_id: str) -> dict[str, Any]:
        """Local QUERY_HISTORY stats. Never send query text or rows to SaaS."""
        token = (query_id or "").strip()
        if not token:
            return {}
        try:
            rows = self.execute(
                "select query_id, bytes_scanned, rows_produced, total_elapsed_time, "
                "credits_used_cloud_services "
                "from table(information_schema.query_history()) "
                f"where query_id = {sql_string(token)} "
                "order by start_time desc limit 1"
            )
        except Exception:
            return {}
        if not rows:
            return {}
        row = rows[0]
        elapsed = row[3] if len(row) > 3 else None
        return {
            "query_id": row[0],
            "bytes_scanned": row[1] if len(row) > 1 else None,
            "rows_produced": row[2] if len(row) > 2 else None,
            "elapsed_ms": elapsed,
            "total_elapsed_ms": elapsed,
            "cloud_services_credits": float(row[4]) if len(row) > 4 and row[4] is not None else None,
        }

    def capture_snapshot(self, relations: list[str] | tuple[str, ...], **kwargs: Any) -> Any:
        from frontier.snapshot import capture_from_catalog, utc_now_iso

        identifier = self._current_timestamp_literal()
        catalog = self._inventory_relations(relations)
        return capture_from_catalog(
            relations,
            catalog=catalog,
            identifier=identifier,
            captured_at=utc_now_iso(),
            attestation_source=kwargs.get("attestation_source"),
        )

    def bind_query_to_snapshot(self, sql: str, snapshot: Any) -> str:
        from frontier.snapshot import bind_sql_to_snapshot

        return bind_sql_to_snapshot(sql, snapshot, dialect=self.dialect)

    def verify_snapshot_binding(self, sql: str, snapshot: Any) -> bool:
        from frontier.snapshot import verify_snapshot_binding as verify

        return verify(sql, snapshot, dialect=self.dialect)

    def _current_timestamp_literal(self) -> str:
        rows = self.execute(
            "select to_varchar(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM')"
        )
        if not rows or rows[0][0] is None:
            from frontier.snapshot import SnapshotError, SOURCE_SNAPSHOT_NOT_PINNED

            raise SnapshotError(SOURCE_SNAPSHOT_NOT_PINNED, "Snowflake did not return a snapshot timestamp")
        return str(rows[0][0]).strip()

    def _inventory_relations(self, relations: list[str] | tuple[str, ...]) -> dict[str, dict[str, Any]]:
        from frontier.snapshot import DYNAMIC_TABLE, EXTERNAL_TABLE, MATERIALIZED_VIEW, VIEW
        from frontier.warehouse import split_relation_parts

        catalog: dict[str, dict[str, Any]] = {}
        pending = [str(item).strip() for item in relations if str(item).strip()]
        seen: set[str] = set()
        while pending:
            name = pending.pop(0)
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            database, schema, table = split_relation_parts(name)
            if not table:
                continue
            entry = self._lookup_table(database, schema, table)
            if entry is None:
                continue
            kind = str(entry.get("kind") or "")
            if kind == VIEW:
                definition = self._view_definition(database, schema, table)
                if definition:
                    entry["view_sql"] = definition
                    from frontier.snapshot import collect_source_relations

                    pending.extend(collect_source_relations(definition, dialect="snowflake"))
            elif kind in {MATERIALIZED_VIEW, EXTERNAL_TABLE, DYNAMIC_TABLE}:
                entry["kind"] = kind
            catalog[name] = entry
            dynamic = self._is_dynamic_table(database, schema, table)
            if dynamic:
                catalog[name]["kind"] = DYNAMIC_TABLE
        return catalog

    def _lookup_table(
        self,
        database: str | None,
        schema: str | None,
        table: str,
    ) -> dict[str, Any] | None:
        sql = (
            "select table_catalog, table_schema, table_name, table_type, "
            "is_transient, retention_time "
            "from information_schema.tables "
            f"where lower(table_name) = lower({sql_string(table)})"
        )
        if schema:
            sql += f" and lower(table_schema) = lower({sql_string(schema)})"
        if database:
            sql += f" and lower(table_catalog) = lower({sql_string(database)})"
        sql += " limit 1"
        try:
            rows = self.execute(sql)
        except Exception:
            return None
        if not rows:
            return None
        row = rows[0]
        table_type = str(row[3] or "").strip().upper()
        is_transient = str(row[4] or "").strip().upper() == "YES"
        retention = row[5] if len(row) > 5 else None
        kind = table_type.lower()
        if table_type == "BASE TABLE" and is_transient:
            kind = "transient"
        elif table_type == "BASE TABLE":
            kind = "permanent_table"
        return {
            "kind": kind,
            "retention_days": int(retention) if retention is not None else None,
        }

    def _view_definition(
        self,
        database: str | None,
        schema: str | None,
        table: str,
    ) -> str | None:
        sql = (
            "select view_definition from information_schema.views "
            f"where lower(table_name) = lower({sql_string(table)})"
        )
        if schema:
            sql += f" and lower(table_schema) = lower({sql_string(schema)})"
        if database:
            sql += f" and lower(table_catalog) = lower({sql_string(database)})"
        sql += " limit 1"
        try:
            rows = self.execute(sql)
        except Exception:
            return None
        if not rows or rows[0][0] is None:
            return None
        text = str(rows[0][0]).strip().rstrip(";")
        if text.lower().startswith("create "):
            lowered = text.lower()
            marker = " as "
            index = lowered.find(marker)
            if index != -1:
                text = text[index + len(marker) :].strip()
        return text or None

    def _is_dynamic_table(
        self,
        database: str | None,
        schema: str | None,
        table: str,
    ) -> bool:
        sql = (
            "select 1 from information_schema.dynamic_tables "
            f"where lower(table_name) = lower({sql_string(table)})"
        )
        if schema:
            sql += f" and lower(table_schema) = lower({sql_string(schema)})"
        if database:
            sql += f" and lower(table_catalog) = lower({sql_string(database)})"
        sql += " limit 1"
        try:
            return bool(self.execute(sql))
        except Exception:
            return False

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
    started = time.perf_counter()
    log_step("Snowflake connector import started")
    try:
        import snowflake.connector
    except ImportError as error:
        log_step(
            "Snowflake connector import completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise ConfigError(
            "Install frontier-runner[snowflake] to open a Snowflake session",
        ) from error
    log_step(
        "Snowflake connector import completed",
        duration_ms=elapsed_ms(started),
        status="ok",
    )
    started = time.perf_counter()
    log_step("Snowflake connection started")
    try:
        connection = snowflake.connector.connect(**config.connect_kwargs())
    except Exception as error:
        log_step(
            "Snowflake connected",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step("Snowflake connected", duration_ms=elapsed_ms(started), status="ok")
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
