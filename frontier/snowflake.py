from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import yaml

from frontier.config import ConfigError, redact

DEFAULT_PROFILES_PATH = Path.home() / ".dbt" / "profiles.yml"


class Warehouse(Protocol):
    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        ...

    def close(self) -> None:
        ...


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


class FakeWarehouse:
    """In-memory warehouse for unit tests. Never talks to Snowflake."""

    def __init__(self, responses: dict[str, list[tuple[Any, ...]]] | None = None):
        self.responses = responses or {}
        self.executed: list[str] = []

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        normalized = " ".join(sql.lower().split())
        for needle, rows in self.responses.items():
            token = needle.lower()
            index = normalized.find(token)
            if index == -1:
                continue
            end = index + len(token)
            if end < len(normalized) and normalized[end].isdigit():
                continue
            return rows
        raise ConfigError(f"FakeWarehouse has no response for SQL: {sql}")

    def close(self) -> None:
        return None


class SnowflakeWarehouse:
    def __init__(self, connection: Any):
        self._connection = connection

    def fetch_all(self, sql: str) -> list[tuple[Any, ...]]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(sql)
            rows = cursor.fetchall() or []
            return [tuple(row) for row in rows]
        finally:
            cursor.close()

    def close(self) -> None:
        self._connection.close()


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return None
    return value


def _require(mapping: dict[str, Any], key: str, env_name: str) -> str:
    value = _env(env_name) or mapping.get(key)
    if value is None or str(value).strip() == "":
        raise ConfigError(f"Snowflake {key} is missing (set {env_name} or dbt profile)")
    return str(value)


def load_snowflake_config(
    project_dir: Path,
    *,
    profiles_path: Path | None = None,
    target: str | None = None,
) -> SnowflakeConnectConfig:
    """Load Snowflake settings from env, falling back to the dbt profile.

    The returned object redacts passwords in repr(). Callers must never print
    connect_kwargs() or the raw profile mapping.
    """
    env_account = _env("SNOWFLAKE_ACCOUNT")
    env_user = _env("SNOWFLAKE_USER")
    env_database = _env("SNOWFLAKE_DATABASE")
    env_schema = _env("SNOWFLAKE_SCHEMA")

    profile_output: dict[str, Any] = {}
    if not all([env_account, env_user, env_database, env_schema]):
        profile_output = _load_profile_output(
            project_dir,
            profiles_path=profiles_path,
            target=target,
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

    password = _env("SNOWFLAKE_PASSWORD") or profile_output.get("password")
    return SnowflakeConnectConfig(
        account=account,
        user=user,
        database=database,
        schema=schema,
        warehouse=_env("SNOWFLAKE_WAREHOUSE") or profile_output.get("warehouse"),
        role=_env("SNOWFLAKE_ROLE") or profile_output.get("role"),
        password=str(password) if password else None,
        authenticator=_env("SNOWFLAKE_AUTHENTICATOR") or profile_output.get("authenticator"),
    )


def _load_profile_output(
    project_dir: Path,
    *,
    profiles_path: Path | None,
    target: str | None,
) -> dict[str, Any]:
    dbt_project_path = project_dir / "dbt_project.yml"
    if not dbt_project_path.is_file():
        raise ConfigError(f"Missing dbt_project.yml in {project_dir}")
    dbt_project = yaml.safe_load(dbt_project_path.read_text()) or {}
    profile_name = dbt_project.get("profile")
    if not profile_name:
        raise ConfigError("dbt_project.yml is missing profile")

    if profiles_path is not None:
        path = profiles_path
    elif os.environ.get("DBT_PROFILES_DIR"):
        path = Path(os.environ["DBT_PROFILES_DIR"]) / "profiles.yml"
    else:
        path = DEFAULT_PROFILES_PATH

    if not path.is_file():
        raise ConfigError(f"Missing dbt profiles.yml at {path}")

    profiles = yaml.safe_load(path.read_text()) or {}
    if not isinstance(profiles, dict) or profile_name not in profiles:
        raise ConfigError(f"Profile '{profile_name}' not found in {path}")

    entry = profiles[profile_name]
    if not isinstance(entry, dict):
        raise ConfigError(f"Profile '{profile_name}' must be a mapping")
    target_name = target or entry.get("target") or "dev"
    outputs = entry.get("outputs") or {}
    output = outputs.get(target_name)
    if not isinstance(output, dict):
        raise ConfigError(f"Profile '{profile_name}' has no output '{target_name}'")
    if str(output.get("type") or "").lower() != "snowflake":
        raise ConfigError("Frontier runner only supports Snowflake dbt profiles")
    return output


def open_warehouse(config: SnowflakeConnectConfig) -> SnowflakeWarehouse:
    try:
        import snowflake.connector
    except ImportError as error:
        raise ConfigError(
            "snowflake-connector-python is required for `frontier run`",
        ) from error
    connection = snowflake.connector.connect(**config.connect_kwargs())
    return SnowflakeWarehouse(connection)


def describe_connection(config: SnowflakeConnectConfig) -> dict[str, Any]:
    """Safe summary for logs and inspect. Never includes the password."""
    return redact(
        {
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
