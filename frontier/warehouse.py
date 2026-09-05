from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Protocol

import yaml

from frontier.config import ConfigError, redact

DEFAULT_PROFILES_PATH = Path.home() / ".dbt" / "profiles.yml"

WAREHOUSE_TYPES = (
    "snowflake",
    "bigquery",
    "databricks",
    "postgres",
    "redshift",
)

_TYPE_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "databricks-sql": "databricks",
    "databricks_sql": "databricks",
}


class WarehouseAdapter(Protocol):
    """Neutral warehouse execution interface.

    Core frontier logic talks only to this protocol. Snowflake (and every
    other vendor) is an adapter behind it.
    """

    warehouse_type: str
    dialect: str
    last_query_id: str | None

    def quote_identifier(self, value: str) -> str: ...

    def execute(self, sql: str) -> list[tuple[Any, ...]]: ...

    def relation_exists(self, relation: str) -> bool: ...

    def estimate_query_cost(self, sql: str) -> dict[str, Any]: ...

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]: ...

    def get_query_profile(self, query_id: str) -> dict[str, Any]: ...

    def close(self) -> None: ...


def normalize_warehouse_type(value: str | None) -> str:
    kind = (value or "snowflake").strip().lower().replace(" ", "-")
    kind = _TYPE_ALIASES.get(kind, kind)
    if kind not in WAREHOUSE_TYPES:
        supported = ", ".join(WAREHOUSE_TYPES)
        raise ConfigError(f"Unsupported warehouse type '{value}'. Supported: {supported}")
    return kind


def quote_identifier(value: str, quote_char: str = '"') -> str:
    parts = split_relation_parts(value)
    return ".".join(_quote_part(part, quote_char) for part in parts if part)


def _quote_part(part: str, quote_char: str) -> str:
    escaped = part.replace(quote_char, quote_char * 2)
    return f"{quote_char}{escaped}{quote_char}"


def split_relation_parts(relation: str) -> tuple[str | None, str | None, str]:
    stripped = relation.strip()
    parts: list[str] = []
    current: list[str] = []
    quote: str | None = None
    for char in stripped:
        if quote:
            if char == quote:
                quote = None
            else:
                current.append(char)
            continue
        if char in {'"', "`"}:
            quote = char
            continue
        if char == ".":
            parts.append("".join(current))
            current = []
            continue
        current.append(char)
    parts.append("".join(current))
    cleaned = [part.strip() for part in parts if part.strip()]
    if not cleaned:
        raise ConfigError(f"Invalid relation name: {relation}")
    if len(cleaned) == 1:
        return None, None, cleaned[0]
    if len(cleaned) == 2:
        return None, cleaned[0], cleaned[1]
    return cleaned[-3], cleaned[-2], cleaned[-1]


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_literal(value: str) -> str:
    stripped = value.strip()
    if re.fullmatch(r"-?\d+", stripped):
        return stripped
    return sql_string(stripped)


def env_value(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and value.strip():
            return value.strip()
    return None


class FakeWarehouse:
    """In-memory warehouse for unit tests. Never opens a vendor client."""

    warehouse_type = "snowflake"
    dialect = "snowflake"

    def __init__(
        self,
        responses: dict[str, list[tuple[Any, ...]]] | None = None,
        *,
        warehouse_type: str = "snowflake",
        dialect: str | None = None,
        relations: set[str] | None = None,
    ):
        self.responses = responses or {}
        self.executed: list[str] = []
        self.warehouse_type = normalize_warehouse_type(warehouse_type)
        self.dialect = dialect or self.warehouse_type
        self.relations = relations
        self.last_query_id: str | None = None
        self.query_ids: list[str] = []

    def quote_identifier(self, value: str) -> str:
        quote = "`" if self.dialect in {"bigquery", "databricks"} else '"'
        return quote_identifier(value, quote)

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        self.executed.append(sql)
        self.last_query_id = f"fake-qid-{len(self.executed)}"
        self.query_ids.append(self.last_query_id)
        normalized = " ".join(sql.lower().split())
        if normalized.startswith(("create ", "drop ", "insert ", "delete ", "merge ", "create or replace")):
            return []
        if "as frontier_origin_keys" in normalized:
            matched = self._match_response(normalized)
            if matched is not None:
                return matched
            return self._synthetic_origin_keys()
        if "as frontier_origin_counts" in normalized:
            matched = self._match_response(normalized)
            if matched is not None:
                return matched
            keys = self._synthetic_origin_keys()
            events = {value for value, origin in keys if origin == "event"}
            sql_change = {value for value, origin in keys if origin == "sql_change"}
            union = {value for value, _origin in keys}
            return [(len(events), len(sql_change), len(union))]
        if "as frontier_cdc_full_count" in normalized:
            matched = self._match_response(normalized)
            if matched is not None:
                return matched
            return [(150_000,)]
        matched = self._match_response(normalized)
        if matched is not None:
            return matched
        raise ConfigError(f"FakeWarehouse has no response for SQL: {sql}")

    def _match_response(self, normalized: str) -> list[tuple[Any, ...]] | None:
        for needle, rows in self.responses.items():
            token = needle.lower()
            index = normalized.find(token)
            if index == -1:
                continue
            end = index + len(token)
            if end < len(normalized) and normalized[end].isdigit():
                continue
            return rows
        return None

    def _synthetic_origin_keys(self) -> list[tuple[Any, ...]]:
        create = next(
            (item for item in reversed(self.executed) if item.lower().lstrip().startswith("create or replace table")),
            "",
        )
        keys: list[tuple[Any, ...]] = []
        seen: set[tuple[str, str]] = set()
        for match in re.finditer(
            r"select\s+(-?\d+|'[^']*')\s+as\s+[A-Za-z_][A-Za-z0-9_]*\s*,\s*'(event|sql_change)'\s+as origin",
            create,
            flags=re.IGNORECASE,
        ):
            raw = match.group(1)
            value = raw[1:-1] if raw.startswith("'") else raw
            origin = match.group(2).lower()
            item = (value, origin)
            if item not in seen:
                seen.add(item)
                keys.append(item)
        normalized_create = " ".join(create.lower().split())
        if "as frontier_sql_change_keys" in normalized_create:
            for needle, rows in self.responses.items():
                token = needle.lower()
                if token in {"frontier_origin_keys", "frontier_origin_counts", "full_entity_count"}:
                    continue
                if token not in normalized_create:
                    continue
                for row in rows:
                    if not row or row[0] is None:
                        continue
                    origin = str(row[1]) if len(row) > 1 and row[1] is not None else "sql_change"
                    item = (str(row[0]), origin)
                    if item not in seen:
                        seen.add(item)
                        keys.append(item)
        return keys

    def get_query_profile(self, query_id: str) -> dict[str, Any]:
        sql = ""
        for qid, executed in zip(self.query_ids, self.executed):
            if qid == query_id:
                sql = executed
                break
        lowered = sql.lower()
        targeted = "frontier_keys" in lowered or "affected_keys" in lowered
        if targeted:
            return {
                "query_id": query_id,
                "bytes_scanned": 1_000,
                "partitions_scanned": 1,
                "rows_produced": 3,
            }
        return {
            "query_id": query_id,
            "bytes_scanned": 1_000_000,
            "partitions_scanned": 80,
            "rows_produced": 150_000,
        }

    def relation_exists(self, relation: str) -> bool:
        if self.relations is None:
            return True
        return relation in self.relations or split_relation_parts(relation)[2] in self.relations

    def estimate_query_cost(self, sql: str) -> dict[str, Any]:
        return {
            "estimated": False,
            "warehouse_type": self.warehouse_type,
            "reason": "fake",
            "sql_chars": len(sql),
        }

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        return []

    def close(self) -> None:
        return None

    def describe(self) -> dict[str, Any]:
        return {"warehouse_type": self.warehouse_type, "mode": "fake"}


def load_dbt_profile_output(
    project_dir: Path,
    *,
    profiles_path: Path | None = None,
    target: str | None = None,
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
    return output


def connect_warehouse(
    project_dir: Path,
    *,
    profiles_path: Path | None = None,
    target: str | None = None,
) -> WarehouseAdapter:
    """Open the adapter matching the dbt profile (or FRONTIER_WAREHOUSE_TYPE)."""
    output: dict[str, Any] = {}
    try:
        output = load_dbt_profile_output(
            project_dir,
            profiles_path=profiles_path,
            target=target,
        )
    except ConfigError:
        if not env_value(
            "FRONTIER_WAREHOUSE_TYPE",
            "SNOWFLAKE_ACCOUNT",
            "BIGQUERY_PROJECT",
            "DATABRICKS_HOST",
            "DATABRICKS_SERVER_HOSTNAME",
            "FRONTIER_POSTGRES_HOST",
            "REDSHIFT_HOST",
        ):
            raise
    kind = env_value("FRONTIER_WAREHOUSE_TYPE") or str(output.get("type") or "")
    if not kind:
        if env_value("SNOWFLAKE_ACCOUNT"):
            kind = "snowflake"
        elif env_value("BIGQUERY_PROJECT", "GOOGLE_CLOUD_PROJECT"):
            kind = "bigquery"
        elif env_value("DATABRICKS_HOST", "DATABRICKS_SERVER_HOSTNAME"):
            kind = "databricks"
        elif env_value("FRONTIER_POSTGRES_HOST", "PGHOST"):
            kind = "postgres"
        elif env_value("REDSHIFT_HOST"):
            kind = "redshift"
        else:
            kind = "snowflake"
    return open_adapter(kind, output, project_dir=project_dir)


def open_adapter(
    warehouse_type: str,
    settings: dict[str, Any] | None = None,
    *,
    project_dir: Path | None = None,
) -> WarehouseAdapter:
    kind = normalize_warehouse_type(warehouse_type)
    payload = settings or {}
    if kind == "snowflake":
        from frontier.adapters.snowflake import open_snowflake_adapter

        return open_snowflake_adapter(payload, project_dir=project_dir)
    if kind == "bigquery":
        from frontier.adapters.bigquery import open_bigquery_adapter

        return open_bigquery_adapter(payload)
    if kind == "databricks":
        from frontier.adapters.databricks import open_databricks_adapter

        return open_databricks_adapter(payload)
    if kind == "postgres":
        from frontier.adapters.postgres import open_postgres_adapter

        return open_postgres_adapter(payload)
    from frontier.adapters.redshift import open_redshift_adapter

    return open_redshift_adapter(payload)


def describe_adapter(adapter: WarehouseAdapter) -> dict[str, Any]:
    describe = getattr(adapter, "describe", None)
    if callable(describe):
        return redact(describe())
    return {"warehouse_type": adapter.warehouse_type}
