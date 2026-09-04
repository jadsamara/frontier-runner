from __future__ import annotations

from typing import Any

from frontier.config import ConfigError
from frontier.warehouse import (
    sql_string,
    split_relation_parts,
)


class CursorAdapter:
    """Shared execute/close for DB-API style connections."""

    warehouse_type: str
    dialect: str
    _connection: Any = None
    last_query_id: str | None = None

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            self.last_query_id = getattr(cursor, "sfqid", None) or getattr(cursor, "sfqId", None)
            if self.last_query_id is not None:
                self.last_query_id = str(self.last_query_id)
            if cursor.description is None:
                return []
            rows = cursor.fetchall() or []
            return [tuple(row) for row in rows]
        finally:
            cursor.close()

    def get_query_profile(self, query_id: str) -> dict[str, Any]:
        del query_id
        return {}

    def close(self) -> None:
        connection = self._connection
        if connection is not None:
            connection.close()
            self._connection = None

    def _require_connection(self) -> Any:
        if self._connection is None:
            raise ConfigError(f"{self.warehouse_type} adapter is not connected")
        return self._connection

    def _table_lookup_sql(self, relation: str) -> str:
        catalog, schema, table = split_relation_parts(relation)
        sql = (
            "select 1 as present from information_schema.tables "
            f"where lower(table_name) = lower({sql_string(table)})"
        )
        if schema:
            sql += f" and lower(table_schema) = lower({sql_string(schema)})"
        if catalog:
            sql += f" and lower(table_catalog) = lower({sql_string(catalog)})"
        return sql + " limit 1"

    def relation_exists(self, relation: str) -> bool:
        return bool(self.execute(self._table_lookup_sql(relation)))

    def estimate_query_cost(self, sql: str) -> dict[str, Any]:
        try:
            rows = self.execute(f"explain {sql}")
        except Exception:
            return {"estimated": False, "warehouse_type": self.warehouse_type}
        return {
            "estimated": True,
            "warehouse_type": self.warehouse_type,
            "plan_rows": len(rows),
        }

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        return []
