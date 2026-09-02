from __future__ import annotations

from typing import Any

from frontier.config import ConfigError, redact
from frontier.warehouse import (
    env_value,
    quote_identifier,
    split_relation_parts,
    sql_string,
)


class BigQueryAdapter:
    warehouse_type = "bigquery"
    dialect = "bigquery"

    def __init__(self, client: Any | None = None, *, project: str | None = None, dataset: str | None = None):
        self._client = client
        self.project = project
        self.dataset = dataset

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, "`")

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        client = self._require_client()
        rows = list(client.query(sql).result())
        return [tuple(row.values()) for row in rows]

    def relation_exists(self, relation: str) -> bool:
        catalog, schema, table = split_relation_parts(relation)
        project = catalog or self.project
        dataset = schema or self.dataset
        if not project or not dataset:
            raise ConfigError("BigQuery relation_exists needs project and dataset")
        sql = (
            f"select 1 from `{project}.{dataset}.INFORMATION_SCHEMA.TABLES` "
            f"where lower(table_name) = lower({sql_string(table)}) "
            "limit 1"
        )
        return bool(self.execute(sql))

    def estimate_query_cost(self, sql: str) -> dict[str, Any]:
        client = self._require_client()
        job_config = _dry_run_config()
        job = client.query(sql, job_config=job_config)
        bytes_processed = int(getattr(job, "total_bytes_processed", 0) or 0)
        return {
            "estimated": True,
            "warehouse_type": self.warehouse_type,
            "total_bytes_processed": bytes_processed,
        }

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        project = self.project
        if not project:
            return []
        tagged = sql_string(run_id)
        sql = (
            f"select job_id, state, total_bytes_processed "
            f"from `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` "
            f"where query like '%' || {tagged} || '%' "
            "order by creation_time desc limit 50"
        )
        try:
            rows = self.execute(sql)
        except Exception:
            return []
        return [
            {"query_id": row[0], "status": row[1], "bytes": row[2] if len(row) > 2 else None}
            for row in rows
        ]

    def close(self) -> None:
        client = self._client
        if client is not None and hasattr(client, "close"):
            client.close()
        self._client = None

    def describe(self) -> dict[str, Any]:
        return redact(
            {
                "warehouse_type": self.warehouse_type,
                "project": self.project,
                "dataset": self.dataset,
            }
        )

    def _require_client(self) -> Any:
        if self._client is None:
            raise ConfigError("bigquery adapter is not connected")
        return self._client


def _dry_run_config() -> Any:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)


def open_bigquery_adapter(settings: dict[str, Any]) -> BigQueryAdapter:
    try:
        from google.cloud import bigquery
    except ImportError as error:
        raise ConfigError(
            "Install frontier-runner[bigquery] to open a BigQuery session",
        ) from error
    project = env_value("BIGQUERY_PROJECT", "GOOGLE_CLOUD_PROJECT") or settings.get("project")
    dataset = (
        env_value("BIGQUERY_DATASET")
        or settings.get("dataset")
        or settings.get("schema")
    )
    if not project:
        raise ConfigError("BigQuery project is required (BIGQUERY_PROJECT or dbt profile)")
    client = bigquery.Client(project=str(project))
    return BigQueryAdapter(client, project=str(project), dataset=str(dataset) if dataset else None)
