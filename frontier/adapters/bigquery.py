from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from frontier.config import ConfigError, redact
from frontier.progress import elapsed_ms, env_int, failure_status, log_step
from frontier.warehouse import (
    env_value,
    quote_identifier,
    split_relation_parts,
    sql_string,
)


DEFAULT_QUERY_TIMEOUT_SECONDS = 300
DEFAULT_LOCATION = "US"


@dataclass
class BigQueryConnectConfig:
    project: str
    dataset: str
    location: str = DEFAULT_LOCATION
    keyfile: str | None = None

    def __repr__(self) -> str:
        return (
            "BigQueryConnectConfig("
            f"project={self.project!r}, dataset={self.dataset!r}, "
            f"location={self.location!r}, keyfile={'********' if self.keyfile else None})"
        )


class BigQueryAdapter:
    warehouse_type = "bigquery"
    dialect = "bigquery"

    def __init__(
        self,
        client: Any | None = None,
        *,
        project: str | None = None,
        dataset: str | None = None,
        location: str | None = None,
        config: BigQueryConnectConfig | None = None,
    ):
        self._client = client
        self._config = config
        self.project = project or (config.project if config else None)
        self.dataset = dataset or (config.dataset if config else None)
        self.location = (
            location
            or (config.location if config else None)
            or DEFAULT_LOCATION
        )
        self.last_query_id: str | None = None
        self.query_timeout_seconds = env_int(
            "FRONTIER_BIGQUERY_QUERY_TIMEOUT",
            DEFAULT_QUERY_TIMEOUT_SECONDS,
        )
        self._job_profiles: dict[str, dict[str, Any]] = {}

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, "`")

    def execute(self, sql: str) -> list[tuple[Any, ...]]:
        client = self._require_client()
        started = time.perf_counter()
        job = None
        try:
            job_config = _query_job_config()
        except ImportError:
            job_config = None
        try:
            job = client.query(
                sql,
                job_config=job_config,
                location=self.location,
            )
            result = job.result(timeout=self.query_timeout_seconds)
        except Exception as error:
            self._record_job(job, started, error=error)
            raise
        self._record_job(job, started)
        rows: list[tuple[Any, ...]] = []
        for row in result:
            if hasattr(row, "values"):
                rows.append(tuple(row.values()))
            else:
                rows.append(tuple(row))
        return rows

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
        job = client.query(
            sql,
            job_config=_dry_run_config(),
            location=self.location,
        )
        bytes_processed = int(getattr(job, "total_bytes_processed", 0) or 0)
        return {
            "estimated": True,
            "warehouse_type": self.warehouse_type,
            "total_bytes_processed": bytes_processed,
            "bytes_scanned": bytes_processed,
            "location": self.location,
        }

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        project = self.project
        if not project:
            return []
        tagged = sql_string(run_id)
        region = _jobs_region(self.location)
        sql = (
            "select job_id, state, total_bytes_processed "
            f"from `{project}.{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` "
            f"where query like concat('%', {tagged}, '%') "
            "order by creation_time desc limit 50"
        )
        try:
            rows = self.execute(sql)
        except Exception:
            return []
        return [
            {
                "query_id": row[0],
                "status": row[1],
                "total_bytes_processed": row[2] if len(row) > 2 else None,
            }
            for row in rows
        ]

    def get_query_profile(self, query_id: str) -> dict[str, Any]:
        token = (query_id or "").strip()
        if not token:
            return {}
        cached = self._job_profiles.get(token)
        if cached:
            return dict(cached)
        project = self.project
        if not project:
            return {}
        region = _jobs_region(self.location)
        sql = (
            "select job_id, total_bytes_processed, total_bytes_billed, "
            "total_slot_ms, timestamp_diff(end_time, start_time, MILLISECOND) "
            f"from `{project}.{region}.INFORMATION_SCHEMA.JOBS_BY_PROJECT` "
            f"where job_id = {sql_string(token)} "
            "order by creation_time desc limit 1"
        )
        try:
            rows = self.execute(sql)
        except Exception:
            return {}
        if not rows:
            return {}
        row = rows[0]
        bytes_processed = row[1] if len(row) > 1 else None
        return {
            "query_id": row[0],
            "job_id": row[0],
            "bytes_scanned": bytes_processed,
            "total_bytes_processed": bytes_processed,
            "total_bytes_billed": row[2] if len(row) > 2 else None,
            "slot_millis": row[3] if len(row) > 3 else None,
            "elapsed_ms": row[4] if len(row) > 4 else None,
            "location": self.location,
        }

    def close(self) -> None:
        client = self._client
        if client is not None and hasattr(client, "close"):
            client.close()
        self._client = None

    def describe(self) -> dict[str, Any]:
        payload = {
            "warehouse_type": self.warehouse_type,
            "project": self.project,
            "dataset": self.dataset,
            "location": self.location,
        }
        if self._config is not None:
            payload["keyfile"] = self._config.keyfile
        return redact(payload)

    def _require_client(self) -> Any:
        if self._client is None:
            raise ConfigError("bigquery adapter is not connected")
        return self._client

    def _record_job(
        self,
        job: Any | None,
        started: float,
        *,
        error: Exception | None = None,
    ) -> None:
        job_id = str(getattr(job, "job_id", "") or "") or None
        self.last_query_id = job_id
        if not job_id:
            return
        bytes_processed = getattr(job, "total_bytes_processed", None)
        profile = {
            "query_id": job_id,
            "job_id": job_id,
            "location": getattr(job, "location", None) or self.location,
            "bytes_scanned": bytes_processed,
            "total_bytes_processed": bytes_processed,
            "total_bytes_billed": getattr(job, "total_bytes_billed", None),
            "slot_millis": getattr(job, "slot_millis", None),
            "cache_hit": getattr(job, "cache_hit", None),
            "elapsed_ms": elapsed_ms(started),
            "state": getattr(job, "state", None),
            "error": type(error).__name__ if error is not None else None,
        }
        self._job_profiles[job_id] = profile
        if error is not None:
            log_step(
                "BigQuery job failed",
                duration_ms=elapsed_ms(started),
                status=failure_status(error),
            )


def _jobs_region(location: str | None) -> str:
    value = (location or DEFAULT_LOCATION).strip().lower().replace("_", "-")
    if value.startswith("region-"):
        return value
    return f"region-{value}"


def _query_job_config() -> Any:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(use_query_cache=False)


def _dry_run_config() -> Any:
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)


def load_bigquery_config(settings: dict[str, Any]) -> BigQueryConnectConfig:
    project = (
        env_value("BIGQUERY_PROJECT", "GOOGLE_CLOUD_PROJECT")
        or settings.get("project")
        or settings.get("database")
    )
    dataset = (
        env_value("BIGQUERY_DATASET")
        or settings.get("dataset")
        or settings.get("schema")
    )
    location = (
        env_value("BIGQUERY_LOCATION")
        or settings.get("location")
        or DEFAULT_LOCATION
    )
    if not project:
        raise ConfigError("BigQuery project is required (BIGQUERY_PROJECT or dbt profile)")
    if not dataset:
        raise ConfigError("BigQuery dataset is required (BIGQUERY_DATASET or dbt profile)")
    keyfile = env_value("GOOGLE_APPLICATION_CREDENTIALS") or settings.get("keyfile")
    return BigQueryConnectConfig(
        project=str(project),
        dataset=str(dataset),
        location=str(location or DEFAULT_LOCATION),
        keyfile=str(keyfile) if keyfile else None,
    )


def open_bigquery_adapter(settings: dict[str, Any]) -> BigQueryAdapter:
    config = load_bigquery_config(settings)
    started = time.perf_counter()
    log_step("BigQuery connector import started")
    try:
        from google.cloud import bigquery
    except ImportError as error:
        log_step(
            "BigQuery connector import completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise ConfigError(
            "Install frontier-runner[bigquery] to open a BigQuery session",
        ) from error
    log_step(
        "BigQuery connector import completed",
        duration_ms=elapsed_ms(started),
        status="ok",
    )
    started = time.perf_counter()
    log_step("BigQuery connection started")
    try:
        client_kwargs: dict[str, Any] = {
            "project": config.project,
            "location": config.location,
        }
        if config.keyfile:
            from google.oauth2 import service_account

            client_kwargs["credentials"] = service_account.Credentials.from_service_account_file(
                config.keyfile,
            )
        client = bigquery.Client(**client_kwargs)
    except Exception as error:
        log_step(
            "BigQuery connected",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step("BigQuery connected", duration_ms=elapsed_ms(started), status="ok")
    return BigQueryAdapter(
        client,
        project=config.project,
        dataset=config.dataset,
        location=config.location,
        config=config,
    )
