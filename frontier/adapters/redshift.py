from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from frontier.adapters.base import CursorAdapter
from frontier.config import ConfigError, redact
from frontier.progress import elapsed_ms, env_int, failure_status, log_step
from frontier.warehouse import env_value, quote_identifier, sql_string


DEFAULT_PORT = 5439
DEFAULT_QUERY_TIMEOUT_SECONDS = 300
DEFAULT_CONNECT_TIMEOUT_SECONDS = 30
_MAX_EXECUTE_ATTEMPTS = 3
_RETRYABLE_TOKENS = (
    "timeout",
    "timed out",
    "connection",
    "could not connect",
    "broken pipe",
    "eof",
    "serialization",
    "network",
    "temporarily unavailable",
    "connection reset",
)
_NON_RETRYABLE_TOKENS = (
    "permission",
    "access denied",
    "not authorized",
    "syntax",
    "undefined table",
    "does not exist",
    "invalid operation",
)


@dataclass
class RedshiftConnectConfig:
    host: str | None
    database: str
    user: str
    schema: str = "public"
    port: int = DEFAULT_PORT
    password: str | None = None
    ssl: bool = True
    method: str = "database"
    cluster_identifier: str | None = None
    region: str | None = None
    iam_profile: str | None = None
    serverless: bool = False

    def __repr__(self) -> str:
        return (
            "RedshiftConnectConfig("
            f"host={self.host!r}, database={self.database!r}, user={self.user!r}, "
            f"schema={self.schema!r}, port={self.port}, method={self.method!r}, "
            f"cluster_identifier={self.cluster_identifier!r}, region={self.region!r}, "
            f"iam_profile={self.iam_profile!r}, serverless={self.serverless}, "
            f"ssl={self.ssl}, password={'********' if self.password else None})"
        )

    def connect_kwargs(self) -> dict[str, Any]:
        timeout = env_int("FRONTIER_REDSHIFT_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT_SECONDS)
        kwargs: dict[str, Any] = {
            "database": self.database,
            "port": self.port,
            "timeout": timeout,
            "ssl": self.ssl,
        }
        if self.method == "iam":
            kwargs["iam"] = True
            kwargs["db_user"] = self.user
            if self.cluster_identifier:
                kwargs["cluster_identifier"] = self.cluster_identifier
            if self.host:
                kwargs["host"] = self.host
            if self.region:
                kwargs["region"] = self.region
            if self.iam_profile:
                kwargs["profile"] = self.iam_profile
            if self.serverless:
                kwargs["is_serverless"] = True
            return kwargs
        if not self.host:
            raise ConfigError("Redshift host is required for database authentication")
        if not self.password:
            raise ConfigError("Redshift password is required for database authentication")
        kwargs["host"] = self.host
        kwargs["user"] = self.user
        kwargs["password"] = self.password
        return kwargs


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
        config: RedshiftConnectConfig | None = None,
    ):
        self._connection = connection
        self._config = config
        self.host = host or (config.host if config else None)
        self.database = database or (config.database if config else None)
        self.schema = schema or (config.schema if config else None)
        self.user = user or (config.user if config else None)
        self.query_timeout_seconds = env_int(
            "FRONTIER_REDSHIFT_QUERY_TIMEOUT",
            DEFAULT_QUERY_TIMEOUT_SECONDS,
        )
        self._query_profiles: dict[str, dict[str, Any]] = {}

    def quote_identifier(self, value: str) -> str:
        return quote_identifier(value, '"')

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> list[tuple[Any, ...]]:
        last_error: Exception | None = None
        for attempt in range(1, _MAX_EXECUTE_ATTEMPTS + 1):
            started = time.perf_counter()
            try:
                rows = self._execute_once(sql, params=params)
                self._record_query(started)
                return rows
            except Exception as error:
                last_error = error
                self._record_query(started, error=error)
                if attempt >= _MAX_EXECUTE_ATTEMPTS or not _retryable(error):
                    raise
                time.sleep(min(0.25 * attempt, 1.0))
        assert last_error is not None
        raise last_error

    def get_query_history(self, run_id: str) -> list[dict[str, Any]]:
        tagged = f"%{run_id}%"
        sql = (
            "select q.query::varchar, "
            "case when q.aborted = 0 then 'completed' else 'aborted' end "
            "from stl_query q "
            "where q.querytxt like %s "
            "order by q.starttime desc limit 50"
        )
        try:
            rows = self._execute_once(sql, params=(tagged,), capture_id=False)
        except Exception:
            return []
        return [{"query_id": row[0], "status": row[1] if len(row) > 1 else None} for row in rows]

    def get_query_profile(self, query_id: str) -> dict[str, Any]:
        token = (query_id or "").strip()
        if not token:
            return {}
        cached = self._query_profiles.get(token)
        if cached and cached.get("metrics_available"):
            return dict(cached)
        profile = dict(cached) if cached else {"query_id": token, "metrics_available": False}
        try:
            query_num = int(token)
        except ValueError:
            profile.setdefault("status", "unavailable")
            profile.setdefault("bytes_scanned", None)
            return profile
        sql = (
            "select q.query::varchar, "
            "case when q.aborted = 0 then 'completed' else 'aborted' end, "
            "datediff(millisecond, q.starttime, q.endtime), "
            "w.total_queue_time / 1000, "
            "w.total_exec_time / 1000, "
            "s.bytes "
            "from stl_query q "
            "left join stl_wlm_query w on w.query = q.query "
            "left join ("
            "  select query, sum(bytes) as bytes from svl_query_summary group by query"
            ") s on s.query = q.query "
            "where q.query = %s "
            "order by q.starttime desc limit 1"
        )
        try:
            rows = self._execute_once(sql, params=(query_num,), capture_id=False)
        except Exception:
            profile["status"] = profile.get("status") or "unavailable"
            profile["bytes_scanned"] = None
            profile["metrics_available"] = False
            return profile
        if not rows:
            profile["status"] = profile.get("status") or "unavailable"
            profile["bytes_scanned"] = None
            profile["metrics_available"] = False
            return profile
        row = rows[0]
        bytes_scanned = row[5] if len(row) > 5 else None
        profile.update(
            {
                "query_id": row[0],
                "status": row[1],
                "elapsed_ms": row[2] if len(row) > 2 else profile.get("elapsed_ms"),
                "queue_ms": row[3] if len(row) > 3 else None,
                "execution_ms": row[4] if len(row) > 4 else None,
                "bytes_scanned": bytes_scanned,
                "total_bytes_processed": bytes_scanned,
                "metrics_available": bytes_scanned is not None,
            }
        )
        self._query_profiles[str(row[0])] = dict(profile)
        return profile

    def describe(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "warehouse_type": self.warehouse_type,
            "host": self.host,
            "database": self.database,
            "schema": self.schema,
            "user": self.user,
        }
        if self._config is not None:
            payload.update(
                {
                    "password": self._config.password,
                    "method": self._config.method,
                    "cluster_identifier": self._config.cluster_identifier,
                    "region": self._config.region,
                    "iam_profile": self._config.iam_profile,
                    "access_key_id": env_value("AWS_ACCESS_KEY_ID"),
                    "secret_access_key": env_value("AWS_SECRET_ACCESS_KEY"),
                    "session_token": env_value("AWS_SESSION_TOKEN"),
                }
            )
        else:
            payload["password"] = env_value("REDSHIFT_PASSWORD")
        return redact(payload)

    def _execute_once(
        self,
        sql: str,
        *,
        params: tuple[Any, ...] | None = None,
        capture_id: bool = True,
    ) -> list[tuple[Any, ...]]:
        connection = self._require_connection()
        cursor = connection.cursor()
        try:
            try:
                if params is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, params)
            except Exception:
                if capture_id:
                    self.last_query_id = _cursor_query_id(cursor) or _pg_last_query_id(connection)
                    if self.last_query_id is not None:
                        self.last_query_id = str(self.last_query_id)
                raise
            if capture_id:
                self.last_query_id = _cursor_query_id(cursor) or _pg_last_query_id(connection)
                if self.last_query_id is not None:
                    self.last_query_id = str(self.last_query_id)
            if cursor.description is None:
                return []
            rows = cursor.fetchall() or []
            return [tuple(row) for row in rows]
        finally:
            cursor.close()

    def _record_query(self, started: float, *, error: Exception | None = None) -> None:
        query_id = self.last_query_id
        if not query_id:
            if error is not None:
                log_step(
                    "Redshift query failed",
                    duration_ms=elapsed_ms(started),
                    status=failure_status(error),
                )
            return
        profile = {
            "query_id": query_id,
            "elapsed_ms": elapsed_ms(started),
            "status": "failed" if error is not None else "completed",
            "error": type(error).__name__ if error is not None else None,
            "metrics_available": False,
            "bytes_scanned": None,
        }
        self._query_profiles[query_id] = profile
        if error is not None:
            log_step(
                "Redshift query failed",
                duration_ms=elapsed_ms(started),
                status=failure_status(error),
            )


def _cursor_query_id(cursor: Any) -> str | None:
    for attr in ("query_id", "queryId"):
        value = getattr(cursor, attr, None)
        if value:
            return str(value)
    return None


def _pg_last_query_id(connection: Any) -> str | None:
    cursor = connection.cursor()
    try:
        cursor.execute("select pg_last_query_id()")
        row = cursor.fetchone()
        if row and row[0] is not None:
            return str(row[0])
    except Exception:
        return None
    finally:
        cursor.close()
    return None


def _retryable(error: Exception) -> bool:
    text = str(error).lower()
    name = type(error).__name__.lower()
    combined = f"{name} {text}"
    if any(token in combined for token in _NON_RETRYABLE_TOKENS):
        return False
    return any(token in combined for token in _RETRYABLE_TOKENS)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "iam"}


def load_redshift_config(settings: dict[str, Any]) -> RedshiftConnectConfig:
    host = env_value("REDSHIFT_HOST") or settings.get("host")
    user = (
        env_value("REDSHIFT_USER", "REDSHIFT_DB_USER")
        or settings.get("user")
        or settings.get("db_user")
    )
    database = (
        env_value("REDSHIFT_DATABASE", "REDSHIFT_DBNAME")
        or settings.get("dbname")
        or settings.get("database")
    )
    port = env_value("REDSHIFT_PORT") or settings.get("port") or DEFAULT_PORT
    password = env_value("REDSHIFT_PASSWORD") or settings.get("password")
    schema = env_value("REDSHIFT_SCHEMA") or settings.get("schema") or "public"
    method_raw = (
        env_value("REDSHIFT_METHOD")
        or settings.get("method")
        or ("iam" if env_value("REDSHIFT_IAM") else "database")
    )
    method = "iam" if _truthy(method_raw) or str(method_raw).strip().lower() == "iam" else "database"
    cluster_identifier = (
        env_value("REDSHIFT_CLUSTER_ID", "REDSHIFT_CLUSTER_IDENTIFIER")
        or settings.get("cluster_id")
        or settings.get("cluster_identifier")
    )
    region = env_value("REDSHIFT_REGION", "AWS_REGION", "AWS_DEFAULT_REGION") or settings.get("region")
    iam_profile = (
        env_value("AWS_PROFILE", "REDSHIFT_IAM_PROFILE")
        or settings.get("iam_profile")
        or settings.get("profile")
    )
    serverless = _truthy(env_value("REDSHIFT_SERVERLESS") or settings.get("is_serverless"))
    sslmode = str(settings.get("sslmode") or env_value("REDSHIFT_SSLMODE") or "require").lower()
    ssl = sslmode not in {"disable", "allow", "false", "0"}
    if not user:
        raise ConfigError("Redshift user is required (REDSHIFT_USER or dbt profile)")
    if not database:
        raise ConfigError("Redshift database is required (REDSHIFT_DATABASE or dbt profile dbname)")
    if method == "iam":
        if not host and not cluster_identifier:
            raise ConfigError(
                "Redshift IAM auth requires REDSHIFT_HOST or REDSHIFT_CLUSTER_ID "
                "(or host/cluster_id in the dbt profile)"
            )
    else:
        if not host:
            raise ConfigError("Redshift host is required (REDSHIFT_HOST or dbt profile)")
        if not password:
            raise ConfigError(
                "Redshift password is required (REDSHIFT_PASSWORD or dbt profile). "
                "For IAM, set method: iam in the profile or REDSHIFT_IAM=1."
            )
    return RedshiftConnectConfig(
        host=str(host) if host else None,
        database=str(database),
        user=str(user),
        schema=str(schema),
        port=int(port),
        password=str(password) if password else None,
        ssl=ssl,
        method=method,
        cluster_identifier=str(cluster_identifier) if cluster_identifier else None,
        region=str(region) if region else None,
        iam_profile=str(iam_profile) if iam_profile else None,
        serverless=serverless,
    )


def open_redshift_adapter(settings: dict[str, Any]) -> RedshiftAdapter:
    config = load_redshift_config(settings)
    started = time.perf_counter()
    log_step("Redshift connector import started")
    try:
        import redshift_connector
    except ImportError as error:
        log_step(
            "Redshift connector import completed",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise ConfigError(
            "Install frontier-runner[redshift] to open a Redshift session",
        ) from error
    log_step(
        "Redshift connector import completed",
        duration_ms=elapsed_ms(started),
        status="ok",
    )
    started = time.perf_counter()
    log_step("Redshift connection started")
    try:
        connection = redshift_connector.connect(**config.connect_kwargs())
        _configure_session(connection, config)
    except Exception as error:
        log_step(
            "Redshift connected",
            duration_ms=elapsed_ms(started),
            status=failure_status(error),
        )
        raise
    log_step("Redshift connected", duration_ms=elapsed_ms(started), status="ok")
    return RedshiftAdapter(
        connection,
        host=config.host,
        database=config.database,
        schema=config.schema,
        user=config.user,
        config=config,
    )


def _configure_session(connection: Any, config: RedshiftConnectConfig) -> None:
    timeout_ms = env_int("FRONTIER_REDSHIFT_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT_SECONDS) * 1000
    schema = quote_identifier(config.schema, '"')
    cursor = connection.cursor()
    try:
        cursor.execute(f"set search_path to {schema}")
        cursor.execute(f"set statement_timeout to {int(timeout_ms)}")
    finally:
        cursor.close()
