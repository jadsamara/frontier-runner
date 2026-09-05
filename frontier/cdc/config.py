from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from frontier.config import ConfigError, is_secret_key
from frontier.warehouse import split_relation_parts

CDC_FILE_NAME = "frontier-cdc.yml"
SUPPORTED_PROVIDERS = frozenset({"snowflake_stream"})
BEFORE_IMAGE_OPS = frozenset({"DELETE", "KEY_CHANGE"})
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class CdcSourceConfig:
    source_model: str
    base_relation: str
    stream_relation: str
    primary_key: str
    target_entity: str
    target_key: str
    require_before_image_for: tuple[str, ...]

    @property
    def stream_name(self) -> str:
        return split_relation_parts(self.stream_relation)[2]


@dataclass(frozen=True)
class CdcConfig:
    version: int
    provider: str
    sources: tuple[CdcSourceConfig, ...]
    path: Path

    @property
    def control_database(self) -> str:
        database, _schema, _table = split_relation_parts(self.sources[0].stream_relation)
        if not database:
            raise ConfigError("CDC stream_relation must be database.schema.table")
        return database

    @property
    def control_schema(self) -> str:
        _database, schema, _table = split_relation_parts(self.sources[0].stream_relation)
        if not schema:
            raise ConfigError("CDC stream_relation must be database.schema.table")
        return schema


def _require_ident(value: str, *, field: str) -> str:
    token = (value or "").strip()
    if not _IDENT.fullmatch(token):
        raise ConfigError(f"CDC {field} is not a safe identifier")
    return token


def _require_relation(value: str, *, field: str) -> str:
    text = (value or "").strip()
    database, schema, table = split_relation_parts(text)
    if not database or not schema or not table:
        raise ConfigError(f"CDC {field} must be database.schema.table")
    for part, name in ((database, "database"), (schema, "schema"), (table, "table")):
        if not _IDENT.fullmatch(part):
            raise ConfigError(f"CDC {field} {name} is not a safe identifier")
    return f"{database}.{schema}.{table}"


def _reject_secrets(payload: Any) -> None:
    if isinstance(payload, dict):
        for key, child in payload.items():
            if is_secret_key(str(key)):
                raise ConfigError("CDC configuration must not contain credentials")
            _reject_secrets(child)
        return
    if isinstance(payload, list):
        for item in payload:
            _reject_secrets(item)


def _before_image_ops(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list) or not raw:
        raise ConfigError("CDC require_before_image_for must be a list")
    ops: list[str] = []
    for item in raw:
        op = str(item or "").strip().upper()
        if op not in BEFORE_IMAGE_OPS:
            raise ConfigError("CDC require_before_image_for contains an unknown operation")
        if op not in ops:
            ops.append(op)
    return tuple(ops)


def load_cdc_config(path: Path) -> CdcConfig:
    if not path.is_file():
        raise ConfigError(f"Missing CDC configuration {path}")
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise ConfigError("CDC configuration must be a mapping")
    _reject_secrets(payload)
    try:
        version = int(payload.get("version") or 1)
    except (TypeError, ValueError) as error:
        raise ConfigError("CDC version must be an integer") from error
    if version != 1:
        raise ConfigError("Unsupported CDC configuration version")
    provider = str(payload.get("provider") or "").strip()
    if provider not in SUPPORTED_PROVIDERS:
        raise ConfigError("Unknown CDC provider")
    rows = payload.get("sources")
    if not isinstance(rows, list) or not rows:
        raise ConfigError("CDC configuration requires sources")
    sources: list[CdcSourceConfig] = []
    seen_streams: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise ConfigError("CDC source must be a mapping")
        source_model = _require_ident(str(row.get("source_model") or ""), field="source_model")
        primary_key = _require_ident(str(row.get("primary_key") or ""), field="primary_key")
        target_key = _require_ident(str(row.get("target_key") or ""), field="target_key")
        target_entity = _require_ident(str(row.get("target_entity") or ""), field="target_entity")
        stream_relation = _require_relation(str(row.get("stream_relation") or ""), field="stream_relation")
        base_relation = _require_relation(str(row.get("base_relation") or ""), field="base_relation")
        stream_key = stream_relation.lower()
        if stream_key in seen_streams:
            raise ConfigError("CDC configuration has duplicate stream relations")
        seen_streams.add(stream_key)
        sources.append(
            CdcSourceConfig(
                source_model=source_model,
                base_relation=base_relation,
                stream_relation=stream_relation,
                primary_key=primary_key,
                target_key=target_key,
                target_entity=target_entity,
                require_before_image_for=_before_image_ops(row.get("require_before_image_for")),
            )
        )
    return CdcConfig(version=version, provider=provider, sources=tuple(sources), path=path)


def cdc_config_path(project_dir: Path, explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    return project_dir / CDC_FILE_NAME
