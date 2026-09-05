from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SECRET_KEY_PARTS = (
    "password",
    "passwd",
    "secret",
    "token",
    "private_key",
    "privatekey",
    "access_key",
    "apikey",
    "api_key",
    "hash_key",
    "credential",
)


class ConfigError(ValueError):
    """Invalid frontier.yml or dbt project configuration."""


def is_secret_key(name: str) -> bool:
    lowered = name.lower().replace("-", "_")
    return any(part in lowered for part in SECRET_KEY_PARTS)


def redact(value: Any) -> Any:
    """Return a copy with secret fields replaced. Never used for connecting."""
    if isinstance(value, dict):
        return {
            key: ("********" if is_secret_key(key) else redact(child))
            for key, child in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


@dataclass(frozen=True)
class Route:
    kind: str
    query: str | None = None


@dataclass(frozen=True)
class RelationConfig:
    name: str
    change_key: str
    route: Route


@dataclass(frozen=True)
class ModelConfig:
    name: str
    entity: str
    key: str
    grain: str


@dataclass(frozen=True)
class UploadConfig:
    include_entity_ids: bool = False
    hash_entity_ids: bool = False


@dataclass(frozen=True)
class ProofConfig:
    before_mart: str = "customer_summary"
    after_mart: str = "customer_summary_after"
    repaired_mart: str = "customer_summary_repaired"
    frontier: str = "frontier_affected_customers"
    targeted_after: str = "frontier_customer_summary_target_after"
    deleted_order: str = "mutation_deleted_order"


@dataclass(frozen=True)
class SqlChangeConfig:
    rebuild_recommended_pct: float = 75.0


@dataclass(frozen=True)
class FrontierConfig:
    project: str
    environment: str
    model: ModelConfig
    relations: dict[str, RelationConfig]
    upload: UploadConfig = field(default_factory=UploadConfig)
    proof: ProofConfig = field(default_factory=ProofConfig)
    sql_change: SqlChangeConfig = field(default_factory=SqlChangeConfig)
    api_url: str = "http://127.0.0.1:3000"
    path: Path | None = None

    def relation(self, name: str) -> RelationConfig:
        try:
            return self.relations[name]
        except KeyError as error:
            raise ConfigError(f"No relation named '{name}' in frontier.yml") from error


def _parse_route(name: str, raw: Any) -> Route:
    if raw == "direct":
        return Route(kind="direct")
    if isinstance(raw, dict) and "query" in raw:
        query = str(raw["query"]).strip()
        if not query:
            raise ConfigError(f"Relation '{name}' route.query is empty")
        extra = set(raw) - {"query"}
        if extra:
            raise ConfigError(f"Relation '{name}' route has unknown keys: {sorted(extra)}")
        return Route(kind="query", query=query)
    raise ConfigError(
        f"Relation '{name}' route must be 'direct' or {{query: ...}}",
    )


def load_frontier_config(path: Path) -> FrontierConfig:
    if not path.is_file():
        raise ConfigError(f"Missing Frontier config: {path}")

    raw = yaml.safe_load(path.read_text())
    if not isinstance(raw, dict):
        raise ConfigError("frontier.yml must be a mapping")

    try:
        project = str(raw["project"]).strip()
        environment = str(raw["environment"]).strip()
        model_raw = raw["model"]
        relations_raw = raw["relations"]
    except KeyError as error:
        raise ConfigError(f"frontier.yml missing required key: {error.args[0]}") from error

    if not project or not environment:
        raise ConfigError("project and environment are required")
    if not isinstance(model_raw, dict):
        raise ConfigError("model must be a mapping")
    if not isinstance(relations_raw, dict) or not relations_raw:
        raise ConfigError("relations must be a non-empty mapping")

    try:
        model = ModelConfig(
            name=str(model_raw["name"]).strip(),
            entity=str(model_raw["entity"]).strip(),
            key=str(model_raw["key"]).strip(),
            grain=str(model_raw["grain"]).strip(),
        )
    except KeyError as error:
        raise ConfigError(f"model missing required key: {error.args[0]}") from error

    if not all([model.name, model.entity, model.key, model.grain]):
        raise ConfigError("model name, entity, key, and grain are required")

    relations: dict[str, RelationConfig] = {}
    for name, spec in relations_raw.items():
        if not isinstance(spec, dict):
            raise ConfigError(f"Relation '{name}' must be a mapping")
        try:
            change_key = str(spec["change_key"]).strip()
            route = _parse_route(str(name), spec["route"])
        except KeyError as error:
            raise ConfigError(
                f"Relation '{name}' missing required key: {error.args[0]}",
            ) from error
        relations[str(name)] = RelationConfig(
            name=str(name),
            change_key=change_key,
            route=route,
        )

    upload_raw = raw.get("upload") or {}
    if upload_raw is None:
        upload_raw = {}
    if not isinstance(upload_raw, dict):
        raise ConfigError("upload must be a mapping")

    api_raw = raw.get("api") or {}
    if not isinstance(api_raw, dict):
        raise ConfigError("api must be a mapping")

    proof_raw = raw.get("proof") or {}
    if proof_raw is None:
        proof_raw = {}
    if not isinstance(proof_raw, dict):
        raise ConfigError("proof must be a mapping")

    defaults = ProofConfig()
    proof = ProofConfig(
        before_mart=str(proof_raw.get("before_mart") or defaults.before_mart).strip(),
        after_mart=str(proof_raw.get("after_mart") or defaults.after_mart).strip(),
        repaired_mart=str(proof_raw.get("repaired_mart") or defaults.repaired_mart).strip(),
        frontier=str(proof_raw.get("frontier") or defaults.frontier).strip(),
        targeted_after=str(proof_raw.get("targeted_after") or defaults.targeted_after).strip(),
        deleted_order=str(proof_raw.get("deleted_order") or defaults.deleted_order).strip(),
    )

    sql_change_raw = raw.get("sql_change") or {}
    if sql_change_raw is None:
        sql_change_raw = {}
    if not isinstance(sql_change_raw, dict):
        raise ConfigError("sql_change must be a mapping")
    sql_change_defaults = SqlChangeConfig()
    pct_raw = sql_change_raw.get(
        "rebuild_recommended_pct",
        sql_change_defaults.rebuild_recommended_pct,
    )
    try:
        rebuild_pct = float(pct_raw)
    except (TypeError, ValueError) as error:
        raise ConfigError("sql_change.rebuild_recommended_pct must be a number") from error
    if rebuild_pct <= 0 or rebuild_pct > 100:
        raise ConfigError("sql_change.rebuild_recommended_pct must be in (0, 100]")

    return FrontierConfig(
        project=project,
        environment=environment,
        model=model,
        relations=relations,
        upload=UploadConfig(
            include_entity_ids=bool(upload_raw.get("include_entity_ids", False)),
            hash_entity_ids=bool(upload_raw.get("hash_entity_ids", False)),
        ),
        proof=proof,
        sql_change=SqlChangeConfig(rebuild_recommended_pct=rebuild_pct),
        api_url=str(api_raw.get("url") or "http://127.0.0.1:3000").rstrip("/"),
        path=path,
    )


INIT_FRONTIER_YML = """\
project: jaffle_shop
environment: dev

model:
  name: customer_summary
  entity: customer
  key: customer_id
  grain: one_row_per_customer

relations:
  stg_customers:
    change_key: customer_id
    route: direct

  stg_orders:
    change_key: order_id
    route:
      query: |
        select customer_id
        from {{ ref('stg_orders') }}
        where order_id in ({{ changed_values }})

sql_change:
  rebuild_recommended_pct: 75
"""


def write_init_config(path: Path, *, force: bool = False) -> Path:
    if path.exists() and not force:
        raise ConfigError(f"{path} already exists (pass --force to overwrite)")
    path.write_text(INIT_FRONTIER_YML)
    return path


def sql_change_rebuild_recommended_pct(config: FrontierConfig) -> float:
    raw = (os.environ.get("FRONTIER_SQL_CHANGE_REBUILD_PCT") or "").strip()
    if raw:
        try:
            value = float(raw)
        except ValueError as error:
            raise ConfigError(
                "FRONTIER_SQL_CHANGE_REBUILD_PCT must be a number in (0, 100]",
            ) from error
    else:
        value = config.sql_change.rebuild_recommended_pct
    if value <= 0 or value > 100:
        raise ConfigError("sql_change rebuild threshold must be in (0, 100]")
    return value


def should_recommend_rebuild(
    candidate_count: int,
    full_entity_count: int,
    pct: float,
) -> bool:
    if full_entity_count <= 0 or candidate_count < 0:
        return False
    return candidate_count * 100 >= pct * full_entity_count
