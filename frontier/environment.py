"""Detect mixed dbt compile environments before warehouse execution."""

from __future__ import annotations

from dataclasses import dataclass

from frontier.snapshot import collect_source_relations
from frontier.warehouse import split_relation_parts

ENVIRONMENT_MISMATCH = "ENVIRONMENT_MISMATCH"
_SHARED_SOURCE_DATABASES = {"SNOWFLAKE_SAMPLE_DATA"}


@dataclass(frozen=True)
class EnvironmentCheck:
    ok: bool
    code: str | None = None
    reason: str | None = None
    artifact_databases: tuple[str, ...] = ()
    profile_database: str | None = None


def _norm(value: str | None) -> str:
    return (value or "").strip().strip('"').upper()


def databases_from_sql(*sqls: str, dialect: str = "snowflake") -> set[str]:
    found: set[str] = set()
    relations = collect_source_relations(*(item for item in sqls if item), dialect=dialect)
    for name in relations:
        database, _schema, _table = split_relation_parts(name)
        token = _norm(database)
        if token and token not in _SHARED_SOURCE_DATABASES:
            found.add(token)
    return found


def databases_from_nodes(*databases: str | None) -> set[str]:
    return {_norm(item) for item in databases if _norm(item)}


def assess_artifact_environment(
    *,
    base_sql: str = "",
    pr_sql: str = "",
    candidate_sql: str = "",
    base_database: str | None = None,
    pr_database: str | None = None,
    profile_database: str | None = None,
    dialect: str = "snowflake",
) -> EnvironmentCheck:
    """Fail closed when compiled SQL and the selected profile disagree."""
    from_sql = databases_from_sql(base_sql, pr_sql, candidate_sql, dialect=dialect)
    from_nodes = databases_from_nodes(base_database, pr_database)
    artifact = tuple(sorted(from_sql | from_nodes))
    profile = _norm(profile_database)
    if not artifact:
        return EnvironmentCheck(ok=True, artifact_databases=artifact, profile_database=profile or None)
    unique = set(artifact)
    if len(unique) > 1:
        reason = (
            "ENVIRONMENT_MISMATCH: base and head compiled relations reference "
            f"databases {list(artifact)}; compiling both against one target is required"
        )
        return EnvironmentCheck(
            ok=False,
            code=ENVIRONMENT_MISMATCH,
            reason=reason,
            artifact_databases=artifact,
            profile_database=profile or None,
        )
    artifact_db = artifact[0]
    if profile and artifact_db != profile:
        reason = (
            "ENVIRONMENT_MISMATCH: compiled artifacts reference "
            f"{artifact_db} but the selected profile/target uses {profile}; "
            "refusing to execute a candidate query from a mixed environment"
        )
        return EnvironmentCheck(
            ok=False,
            code=ENVIRONMENT_MISMATCH,
            reason=reason,
            artifact_databases=artifact,
            profile_database=profile,
        )
    return EnvironmentCheck(ok=True, artifact_databases=artifact, profile_database=profile or None)
