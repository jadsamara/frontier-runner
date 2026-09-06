from __future__ import annotations


def snowflake_permission_sql(
    *,
    database: str,
    schema: str,
    role: str = "FRONTIER_PR_ASSESSOR",
    warehouse: str = "COMPUTE_WH",
    work_schema: str = "FRONTIER_WORK",
    include_query_history: bool = False,
    include_cdc: bool = False,
    cdc_schema: str = "FRONTIER_CDC",
) -> str:
    database = database.strip() or "DEV"
    schema = schema.strip() or "DBT_DEV"
    lines = [
        f"-- Least-privilege Snowflake grants for Frontier PR assessments.",
        f"-- Do not use ACCOUNTADMIN. Do not grant write access to production marts.",
        f"-- Apply as a security administrator, then use ROLE {role} in CI.",
        "",
        f"CREATE ROLE IF NOT EXISTS {role};",
        f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};",
        f"GRANT USAGE ON DATABASE {database} TO ROLE {role};",
        f"GRANT USAGE ON SCHEMA {database}.{schema} TO ROLE {role};",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {database}.{schema} TO ROLE {role};",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {database}.{schema} TO ROLE {role};",
        f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.{schema} TO ROLE {role};",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.{schema} TO ROLE {role};",
        "",
        f"-- Temporary Frontier key tables (not production marts).",
        f"CREATE SCHEMA IF NOT EXISTS {database}.{work_schema};",
        f"GRANT USAGE, CREATE TABLE ON SCHEMA {database}.{work_schema} TO ROLE {role};",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {database}.{work_schema} TO ROLE {role};",
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA {database}.{work_schema} TO ROLE {role};",
    ]
    if include_query_history:
        lines.extend(
            [
                "",
                "-- Query history is optional and disabled unless requested.",
                "GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE "
                f"{role};",
            ]
        )
    if include_cdc:
        lines.extend(
            [
                "",
                "-- CDC Streams only when CDC is configured.",
                f"GRANT USAGE ON SCHEMA {database}.{cdc_schema} TO ROLE {role};",
                f"GRANT SELECT ON ALL TABLES IN SCHEMA {database}.{cdc_schema} TO ROLE {role};",
                f"GRANT SELECT ON ALL STREAMS IN SCHEMA {database}.{cdc_schema} TO ROLE {role};",
            ]
        )
    lines.extend(
        [
            "",
            f"-- Intentionally omitted: INSERT/UPDATE/DELETE on {database}.{schema}",
            "-- (production or shared marts). The PR-assessment role is read-only there.",
        ]
    )
    return "\n".join(lines) + "\n"
