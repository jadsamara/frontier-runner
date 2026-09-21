from __future__ import annotations


def bigquery_permission_guidance(
    *,
    project: str,
    dataset: str,
    location: str = "US",
    work_dataset: str | None = None,
) -> str:
    project = project.strip() or "my-gcp-project"
    dataset = dataset.strip() or "dbt_dev"
    work = (work_dataset or dataset).strip() or dataset
    return "\n".join(
        [
            "# Least-privilege BigQuery IAM for Frontier PR assessments.",
            "# Do not grant roles/bigquery.admin. Do not write to production marts.",
            "# Authenticate GitHub Actions with Workload Identity Federation.",
            "# Do not commit a service-account JSON key. CDC is not available for BigQuery.",
            "",
            f"# Project: {project}",
            f"# Model dataset (read): {dataset}",
            f"# Isolated work dataset (create/drop FRONTIER_* tables): {work}",
            f"# Location: {location}",
            "",
            "gcloud projects add-iam-policy-binding "
            f"{project} \\",
            '  --member="serviceAccount:frontier-pr-assessor@'
            f'{project}.iam.gserviceaccount.com" \\',
            '  --role="roles/bigquery.jobUser"',
            "",
            "# Grant dataViewer on source and model datasets, and dataEditor only on the",
            "# non-production work dataset used for isolated FRONTIER_* key tables.",
            f"bq update --source /dev/stdin {project}:{dataset} <<'EOF'",
            "{",
            '  "access": [',
            "    {",
            '      "role": "READER",',
            f'      "userByEmail": "frontier-pr-assessor@{project}.iam.gserviceaccount.com"',
            "    }",
            "  ]",
            "}",
            "EOF",
            "",
            "# Intentionally omitted: write access to production mart datasets.",
            "# Isolated tables are named FRONTIER_<run_id>_AFFECTED_KEYS and must not be shared across PRs.",
            "",
        ]
    )


def redshift_permission_sql(
    *,
    database: str,
    schema: str,
    user: str = "frontier_pr_assessor",
    work_schema: str | None = None,
) -> str:
    database = database.strip() or "dev"
    schema = schema.strip() or "dbt_ci"
    work = (work_schema or schema).strip() or schema
    return "\n".join(
        [
            "-- Least-privilege Redshift grants for Frontier PR assessments.",
            "-- Do not grant superuser. Do not write to production marts.",
            "-- Keep host, password, and AWS tokens in the customer environment / GitHub secrets.",
            "-- CDC is not available for Redshift.",
            "",
            f"-- Database: {database}",
            f"-- Model schema (read): {schema}",
            f"-- Isolated work schema (create/drop FRONTIER_* tables): {work}",
            f"-- User: {user}",
            "",
            f"GRANT USAGE ON SCHEMA {schema} TO {user};",
            f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {user};",
            f"GRANT SELECT ON ALL VIEWS IN SCHEMA {schema} TO {user};",
            "",
            "-- Temporary Frontier key tables (not production marts).",
            f"CREATE SCHEMA IF NOT EXISTS {work};",
            f"GRANT USAGE, CREATE ON SCHEMA {work} TO {user};",
            f"GRANT SELECT, INSERT, UPDATE, DELETE, DROP ON ALL TABLES IN SCHEMA {work} TO {user};",
            "",
            "-- Optional query history (own STL rows). Without SYSLOG ACCESS UNRESTRICTED,",
            "-- scan metrics are marked unavailable rather than invented.",
            f"-- ALTER USER {user} SYSLOG ACCESS RESTRICTED;",
            "",
            f"-- Intentionally omitted: INSERT/UPDATE/DELETE on {schema}",
            "-- (production or shared marts). The PR-assessment user is read-only there.",
            "-- Isolated tables are named FRONTIER_<run_id>_AFFECTED_KEYS and must not be shared across PRs.",
            "",
        ]
    )


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
