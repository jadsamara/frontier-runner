# Frontier Runner

The runner executes in the customer environment next to the dbt project and the warehouse.

It sends metadata and aggregate evidence to Frontier SaaS. Raw warehouse rows and warehouse credentials stay here.

## Setup

```bash
python3 -m pip install -e ".[dev]"
# Live sessions need the extra for that warehouse:
python3 -m pip install -e ".[snowflake]"   # Snowflake
python3 -m pip install -e ".[bigquery]"    # BigQuery
python3 -m pip install -e ".[databricks]"  # Databricks SQL
python3 -m pip install -e ".[postgres]"    # PostgreSQL
python3 -m pip install -e ".[redshift]"    # Redshift
# pyenv: refresh shims so the `frontier` command is on PATH
pyenv rehash
```

From the SaaS monorepo, run those commands inside `runner/`.

Then, from the dbt project or with a project path:

```bash
python3 -m frontier init /path/to/jaffle_shop
python3 -m frontier inspect /path/to/jaffle_shop --base-manifest /path/to/base/manifest.json
python3 -m frontier compare /path/to/jaffle_shop --base-manifest /path/to/base/manifest.json
python3 -m frontier run /path/to/jaffle_shop
python3 -m frontier prove /path/to/jaffle_shop --base-manifest /path/to/base/manifest.json
python3 -m frontier upload /path/to/jaffle_shop
python3 -m frontier cdc inspect /path/to/jaffle_shop --target dev
python3 -m frontier cdc status /path/to/jaffle_shop --target dev
python3 -m frontier cdc consume /path/to/jaffle_shop --target dev
python3 -m frontier cdc prove /path/to/jaffle_shop --target dev
python3 -m frontier cdc upload /path/to/jaffle_shop --target dev
```

`python3 -m frontier` always works. After `pyenv rehash`, `frontier` works the same way.

`frontier run` reads `~/.dbt/profiles.yml` (and warehouse env vars such as `SNOWFLAKE_*`). The profile `type` selects Snowflake, BigQuery, Databricks SQL, PostgreSQL, or Redshift. Use `--dry-run` to exercise the CLI without a warehouse.

Entity IDs in `frontier-run.json` are HMAC-SHA-256 hashed with `FRONTIER_ENTITY_HASH_KEY` unless `--include-entity-ids` is set. The key is required for hashed output; there is no plain SHA-256 fallback.

`frontier prove` measures a SQL-change or mutation-repair experiment.
When `--base-manifest` shows modified, added, or removed SQL, the default
`seeds/change_events.csv` is ignored: the assessment is the compiled SQL
diff, not a hand-edited event list. Isolated affected keys are written to
`DBT_CI.FRONTIER_<run_id>_AFFECTED_KEYS` with separate event and
SQL-change origins. The M14 impact query runs in Snowflake and is unioned
for execution. Targeted SQL pushes the key join into source CTEs before
aggregates. Hand-written `frontier_affected_customers` / repaired models
are not required for a SQL-change proof. Impact compilation skips models
tagged `frontier_demo` / `frontier_mutation` and `*_after` overlays unless
they are the configured target. Candidate discovery never joins
`frontier_affected_customers` or the isolated keys table; equivalent
predicates from multiple consumers collapse to one query. When candidates
exceed `sql_change.rebuild_recommended_pct` of the full entity set
(default 75, or `FRONTIER_SQL_CHANGE_REBUILD_PCT`), the assessment is
`FULL_REBUILD_RECOMMENDED` instead of an inefficient targeted proof.
When base and PR SQL differ, a
missing or failed impact query is `FULL_REBUILD_REQUIRED` rather than an
event-only frontier. Customer CI must call `prove`, not `run`.

`frontier record-failure` writes a failed assessment without reading `target/manifest.json` or `run_results.json`. Use it when dbt build fails so CI cannot upload stale artifacts.

`frontier cdc inspect|status|consume|prove|upload` reads `frontier-cdc.yml` in the dbt project. Inspect prints stream mappings. Status calls `SYSTEM$STREAM_HAS_DATA` without consuming. Consume copies pending Snowflake stream rows into `DATA_AGENT_DEV.FRONTIER_CDC` control tables inside a transaction, then normalizes a DELETE/INSERT `METADATA$ISUPDATE=TRUE` pair into one UPDATE. A plain SELECT is never treated as consumption. `cdc prove` claims the oldest CAPTURED or FAILED batch, routes events to target keys from the YAML mapping, materializes `DBT_DEV.FRONTIER_<batch_id>_AFFECTED_KEYS`, and runs targeted compiled `customer_summary` SQL against current source state. The existing mart is the pre-change baseline. Completion requires routing and validation, not merely stream consumption. A candidate no-op is a successful conservative assessment. Default prove is assessment-only; `--apply` is required to mutate the mart. `cdc upload` sends aggregate CDC evidence to SaaS without recapturing or reproving. If a previous COMPLETED batch was not applied, a later prove fails with `BASELINE_STALE`. Logs include stream name, batch id, counts, status, and duration — never entity IDs, row contents, or credentials. Do not upload raw CDC keys to SaaS.

`frontier compare` reads compiled SQL from the base-branch and PR manifests (and `target/compiled` / `target-base/compiled` when `compiled_code` is missing), classifies semantic changes with a restricted Snowflake parser (sqlglot), and compiles supported diffs into a candidate-key impact query. Alias and formatting changes are ignored. Grain changes, unknown UDFs, empty compiled SQL, and other unsupported SQL return `FULL_REBUILD_REQUIRED` instead of an empty candidate set. `frontier prove` confirms and repairs using the production model whose compiled SQL actually changed, not a downstream mart that only `ref()`s it. The comparison does not send warehouse rows to SaaS. `inspect`, `run`, and `prove` accept `--base-manifest` so artifact fingerprints, change kinds, and impact status are stored on the uploaded assessment.

`frontier upload` posts `target/frontier-run.json` to `POST /api/v1/runs`. It retries HTTP 429/5xx and network errors, and honors `Retry-After`. It uses `FRONTIER_API_KEY` if that variable is set, otherwise `FRONTIER_DEMO_API_KEY`. A leftover placeholder in `FRONTIER_API_KEY` will win over the demo key — `unset FRONTIER_API_KEY` if you intend to use the local demo key. Hashed uploads set `entityIdsHashed: true`.

In GitHub Actions, assessments use `{project}-{GITHUB_SHA}` as `externalRunId` and record repository, branch, commit, and PR number. After a successful upload the runner upserts one pull-request comment (aggregates only, plus a dashboard `/runs/<id>` link). `GITHUB_TOKEN` stays in the customer job. `FRONTIER_DRY_RUN=true` is only for the SaaS fixture self-test and is rejected by `frontier prove` in GitHub Actions. Customer CI must execute against the live warehouse (`DATA_AGENT_DEV.DBT_CI`). Uploaded assessments set `runMode` to `live` or `fixture`. `frontier upload --blocking` (or `FRONTIER_BLOCKING=true`) uploads and comments first, then exits 1 if the assessment failed.

## Releases

Pin an immutable version from an external dbt repository:

```bash
pip install "frontier-runner[snowflake]==0.1.0"
# until PyPI publish:
pip install "frontier-runner[snowflake] @ git+https://github.com/jadsamara/frontier-runner.git@<commit-sha>"
```

Do not `pip install ./runner` from a dbt repository. That path exists only in the SaaS monorepo.

See `docs/milestone-6.md` in [frontier-software](https://github.com/jadsamara/frontier-software).
