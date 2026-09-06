# Frontier Runner

Customer-side CLI for dbt + Snowflake + GitHub impact assessments.

The runner executes next to the dbt project. It sends metadata and aggregate
evidence to Frontier SaaS. Warehouse rows and warehouse credentials stay here.

Supported stack: **dbt Core or dbt Fusion**, **Snowflake**, **GitHub**, and
hosted Frontier SaaS. Other warehouses and Git providers are not available in
this installer.

## Install

```bash
pipx install "frontier-runner[snowflake]"
# or
python3 -m pip install "frontier-runner[snowflake]"
frontier --version
```

Until the package is on PyPI, install the GitHub Release wheel for a version
tag. You do not need a commit SHA:

```bash
pipx install https://github.com/jadsamara/frontier-runner/releases/download/v0.1.0/frontier_runner-0.1.0-py3-none-any.whl
pipx inject frontier-runner "snowflake-connector-python>=3.12,<4"
```

## First assessment (under 15 minutes)

From the root of an existing dbt project:

```bash
frontier signup
frontier login --api-key
frontier init
frontier discover
# review and activate the draft in Frontier
frontier doctor
frontier setup github
```

Commit `.github/workflows/frontier.yml`, open a test pull request, and Frontier
posts one PR comment. Then open the hosted assessment and confirm it links to
the pinned semantic manifest.

API keys are stored in the OS keychain (or `~/.config/frontier/credentials`
mode 0600). They are never written to `frontier.yml`, `dbt_project.yml`,
`profiles.yml`, Git, or generated workflows.

## Developer setup

From this repository:

```bash
python3 -m pip install -e ".[dev,snowflake]"
```

## Assessment commands

`python3 -m frontier` always works after an editable install.

`frontier run` reads `~/.dbt/profiles.yml` (and warehouse env vars such as
`SNOWFLAKE_*`). This installer supports Snowflake. Use `--dry-run` to exercise
the CLI without a warehouse.

Entity IDs in `frontier-run.json` are HMAC-SHA-256 hashed with
`FRONTIER_ENTITY_HASH_KEY` unless `--include-entity-ids` is set. The key is
required for hashed output; there is no plain SHA-256 fallback. Rotating the
key changes entity fingerprints across assessments. The hash key is never sent
to SaaS.

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

`frontier record-failure` writes a failed assessment without reading
`target/manifest.json` or `run_results.json`. Use it when dbt build fails so CI
cannot upload stale artifacts.

`frontier cdc inspect|status|consume|prove|upload` reads `frontier-cdc.yml` in
the dbt project. Inspect prints stream mappings. Status calls
`SYSTEM$STREAM_HAS_DATA` without consuming. Consume copies pending Snowflake
stream rows into `DATA_AGENT_DEV.FRONTIER_CDC` control tables inside a
transaction, then normalizes a DELETE/INSERT `METADATA$ISUPDATE=TRUE` pair
into one UPDATE. A plain SELECT is never treated as consumption. `cdc prove`
claims the oldest CAPTURED or FAILED batch, routes events to target keys from
the YAML mapping, materializes `DBT_DEV.FRONTIER_<batch_id>_AFFECTED_KEYS`,
and runs targeted compiled `customer_summary` SQL against current source
state. The existing mart is the pre-change baseline. Completion requires
routing and validation, not merely stream consumption. A candidate no-op is a
successful conservative assessment. Default prove is assessment-only;
`--apply` is required to mutate the mart. `cdc upload` sends aggregate CDC
evidence to SaaS without recapturing or reproving. If a previous COMPLETED
batch was not applied, a later prove fails with `BASELINE_STALE`. Logs include
stream name, batch id, counts, status, and duration — never entity IDs, row
contents, or credentials. Do not upload raw CDC keys to SaaS. Scheduled CDC
processing is not part of this installer.

`frontier compare` reads compiled SQL from the base-branch and PR manifests
(and `target/compiled` / `target-base/compiled` when `compiled_code` is
missing), classifies semantic changes with a restricted Snowflake parser
(sqlglot), and compiles supported diffs into a candidate-key impact query.
Alias and formatting changes are ignored. Grain changes, unknown UDFs, empty
compiled SQL, and other unsupported SQL return `FULL_REBUILD_REQUIRED`
instead of an empty candidate set. `frontier prove` confirms and repairs using
the production model whose compiled SQL actually changed, not a downstream
mart that only `ref()`s it. The comparison does not send warehouse rows to
SaaS. `inspect`, `run`, and `prove` accept `--base-manifest` so artifact
fingerprints, change kinds, and impact status are stored on the uploaded
assessment.

`frontier upload` posts `target/frontier-run.json` to `POST /api/v1/runs`. It
retries HTTP 429/5xx and network errors, and honors `Retry-After`. SaaS
commands resolve credentials in this order: `FRONTIER_API_KEY`, the OS
keychain, the `0600` fallback file, then `FRONTIER_DEMO_API_KEY` only when
`FRONTIER_ALLOW_LOCAL_MANIFEST` is set outside GitHub Actions. If none are
present, the CLI exits with `AUTH_REQUIRED: Run \`frontier login --api-key\``.
Hashed uploads set `entityIdsHashed: true`.

In GitHub Actions, assessments use `{project}-{GITHUB_SHA}` as `externalRunId`
and record repository, branch, commit, and PR number. After a successful
upload the runner upserts one pull-request comment (aggregates only, plus a
dashboard `/runs/<id>` link). `GITHUB_TOKEN` stays in the customer job.
`FRONTIER_DRY_RUN=true` is only for the SaaS fixture self-test and is rejected
by `frontier prove` in GitHub Actions. Customer CI must execute against the
live warehouse. Uploaded assessments set `runMode` to `live` or `fixture`.
`frontier upload --blocking` (or `FRONTIER_BLOCKING=true`) uploads and comments
first, then exits 1 if the assessment failed. The generated first-verification
workflow uses `FRONTIER_BLOCKING=false` unless you pass `--blocking`.

## Releases

Pin an immutable released version:

```bash
pip install "frontier-runner[snowflake]==0.1.0"
```

Until PyPI trusted publishing is reviewed and live, install the GitHub Release
wheel for the same version tag. Do not look up a runner Git SHA.

Do not `pip install ./runner` from a dbt repository. That path exists only in
the SaaS monorepo.

