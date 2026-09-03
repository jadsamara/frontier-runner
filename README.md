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
```

`python3 -m frontier` always works. After `pyenv rehash`, `frontier` works the same way.

`frontier run` reads `~/.dbt/profiles.yml` (and warehouse env vars such as `SNOWFLAKE_*`). The profile `type` selects Snowflake, BigQuery, Databricks SQL, PostgreSQL, or Redshift. Use `--dry-run` to exercise the CLI without a warehouse.

Entity IDs in `frontier-run.json` are HMAC-SHA-256 hashed with `FRONTIER_ENTITY_HASH_KEY` unless `--include-entity-ids` is set. The key is required for hashed output; there is no plain SHA-256 fallback.

`frontier prove` measures the mutation-repair experiment (full vs frontier rows recomputed, missing/extra frontier entities, mismatched final rows, and EXCEPT duration) after the jaffle-shop overlay models have been built. Customer CI must call `prove`, not `run`.

`frontier record-failure` writes a failed assessment without reading `target/manifest.json` or `run_results.json`. Use it when dbt build fails so CI cannot upload stale artifacts.

`frontier compare` reads compiled SQL from the base-branch and PR manifests, classifies semantic changes with a restricted Snowflake parser (sqlglot), and reports added, removed, and modified models plus downstream consumers. Alias and formatting changes are ignored. Grouping, grain, and unsupported SQL cannot be treated as a safe narrow frontier. The comparison does not infer affected rows. `inspect`, `run`, and `prove` accept `--base-manifest` so both artifact fingerprints and change kinds are stored on the uploaded assessment.

`frontier upload` posts `target/frontier-run.json` to `POST /api/v1/runs`. It retries HTTP 429/5xx and network errors, and honors `Retry-After`. It uses `FRONTIER_API_KEY` if that variable is set, otherwise `FRONTIER_DEMO_API_KEY`. A leftover placeholder in `FRONTIER_API_KEY` will win over the demo key — `unset FRONTIER_API_KEY` if you intend to use the local demo key. Hashed uploads set `entityIdsHashed: true`.

In GitHub Actions, assessments use `{project}-{GITHUB_SHA}` as `externalRunId` and record repository, branch, commit, and PR number. After a successful upload the runner upserts one pull-request comment (aggregates only, plus a dashboard `/runs/<id>` link). `GITHUB_TOKEN` stays in the customer job. `FRONTIER_DRY_RUN=true` is only for the SaaS fixture self-test. `frontier upload --blocking` (or `FRONTIER_BLOCKING=true`) uploads and comments first, then exits 1 if the assessment failed.

## Releases

Pin an immutable version from an external dbt repository:

```bash
pip install "frontier-runner[snowflake]==0.1.0"
# until PyPI publish:
pip install "frontier-runner[snowflake] @ git+https://github.com/jadsamara/frontier-runner.git@<commit-sha>"
```

Do not `pip install ./runner` from a dbt repository. That path exists only in the SaaS monorepo.

See `docs/milestone-6.md` in [frontier-software](https://github.com/jadsamara/frontier-software).
