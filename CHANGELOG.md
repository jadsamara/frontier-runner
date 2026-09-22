# Changelog

All notable changes to frontier-runner are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] — 2026-09-22

- Made filter-v1 eligibility and the RowCover candidate compiler authoritative for `compare` and `prove`. Ineligible plans stay `UNCERTIFIED` and are not reported as safe for targeted repair.
- Failed `prove` writes a fresh uniquely identified run artifact. `upload` refuses a stale `frontier-run.json`.
- Targeted SQL generation failures are classified as execution failures instead of being swallowed.
- Mixed compiled dbt environments fail before warehouse execution with `ENVIRONMENT_MISMATCH`.
- Economics stay independent of `SQL_CERTIFIED` and use measured bytes rather than a demonstrated cost saving.

## [0.2.0] — 2026-09-21

- Added browser-based CLI authentication.
- Added warehouse adapter architecture.
- Added BigQuery and Redshift integration work.
- Improved generated semantic mappings and onboarding.
- Preserved measured candidates when warehouse execution fails.

## [0.1.3] — 2026-09-14

### Added

- Generated semantic mappings are runtime-ready for SQL-change assessments
  when discovered routes verify. Review in Frontier is optional.
- BigQuery SQL-change assessments: install `frontier-runner[bigquery]`,
  authenticate with Workload Identity Federation in GitHub Actions, and run
  the same inspect/compare/prove path. CDC is not available for BigQuery.

### Fixed

- Manifest discovery is idempotent: an unchanged generated mapping reuses the
  active runtime version instead of creating another.
- Invalid human overrides are discarded safely and do not churn a new version
  on every `frontier discover`.
- Route inference and change-scoped eligibility keep SQL-change assessments
  unblocked when unrelated sources are unresolved.
- Execution failures remain `EXECUTION_FAILED` and preserve measured
  candidates instead of dropping them.
- Clean SaaS onboarding configuration fixes for `.frontier/config.yml` and
  related setup.

## [0.1.2] — 2026-09-06

### Fixed

- SaaS-backed commands read `.frontier/config.yml` from `frontier init` and no
  longer require a root `frontier.yml`. That legacy file is optional and is
  used only with `--allow-local-manifest`.
- `frontier manifest fetch` pins the authenticated project's active semantic
  manifest to `target/frontier-manifest.json` without a local mapping file.
- `frontier discover` prints a public review URL from the configured SaaS
  origin instead of a Cloud Run bind host such as `0.0.0.0`.
- `frontier init` prints `Next: frontier discover` when already authenticated.
- `frontier doctor` sequences the next action by onboarding dependency and
  does not mark “Active manifest matches dbt project” as passed when none
  exists.
- Discovering a dbt project whose name differs from the authenticated Frontier
  project requires confirmation (or `--force`) before upload.

## [0.1.1] — 2026-09-06

### Fixed

- Clean-install SaaS commands resolve stored OS keychain credentials through
  one shared resolver instead of requiring `FRONTIER_API_KEY` in the
  environment.
- Python package metadata (`requires-python`) matches the supported 3.11–3.13
  installer range.
- CDC prove tests detect raw entity-ID exposure without treating digit
  sequences inside fingerprints as leaks.

### Changed

- The release workflow verifies built metadata and smoke-installs the wheel
  (`frontier --version`, `frontier --help`, Snowflake extra) before PyPI
  publish and GitHub Release.

## [0.1.0] — 2026-09-06

### Added

- Customer CLI for dbt + Snowflake + GitHub assessments.
- `frontier signup`, `login`, `init`, `discover`, `doctor`, `setup github`,
  `setup hash-key`, `demo change`, `update-check`, `logout`, and `auth status`.
- Active SaaS semantic-manifest fetch, SQL-change compare/prove, and aggregate upload.

### Fixed

- SaaS commands resolve stored keychain credentials through one shared
  resolver instead of requiring `FRONTIER_API_KEY` in the environment.

[0.2.1]: https://github.com/jadsamara/frontier-runner/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/jadsamara/frontier-runner/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/jadsamara/frontier-runner/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/jadsamara/frontier-runner/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/jadsamara/frontier-runner/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jadsamara/frontier-runner/releases/tag/v0.1.0
