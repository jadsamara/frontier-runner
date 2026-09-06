# Changelog

All notable changes to frontier-runner are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.1]: https://github.com/jadsamara/frontier-runner/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jadsamara/frontier-runner/releases/tag/v0.1.0
