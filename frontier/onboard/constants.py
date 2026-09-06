from __future__ import annotations

DEFAULT_API_URL = "https://frontier-web-x3l3etwczq-pd.a.run.app"
DOCS_ORIGIN = DEFAULT_API_URL
RUNNER_PACKAGE = "frontier-runner"
SUPPORTED_PYTHON = ((3, 11), (3, 12), (3, 13))
GITHUB_RELEASES = "https://github.com/jadsamara/frontier-runner/releases"
GITHUB_WHEEL = (
    "https://github.com/jadsamara/frontier-runner/releases/download/"
    "v{version}/frontier_runner-{version}-py3-none-any.whl"
)
KEYRING_SERVICE = "frontier-runner"
PRODUCTION_SCHEMA_HINTS = ("prod", "production", "analytics_prod")
CONFIG_DIR_NAME = ".frontier"
CONFIG_FILE_NAME = "config.yml"
GITIGNORE_ENTRIES = (
    "target/frontier-*.json",
    ".frontier/cache/",
    ".frontier/credentials*",
)
