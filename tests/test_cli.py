from __future__ import annotations

import json
from pathlib import Path

from frontier.cli import main
from frontier.hashing import ENTITY_HASH_KEY_ENV
from tests.conftest import FIXTURES, JAFFLE_SHOP

TEST_HASH_KEY = "test-only-frontier-entity-hash-key"
SHA256_OF_ONE = "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b"


def test_init_writes_into_project_dir(tmp_path: Path, capsys) -> None:
    project = tmp_path / "jaffle_shop"
    project.mkdir()
    assert main(["init", str(project)]) == 0
    config_path = project / "frontier.yml"
    assert config_path.is_file()
    out = capsys.readouterr().out
    assert str(config_path) in out
    assert "customer_summary" in config_path.read_text()


def test_init_and_inspect(dbt_project: Path, capsys) -> None:
    assert main(["inspect", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "customer_summary" in out
    assert "stg_customers" in out
    assert "stg_orders" in out
    assert "DATA_AGENT_DEV.DBT_DEV.customer_summary" in out
    assert "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER" in out
    assert "super-secret-password" not in out


def test_inspect_prints_both_artifact_fingerprints(dbt_project: Path, capsys) -> None:
    base = dbt_project / "target-base" / "manifest.json"
    base.parent.mkdir()
    base.write_text((dbt_project / "target" / "manifest.json").read_text())
    assert (
        main(
            [
                "inspect",
                "--project-dir",
                str(dbt_project),
                "--base-manifest",
                str(base),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Base artifact:" in out
    assert "PR artifact:" in out
    assert "Modified:" in out


def test_run_dry_run_writes_metrics(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
            "--run-id",
            "snowflake-demo-001",
        ]
    )
    assert code == 0
    out = capsys.readouterr().out
    assert "150000" in out or "150,000" in out or "Full entities: 150000" in out
    assert "Frontier: 3" in out
    assert "99.998%" in out
    assert "assert_frontier_events_resolve: passed" in out
    run_file = dbt_project / "target" / "frontier-run.json"
    payload = run_file.read_text()
    assert '"fullEntityCount": 150000' in payload
    assert '"frontierEntityCount": 3' in payload
    assert "370" in payload
    assert "password" not in payload


def test_prove_dry_run_records_mutation_metrics(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(
        [
            "prove",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
            "--run-id",
            "mutation-proof-001",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert "Full rows recomputed: 150000" in captured.out
    assert "Frontier rows recomputed: 3" in captured.out
    assert "Missing frontier entities: 0" in captured.out
    assert "Mismatched final rows: 0" in captured.out
    payload = (dbt_project / "target" / "frontier-run.json").read_text()
    parsed = json.loads(payload)
    assert parsed["metrics"]["fullRowsRecomputed"] == 150000
    assert parsed["metrics"]["mismatchedFinalRows"] == 0
    assert parsed["metrics"]["testDurationMs"] == 1
    names = {item["testName"] for item in parsed["validationResults"]}
    assert "assert_repaired_equals_reference" in names
    delete = next(event for event in parsed["changeEvents"] if event["eventId"] == "event_003")
    assert delete["entityValue"] == "5"
    assert delete["priorEntityValue"] == "781"


def _write_sql_change_manifests(project: Path) -> Path:
    pr_path = project / "target" / "manifest.json"
    payload = json.loads(pr_path.read_text())
    after_sql = (
        "select customer_id from DATA_AGENT_DEV.DBT_DEV.stg_orders "
        "where order_status in ('F', 'O')"
    )
    before_sql = (
        "select customer_id from DATA_AGENT_DEV.DBT_DEV.stg_orders "
        "where order_status = 'F'"
    )
    payload["nodes"]["model.jaffle_shop.int_customer_orders"]["compiled_code"] = after_sql
    pr_path.write_text(json.dumps(payload))
    base = json.loads(json.dumps(payload))
    base["nodes"]["model.jaffle_shop.int_customer_orders"]["compiled_code"] = before_sql
    base_path = project / "target-base" / "manifest.json"
    base_path.parent.mkdir(parents=True, exist_ok=True)
    base_path.write_text(json.dumps(base))
    events = project / "seeds" / "change_events.csv"
    if events.exists():
        events.unlink()
    return base_path


def test_prove_dry_run_sql_change_without_events(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    base_path = _write_sql_change_manifests(dbt_project)
    assert not (dbt_project / "seeds" / "change_events.csv").exists()
    code = main(
        [
            "prove",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
            "--run-id",
            "sql-change-proof-001",
            "--base-manifest",
            str(base_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert "SQL operator: FILTER_CHANGED" in captured.out
    assert "Changed source rows: 12" in captured.out
    assert "Candidate customers: 12" in captured.out
    assert "Event-derived candidates: 0" in captured.out
    assert "Confirmed changed summaries: 8" in captured.out
    assert "Row count: 150000 → 150000" in captured.out or "Row count: 150,000 → 150,000" in captured.out
    assert "Targeted repair: safe" in captured.out
    assert "Full backfill: not required" in captured.out
    assert "Impact compilation: COMPILED" in captured.out
    assert "Impact execution: NOT_EVALUATED" in captured.out
    assert "artifact comparison:" in captured.out
    assert "SQL-change candidates: 12" in captured.out
    assert "customer_summary_repaired" not in captured.out
    assert "frontier_affected_customers" not in captured.out
    payload = json.loads((dbt_project / "target" / "frontier-run.json").read_text())
    assert payload["changeEvents"] == []
    assert payload["runMode"] == "fixture"
    assert payload["candidateSetOrigin"] == "sql_change"
    assert payload["metrics"]["frontierEntityCount"] == 12
    assert payload["metrics"]["percentRowsAvoided"] == 99.992
    assert payload["metrics"]["candidateFrontierCount"] == 12
    assert payload["metrics"]["confirmedFrontierCount"] == 8
    assert payload["metrics"]["sourcePopulationCount"] == 12
    assert payload["metrics"]["changedSourceRowCount"] == 12
    assert payload["metrics"]["eventCandidateCount"] == 0
    assert payload["sqlComparison"]["modified"][0]["impactStatus"] == "COMPILED"
    assert payload["sqlComparison"]["modified"][0]["impactExecution"] == "NOT_EVALUATED"
    assert payload["sqlComparison"]["modified"][0]["name"] == "int_customer_orders"
    assert payload["sqlComparison"]["modified"][0]["changeKinds"] == ["FILTER_CHANGED"]
    assert "FILTER_CHANGED" in (payload["sqlComparison"]["modified"][0].get("changeSummary") or "")
    assert "candidateSql" not in payload["sqlComparison"]["modified"][0]
    names = {item["testName"] for item in payload["validationResults"]}
    assert "assert_sql_frontier_covers_reference" in names
    assert "assert_repaired_equals_reference" in names
    assert "assert_changed_customers_in_frontier" not in names
    values = {entity["entityValue"] for entity in payload["affectedEntities"]}
    assert values == {"4", "7", "9", "22", "31", "44", "73", "88"}


def test_run_requires_entity_hash_key(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(["run", "--project-dir", str(dbt_project), "--dry-run", "--run-id", "missing-key"])
    captured = capsys.readouterr()
    assert code == 1
    assert "FRONTIER_ENTITY_HASH_KEY" in captured.err
    assert TEST_HASH_KEY not in captured.out
    assert TEST_HASH_KEY not in captured.err
    assert not (dbt_project / "target" / "frontier-run.json").exists()


def test_run_hashes_entity_ids_with_env_key(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv(ENTITY_HASH_KEY_ENV, TEST_HASH_KEY)
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--run-id",
            "hmac-demo-001",
        ]
    )
    captured = capsys.readouterr()
    assert code == 0
    assert TEST_HASH_KEY not in captured.out
    assert TEST_HASH_KEY not in captured.err
    assert ENTITY_HASH_KEY_ENV not in captured.out
    run_file = dbt_project / "target" / "frontier-run.json"
    payload = run_file.read_text()
    parsed = json.loads(payload)
    values = [entity["entityValue"] for entity in parsed["affectedEntities"]]
    assert "370" not in values
    assert SHA256_OF_ONE not in payload
    assert TEST_HASH_KEY not in payload
    assert "Order 1 belongs to customer" not in payload


def test_upload_uses_run_file(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv(ENTITY_HASH_KEY_ENV, TEST_HASH_KEY)
    assert main(["run", "--project-dir", str(dbt_project), "--dry-run", "--run-id", "cli-upload-1"]) == 0

    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return b'{"id":"11111111-1111-4111-8111-111111111111","created":false,"externalRunId":"cli-upload-1"}'

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr("frontier.api.urllib.request.urlopen", lambda request, timeout=30: FakeResponse())
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key_not_a_password")
    assert main(["upload", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "cli-upload-1" in out
    assert "frn_test_key_not_a_password" not in out
    assert "FRONTIER_API_KEY" in out
    assert TEST_HASH_KEY not in out


def test_inspect_real_jaffle_shop(capsys) -> None:
    if not (JAFFLE_SHOP / "target" / "manifest.json").is_file():
        return
    assert main(
        [
            "inspect",
            "--project-dir",
            str(JAFFLE_SHOP),
            "--config",
            str(FIXTURES / "frontier.yml"),
        ]
    ) == 0
    out = capsys.readouterr().out
    assert "customer_summary" in out
    assert "stg_customers" in out
    assert "stg_orders" in out


class FakeUploadResponse:
    status = 201

    def read(self) -> bytes:
        return b'{"id":"11111111-1111-4111-8111-111111111111","created":true,"externalRunId":"ignored"}'

    def __enter__(self) -> FakeUploadResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_run_uses_commit_sha_and_git_context(dbt_project: Path, monkeypatch) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    monkeypatch.setenv("GITHUB_SHA", "abc1234deadbeef0123456789abcdef01234567")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/jaffle_shop")
    monkeypatch.setenv("GITHUB_HEAD_REF", "feat/orders")
    monkeypatch.setenv("FRONTIER_PULL_REQUEST", "42")
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_must_not_appear_in_payload")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "super-secret-password")
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
        ]
    )
    assert code == 0
    payload = json.loads((dbt_project / "target" / "frontier-run.json").read_text())
    assert payload["externalRunId"] == "jaffle_shop-abc1234deadbeef0123456789abcdef01234567"
    assert payload["git"]["repository"] == "acme/jaffle_shop"
    assert payload["git"]["branch"] == "feat/orders"
    assert payload["git"]["commitSha"] == "abc1234deadbeef0123456789abcdef01234567"
    assert payload["git"]["pullRequestNumber"] == 42
    dumped = json.dumps(payload)
    assert "ghs_must_not_appear_in_payload" not in dumped
    assert "super-secret-password" not in dumped
    assert "password" not in dumped


def test_run_writes_failed_validations_and_exits_zero(dbt_project: Path, monkeypatch) -> None:
    results = json.loads((dbt_project / "target" / "run_results.json").read_text())
    results["results"][0]["status"] = "fail"
    results["results"][0]["failures"] = 2
    (dbt_project / "target" / "run_results.json").write_text(json.dumps(results))
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
            "--run-id",
            "failed-assessment-001",
        ]
    )
    assert code == 0
    payload = json.loads((dbt_project / "target" / "frontier-run.json").read_text())
    assert payload["status"] == "failed"
    failed = [item for item in payload["validationResults"] if item["status"] == "failed"]
    assert failed
    assert payload["externalRunId"] == "failed-assessment-001"


def test_upload_blocking_exits_nonzero_after_upload(
    dbt_project: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    results = json.loads((dbt_project / "target" / "run_results.json").read_text())
    results["results"][0]["status"] = "fail"
    results["results"][0]["failures"] = 1
    (dbt_project / "target" / "run_results.json").write_text(json.dumps(results))
    assert (
        main(
            [
                "run",
                "--project-dir",
                str(dbt_project),
                "--dry-run",
                "--include-entity-ids",
                "--run-id",
                "blocking-fail-001",
            ]
        )
        == 0
    )
    monkeypatch.setattr("frontier.api.urllib.request.urlopen", lambda request, timeout=30: FakeUploadResponse())
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key_not_a_password")
    code = main(["upload", "--project-dir", str(dbt_project), "--blocking"])
    captured = capsys.readouterr()
    assert code == 1
    assert "blocking-fail-001" in captured.out
    assert "uploaded diagnostics" in captured.err


def test_upload_upserts_pr_comment_in_github_actions(
    dbt_project: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(ENTITY_HASH_KEY_ENV, TEST_HASH_KEY)
    assert main(["run", "--project-dir", str(dbt_project), "--dry-run", "--run-id", "pr-comment-1"]) == 0
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key_not_a_password")
    monkeypatch.setenv("FRONTIER_API_URL", "https://frontier.example")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("FRONTIER_DRY_RUN", "true")
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_must_not_appear")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/jaffle_shop")
    monkeypatch.setenv("FRONTIER_PULL_REQUEST", "42")

    comment_calls: list[str] = []

    class GitHubResponse:
        status = 200

        def __init__(self, payload: bytes) -> None:
            self._payload = payload

        def read(self) -> bytes:
            return self._payload

        def __enter__(self) -> GitHubResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def fake_urlopen(request, timeout=30):
        url = request.full_url
        if "api.github.com" in url:
            comment_calls.append(request.get_method())
            if request.get_method() == "GET":
                return GitHubResponse(b"[]")
            return GitHubResponse(b'{"id":1}')
        return FakeUploadResponse()

    monkeypatch.setattr("frontier.comment.urllib.request.urlopen", fake_urlopen)
    monkeypatch.setattr("frontier.api.urllib.request.urlopen", fake_urlopen)
    code = main(["upload", "--project-dir", str(dbt_project)])
    captured = capsys.readouterr()
    assert code == 0
    assert "Pull request comment created" in captured.out
    assert "ghs_must_not_appear" not in captured.out
    assert "ghs_must_not_appear" not in captured.err
    assert comment_calls == ["GET", "POST"]
    assert "Pull request comment created" in captured.out
    assert "ghs_must_not_appear" not in captured.out
    assert "ghs_must_not_appear" not in captured.err
    assert comment_calls == ["GET", "POST"]


def test_run_honors_dry_run_env(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    monkeypatch.setenv("FRONTIER_DRY_RUN", "true")
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--include-entity-ids",
            "--run-id",
            "env-dry-run-001",
        ]
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "No live warehouse session" in out
    assert (dbt_project / "target" / "frontier-run.json").is_file()


def test_record_failure_ignores_stale_run_results(dbt_project: Path, monkeypatch) -> None:
    monkeypatch.setenv(ENTITY_HASH_KEY_ENV, TEST_HASH_KEY)
    monkeypatch.setenv("GITHUB_SHA", "abc1234deadbeef0123456789abcdef01234567")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/jaffle_shop")
    monkeypatch.setenv("GITHUB_HEAD_REF", "feat/orders")
    code = main(
        [
            "record-failure",
            "--project-dir",
            str(dbt_project),
            "--reason",
            "dbt build failed; no current artifacts",
        ]
    )
    assert code == 0
    payload = json.loads((dbt_project / "target" / "frontier-run.json").read_text())
    assert payload["status"] == "failed"
    assert payload["evidenceLevel"] == "none"
    names = {item["testName"] for item in payload["validationResults"]}
    assert names == {"dbt_build"}
    assert "assert_frontier_events_resolve" not in names
    dumped = json.dumps(payload)
    assert "password" not in dumped
    assert TEST_HASH_KEY not in dumped


def test_run_refuses_stale_artifact_sha(dbt_project: Path, monkeypatch, capsys) -> None:
    from frontier.artifacts import write_artifact_sha

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "newsha1234567890abcdef")
    write_artifact_sha(dbt_project / "target", "oldsha1234567890abcdef")
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "stale" in captured.err.lower() or "not newsha" in captured.err


def test_run_refuses_missing_artifact_sha_in_ci(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "newsha1234567890abcdef")
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "frontier-artifact-sha" in captured.err


def _mini_manifest(
    *,
    orders_sql: str,
    include_legacy: bool = True,
) -> dict:
    nodes = {
        "model.jaffle_shop.stg_customers": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.stg_customers",
            "name": "stg_customers",
            "database": "DATA_AGENT_DEV",
            "schema": "DBT_DEV",
            "relation_name": "DATA_AGENT_DEV.DBT_DEV.stg_customers",
            "depends_on": {"nodes": []},
            "compiled_code": "select id as customer_id from customer",
            "original_file_path": "models/stg_customers.sql",
            "package_name": "jaffle_shop",
        },
        "model.jaffle_shop.stg_orders": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.stg_orders",
            "name": "stg_orders",
            "database": "DATA_AGENT_DEV",
            "schema": "DBT_DEV",
            "relation_name": "DATA_AGENT_DEV.DBT_DEV.stg_orders",
            "depends_on": {"nodes": []},
            "compiled_code": orders_sql,
            "original_file_path": "models/stg_orders.sql",
            "package_name": "jaffle_shop",
        },
        "model.jaffle_shop.customer_summary": {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.customer_summary",
            "name": "customer_summary",
            "database": "DATA_AGENT_DEV",
            "schema": "DBT_DEV",
            "relation_name": "DATA_AGENT_DEV.DBT_DEV.customer_summary",
            "depends_on": {"nodes": ["model.jaffle_shop.stg_orders"]},
            "compiled_code": "select customer_id from stg_orders",
            "original_file_path": "models/customer_summary.sql",
            "package_name": "jaffle_shop",
        },
    }
    if include_legacy:
        nodes["model.jaffle_shop.stg_legacy"] = {
            "resource_type": "model",
            "unique_id": "model.jaffle_shop.stg_legacy",
            "name": "stg_legacy",
            "database": "DATA_AGENT_DEV",
            "schema": "DBT_DEV",
            "relation_name": "DATA_AGENT_DEV.DBT_DEV.stg_legacy",
            "depends_on": {"nodes": []},
            "compiled_code": "select 1 as id",
            "original_file_path": "models/stg_legacy.sql",
            "package_name": "jaffle_shop",
        }
        nodes["model.jaffle_shop.customer_summary"]["depends_on"]["nodes"].append(
            "model.jaffle_shop.stg_legacy"
        )
    return {
        "metadata": {"project_name": "jaffle_shop", "adapter_type": "snowflake"},
        "nodes": nodes,
        "sources": {},
    }


def test_compare_cli_classifies_filter_and_removed(tmp_path: Path, capsys) -> None:
    base_path = tmp_path / "base-manifest.json"
    pr_path = tmp_path / "pr-manifest.json"
    out_path = tmp_path / "frontier-compare.json"
    (tmp_path / "frontier.yml").write_text(
        "\n".join(
            [
                "project: jaffle_shop",
                "environment: dev",
                "model:",
                "  name: customer_summary",
                "  entity: customer",
                "  key: customer_id",
                "  grain: one_row_per_customer",
                "relations:",
                "  stg_orders:",
                "    change_key: order_id",
                "    route: direct",
                "",
            ]
        )
    )
    base_sql = "select id as order_id from orders where status = 'complete'"
    pr_sql = "select id as order_id from orders where status = 'returned'"
    base_path.write_text(json.dumps(_mini_manifest(orders_sql=base_sql, include_legacy=True)))
    pr_path.write_text(json.dumps(_mini_manifest(orders_sql=pr_sql, include_legacy=False)))
    code = main(
        [
            "compare",
            "--project-dir",
            str(tmp_path),
            "--base-manifest",
            str(base_path),
            "--pr-manifest",
            str(pr_path),
            "--output",
            str(out_path),
        ]
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "Modified:" in out
    assert "stg_orders" in out
    assert "Removed:" in out
    assert "stg_legacy" in out
    assert "customer_summary" in out
    comparison = json.loads(out_path.read_text())
    assert comparison["base"]["fingerprint"] != comparison["pr"]["fingerprint"]
    assert comparison["modified"][0]["name"] == "stg_orders"
    assert comparison["modified"][0]["changeKinds"] == ["FILTER_CHANGED"]
    assert comparison["modified"][0]["unsafe"] is False
    assert comparison["modified"][0]["impactStatus"] == "COMPILED"
    assert "is distinct from" in (comparison["modified"][0].get("candidateSql") or "").lower()
    assert comparison["narrowFrontierSafe"] is True
    assert comparison["removed"][0]["name"] == "stg_legacy"
    assert comparison["removed"][0]["impactStatus"] == "FULL_REBUILD_REQUIRED"
    assert comparison["fullRebuildRequired"] is True
    assert "affectedEntities" not in comparison
    assert "FILTER_CHANGED" in out
    assert "COMPILED" in out
    assert "Narrow frontier safe: yes" in out


def test_run_attaches_both_artifact_fingerprints(dbt_project: Path, monkeypatch) -> None:
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    monkeypatch.setenv("FRONTIER_BASE_SHA", "base1234deadbeef")
    monkeypatch.setenv("GITHUB_SHA", "prsha1234deadbeef")
    base = dbt_project / "target-base" / "manifest.json"
    base.parent.mkdir()
    base.write_text((dbt_project / "target" / "manifest.json").read_text())
    code = main(
        [
            "run",
            "--project-dir",
            str(dbt_project),
            "--dry-run",
            "--include-entity-ids",
            "--run-id",
            "sql-compare-run-001",
            "--base-manifest",
            str(base),
        ]
    )
    assert code == 0
    payload = json.loads((dbt_project / "target" / "frontier-run.json").read_text())
    assert payload["sqlComparison"]["base"]["fingerprint"]
    assert payload["sqlComparison"]["pr"]["fingerprint"]
    assert payload["sqlComparison"]["base"]["fingerprint"] == payload["sqlComparison"]["pr"]["fingerprint"]
    assert payload["sqlComparison"]["base"]["commitSha"] == "base1234deadbeef"
    assert payload["sqlComparison"]["pr"]["commitSha"] == "prsha1234deadbeef"
    assert payload["runMode"] == "fixture"
    assert payload["sqlComparison"]["narrowFrontierSafe"] is True
    names = {item["testName"] for item in payload["validationResults"]}
    assert "assert_sql_change_allows_narrow_frontier" in names
    assert payload["status"] == "passed"
    assert len(payload["affectedEntities"]) == 3
    assert "password" not in json.dumps(payload)


def test_prove_dry_run_is_rejected_in_github_actions(dbt_project: Path, monkeypatch, capsys) -> None:
    sha = "abc1234deadbeef0123456789abcdef01234567"
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("FRONTIER_DRY_RUN", "true")
    monkeypatch.setenv("GITHUB_SHA", sha)
    monkeypatch.delenv(ENTITY_HASH_KEY_ENV, raising=False)
    base_path = _write_sql_change_manifests(dbt_project)
    (dbt_project / "target" / "frontier-artifact-sha").write_text(sha + "\n")
    code = main(
        [
            "prove",
            "--project-dir",
            str(dbt_project),
            "--base-manifest",
            str(base_path),
        ]
    )
    captured = capsys.readouterr()
    assert code == 1
    assert "FRONTIER_DRY_RUN is not allowed in GitHub Actions prove" in captured.err
    assert not (dbt_project / "target" / "frontier-run.json").exists()


