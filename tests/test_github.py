from __future__ import annotations

from pathlib import Path

import pytest

from frontier.github import default_external_run_id, env_flag, github_source, pull_request_number

RUNNER_ROOT = Path(__file__).resolve().parents[1]
SAAS_ROOT = RUNNER_ROOT.parent
WORKFLOW = SAAS_ROOT / ".github" / "workflows" / "frontier.yml"
REFERENCE_WORKFLOW = SAAS_ROOT / "docs" / "data-agent-pipeline-workflow.yml"


def test_github_source_captures_pr_context(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "abc1234deadbeef0123456789abcdef01234567")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/jaffle_shop")
    monkeypatch.setenv("GITHUB_HEAD_REF", "feat/orders")
    monkeypatch.setenv("GITHUB_REF_NAME", "42/merge")
    monkeypatch.setenv("GITHUB_REF", "refs/pull/42/merge")
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_should_never_leave_the_runner")
    monkeypatch.delenv("FRONTIER_PULL_REQUEST", raising=False)
    monkeypatch.delenv("GITHUB_PR_NUMBER", raising=False)

    source = github_source()
    assert source == {
        "repository": "acme/jaffle_shop",
        "branch": "feat/orders",
        "commitSha": "abc1234deadbeef0123456789abcdef01234567",
        "pullRequestNumber": 42,
    }
    dumped = str(source)
    assert "ghs_" not in dumped
    assert "token" not in dumped.lower()


def test_pull_request_prefers_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("FRONTIER_PULL_REQUEST", "99")
    monkeypatch.setenv("GITHUB_REF", "refs/pull/42/merge")
    assert pull_request_number() == 99


def test_run_id_uses_commit_sha(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert default_external_run_id("jaffle_shop") == "jaffle_shop-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    monkeypatch.setenv("GITHUB_SHA", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
    assert default_external_run_id("jaffle_shop") == "jaffle_shop-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def test_run_id_is_stable_for_the_same_sha(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", "cccccccccccccccccccccccccccccccccccccccc")
    first = default_external_run_id("jaffle_shop")
    second = default_external_run_id("jaffle_shop")
    assert first == second


def test_env_flag(monkeypatch) -> None:
    monkeypatch.delenv("FRONTIER_DRY_RUN", raising=False)
    assert env_flag("FRONTIER_DRY_RUN") is False
    monkeypatch.setenv("FRONTIER_DRY_RUN", "true")
    assert env_flag("FRONTIER_DRY_RUN") is True


def test_workflow_contains_required_steps() -> None:
    if not WORKFLOW.is_file():
        pytest.skip("SaaS workflow is not present in a standalone runner checkout")
    text = WORKFLOW.read_text()
    assert "pip install ./runner" in text
    assert "frontier inspect" in text
    assert "frontier run" in text
    assert "frontier record-failure" in text
    assert "frontier upload" in text
    assert "FRONTIER_DRY_RUN" in text
    assert "rm -rf ci-dbt" in text
    assert "frontier-artifact-sha" in text
    assert "FRONTIER_API_URL: ${{ secrets.FRONTIER_API_URL }}" in text
    assert "FRONTIER_API_KEY: ${{ secrets.FRONTIER_API_KEY }}" in text
    assert "if: always() && !cancelled()" in text
    assert "continue-on-error: true" in text
    assert "FRONTIER_BLOCKING" in text
    assert "pull-requests: write" in text
    assert "github.token" in text
    assert "frontier prove" not in text
    assert "SNOWFLAKE_PASSWORD" not in text
    assert "SNOWFLAKE_ACCOUNT" not in text


def test_reference_customer_workflow_uses_prove_and_pinned_runner() -> None:
    if not REFERENCE_WORKFLOW.is_file():
        pytest.skip("SaaS docs are not present in a standalone runner checkout")
    text = REFERENCE_WORKFLOW.read_text()
    assert "frontier inspect" in text
    assert "frontier prove" in text
    assert "frontier upload" in text
    assert "frontier record-failure" in text
    assert "rm -rf target" in text
    assert "frontier-artifact-sha" in text
    assert "frontier-runner[snowflake]==0.1.0" in text
    assert "pip install ./runner" not in text
    assert "FRONTIER_BLOCKING" in text
    assert "FRONTIER_DRY_RUN" not in text
    assert "pull-requests: write" in text
    assert "github.token" in text
    generate = text.split("- name: Generate impact assessment", 1)[1]
    assert "SNOWFLAKE_PASSWORD" not in generate
    assert "https://" in text

