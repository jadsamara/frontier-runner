from __future__ import annotations

from pathlib import Path

import pytest

from frontier.artifacts import require_current_artifacts, write_artifact_sha
from frontier.config import ConfigError


def test_require_current_artifacts_skips_outside_ci(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("FRONTIER_REQUIRE_ARTIFACT_SHA", raising=False)
    monkeypatch.setenv("GITHUB_SHA", "abc1234")
    assert require_current_artifacts(tmp_path / "target") is None


def test_require_current_artifacts_rejects_mismatch(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "target"
    write_artifact_sha(target, "oldsha")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "newsha")
    with pytest.raises(ConfigError, match="stale"):
        require_current_artifacts(target)


def test_require_current_artifacts_accepts_matching_sha(tmp_path: Path, monkeypatch) -> None:
    target = tmp_path / "target"
    write_artifact_sha(target, "abc1234deadbeef")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "abc1234deadbeef")
    assert require_current_artifacts(target) == "abc1234deadbeef"
