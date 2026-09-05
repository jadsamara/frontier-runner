from __future__ import annotations

from pathlib import Path

from frontier.cli import main
from tests.conftest import FIXTURES


def test_cdc_inspect_prints_streams_without_secrets(tmp_path: Path, capsys) -> None:
    project = tmp_path / "jaffle_shop"
    project.mkdir()
    (project / "frontier-cdc.yml").write_text((FIXTURES / "frontier-cdc.yml").read_text())
    code = main(["cdc", "inspect", "--project-dir", str(project), "--target", "dev"])
    captured = capsys.readouterr()
    assert code == 0
    assert "snowflake_stream" in captured.out
    assert "ORDERS_STREAM" in captured.out
    assert "CUSTOMER_STREAM" in captured.out
    assert "stg_orders" in captured.out
    assert "super-secret-password" not in captured.out
    assert "370" not in captured.out
    assert "customer_id=1" not in captured.out


def test_cdc_prove_cli_prints_aggregates_without_entity_ids(dbt_project: Path, monkeypatch, capsys) -> None:
    from frontier.cdc.prove import ProveResult
    from frontier.warehouse import FakeWarehouse

    (dbt_project / "frontier-cdc.yml").write_text((FIXTURES / "frontier-cdc.yml").read_text())
    monkeypatch.setattr("frontier.cli.connect_warehouse", lambda *args, **kwargs: FakeWarehouse())

    def fake_prove(**kwargs):
        assert kwargs.get("apply") is False
        return ProveResult(
            batch_id="cdc-test-batch",
            status="COMPLETED",
            logical_event_count=1,
            event_candidate_count=1,
            sql_change_candidate_count=0,
            union_candidate_count=1,
            confirmed_change_count=0,
            no_op_count=1,
            missed_event_count=0,
            validation="passed",
            duration_ms=12,
            evidence=("event_routing_validated", "targeted_rows_compared"),
        )

    monkeypatch.setattr("frontier.cli.prove_batch", fake_prove)
    monkeypatch.setattr(
        "frontier.cli.load_change_events_csv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("csv")),
    )
    code = main(["cdc", "prove", "--project-dir", str(dbt_project), "--target", "dev"])
    captured = capsys.readouterr()
    assert code == 0
    assert "logical events: 1" in captured.out
    assert "event-derived candidates: 1" in captured.out
    assert "SQL-change candidates: 0" in captured.out
    assert "union candidates: 1" in captured.out
    assert "confirmed changes: 0" in captured.out
    assert "candidate no-ops: 1" in captured.out
    assert "missed events: 0" in captured.out
    assert "validation: passed" in captured.out
    assert "batch: COMPLETED" in captured.out
    assert "370" not in captured.out
    assert "password" not in captured.out


def test_cdc_upload_cli_prints_idempotent_result(dbt_project: Path, monkeypatch, capsys) -> None:
    from frontier.warehouse import FakeWarehouse

    (dbt_project / "frontier-cdc.yml").write_text((FIXTURES / "frontier-cdc.yml").read_text())
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.setattr("frontier.cli.connect_warehouse", lambda *args, **kwargs: FakeWarehouse())

    def fake_upload(**kwargs):
        assert kwargs.get("batch_id") is None
        return {
            "httpStatus": 201,
            "created": True,
            "id": "11111111-1111-4111-8111-111111111111",
            "batchId": "cdc-test-batch",
            "uploadStatus": "UPLOADED",
            "assessmentType": "cdc",
        }

    monkeypatch.setattr("frontier.cli.upload_cdc_batch", fake_upload)
    code = main(["cdc", "upload", "--project-dir", str(dbt_project), "--target", "dev"])
    captured = capsys.readouterr()
    assert code == 0
    assert "HTTP 201" in captured.out
    assert "created: true" in captured.out
    assert "assessment type: cdc" in captured.out
    assert "batch upload status: uploaded" in captured.out
    assert "frn_test_key" not in captured.out
    assert "370" not in captured.out
