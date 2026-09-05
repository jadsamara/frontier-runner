from __future__ import annotations

from pathlib import Path

import pytest

from frontier.cdc.config import load_cdc_config
from frontier.cdc.consume import consume_all
from frontier.cdc.prove import prove_batch
from frontier.cdc.store import CdcBatch, CdcProofRecord, FakeCdcStore
from frontier.cdc.upload import (
    BASELINE_STALE,
    assert_baseline_fresh,
    build_cdc_ingest_payload,
    cdc_external_run_id,
    select_upload_batch,
    upload_cdc_batch,
)
from frontier.config import ConfigError, load_frontier_config
from frontier.warehouse import FakeWarehouse
from tests.conftest import FIXTURES
from tests.test_cdc_prove import _captured, _manifest, _prove, _warehouse

ORDERS = "DATA_AGENT_DEV.FRONTIER_CDC.ORDERS_STREAM"
CUSTOMERS = "DATA_AGENT_DEV.FRONTIER_CDC.CUSTOMER_STREAM"
FINGERPRINT = "ab" * 32


def _proof(**kwargs) -> CdcProofRecord:
    values = dict(
        batch_id="cdc-batch",
        candidate_count=1,
        sql_change_candidate_count=0,
        union_candidate_count=1,
        confirmed_change_count=1,
        no_op_count=0,
        missed_event_count=0,
        validation_status="passed",
        event_routing_validated=True,
        targeted_rows_compared=True,
        whole_table_reference_executed=False,
        repair_applied=False,
    )
    values.update(kwargs)
    return CdcProofRecord(**values)


def _completed_store(tmp_path: Path) -> FakeCdcStore:
    store = _captured()
    _prove(store, _warehouse(confirmed=1, targeted=1), tmp_path)
    return store


def test_external_run_id_uses_project_and_fingerprint() -> None:
    assert cdc_external_run_id("jaffle_shop", FINGERPRINT) == f"jaffle_shop-cdc-{FINGERPRINT}"


def test_select_newest_completed_not_uploaded(tmp_path: Path) -> None:
    store = _completed_store(tmp_path)
    older = store.batches[0]
    newer = CdcBatch(
        batch_id="cdc-newer",
        project_name="jaffle_shop",
        stream_relation=ORDERS,
        status="COMPLETED",
        payload_fingerprint="cd" * 32,
        raw_record_count=0,
        logical_event_count=0,
    )
    store.insert_batch(newer)
    store.persist_proof(newer.batch_id, _proof(batch_id=newer.batch_id, confirmed_change_count=0, candidate_count=0, union_candidate_count=0))
    older.upload_status = "UPLOADED"
    selected = select_upload_batch(store, "jaffle_shop")
    assert selected.batch_id == "cdc-newer"


def test_explicit_batch_selection(tmp_path: Path) -> None:
    store = _completed_store(tmp_path)
    batch_id = store.batches[0].batch_id
    selected = select_upload_batch(store, "jaffle_shop", batch_id=batch_id)
    assert selected.batch_id == batch_id


def test_rejects_non_completed_successful_assessment(tmp_path: Path) -> None:
    store = _captured()
    config = load_cdc_config(FIXTURES / "frontier-cdc.yml")
    frontier = load_frontier_config(FIXTURES / "frontier.yml")
    with pytest.raises(ConfigError, match="CAPTURED or PROCESSING"):
        build_cdc_ingest_payload(
            batch=store.batches[0],
            proof=None,
            events=store.list_events(store.batches[0].batch_id),
            config=frontier,
            cdc_config=config,
            manifest=_manifest(),
            full_entity_count=150000,
        )


def test_successful_payload_omits_entity_ids(tmp_path: Path) -> None:
    store = _completed_store(tmp_path)
    batch = store.batches[0]
    payload = build_cdc_ingest_payload(
        batch=batch,
        proof=store.get_proof(batch.batch_id),
        events=store.list_events(batch.batch_id),
        config=load_frontier_config(FIXTURES / "frontier.yml"),
        cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
        manifest=_manifest(),
        full_entity_count=150000,
    )
    dumped = str(payload)
    assert payload["assessmentType"] == "cdc"
    assert payload["runMode"] == "live"
    assert payload["candidateSetOrigin"] == "event"
    assert payload["changeEvents"] == []
    assert payload["affectedEntities"] == []
    assert payload["cdc"]["logicalEventCount"] == 1
    assert payload["cdc"]["repairApplied"] is False
    assert payload["cdc"]["fullReferenceValidation"] == "NOT_RUN"
    assert payload["cdc"]["targetedValidation"] == "PASSED"
    assert payload["metrics"]["confirmedEntityCount"] == 1
    assert "370" not in dumped
    assert "O_ORDERKEY" not in dumped
    assert "METADATA$ROW_ID" not in dumped
    assert payload["externalRunId"] == cdc_external_run_id("jaffle_shop", batch.payload_fingerprint or "")


def test_zero_confirmed_one_noop_payload() -> None:
    store = FakeCdcStore()
    batch = CdcBatch(
        batch_id="cdc-noop",
        project_name="jaffle_shop",
        stream_relation=ORDERS,
        status="COMPLETED",
        payload_fingerprint=FINGERPRINT,
        raw_record_count=2,
        logical_event_count=1,
        completed_at="2026-09-05T15:01:00+00:00",
    )
    store.insert_batch(batch)
    proof = _proof(confirmed_change_count=0, no_op_count=1)
    payload = build_cdc_ingest_payload(
        batch=batch,
        proof=proof,
        events=[],
        config=load_frontier_config(FIXTURES / "frontier.yml"),
        cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
        manifest=_manifest(),
        full_entity_count=150000,
    )
    assert payload["status"] == "passed"
    assert payload["metrics"]["confirmedEntityCount"] == 0
    assert payload["metrics"]["candidateNoOpCount"] == 1


def test_upload_success_and_retry(tmp_path: Path, monkeypatch) -> None:
    store = _completed_store(tmp_path)
    calls: list[dict] = []

    def fake_upload(payload, **kwargs):
        calls.append(payload)
        created = len(calls) == 1
        return {
            "_httpStatus": 201 if created else 200,
            "id": "11111111-1111-4111-8111-111111111111",
            "externalRunId": payload["externalRunId"],
            "created": created,
        }

    monkeypatch.setattr("frontier.cdc.upload.upload_run", fake_upload)
    first = upload_cdc_batch(
        store=store,
        warehouse=FakeWarehouse(),
        cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
        frontier_config=load_frontier_config(FIXTURES / "frontier.yml"),
        manifest=_manifest(),
        project_name="jaffle_shop",
        api_url="http://127.0.0.1:3000",
        api_key="frn_test",
    )
    assert first["httpStatus"] == 201
    assert first["created"] is True
    assert store.batches[0].upload_status == "UPLOADED"
    second = upload_cdc_batch(
        store=store,
        warehouse=FakeWarehouse(),
        cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
        frontier_config=load_frontier_config(FIXTURES / "frontier.yml"),
        manifest=_manifest(),
        project_name="jaffle_shop",
        api_url="http://127.0.0.1:3000",
        api_key="frn_test",
        batch_id=store.batches[0].batch_id,
    )
    assert second["httpStatus"] == 200
    assert second["created"] is False
    assert store.batches[0].upload_attempt_count == 2
    assert len(calls) == 2
    assert calls[0]["externalRunId"] == calls[1]["externalRunId"]


def test_upload_failure_then_retry(tmp_path: Path, monkeypatch) -> None:
    store = _completed_store(tmp_path)
    attempts = {"n": 0}

    def fake_upload(payload, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConfigError("Upload failed with HTTP 503: unavailable")
        return {
            "_httpStatus": 201,
            "id": "11111111-1111-4111-8111-111111111111",
            "externalRunId": payload["externalRunId"],
            "created": True,
        }

    monkeypatch.setattr("frontier.cdc.upload.upload_run", fake_upload)
    with pytest.raises(ConfigError, match="HTTP 503"):
        upload_cdc_batch(
            store=store,
            warehouse=FakeWarehouse(),
            cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
            frontier_config=load_frontier_config(FIXTURES / "frontier.yml"),
            manifest=_manifest(),
            project_name="jaffle_shop",
            api_url="http://127.0.0.1:3000",
            api_key="frn_test",
        )
    assert store.batches[0].status == "COMPLETED"
    assert store.batches[0].upload_status == "FAILED"
    assert store.batches[0].last_upload_error_code == "HTTP_503"
    retry = upload_cdc_batch(
        store=store,
        warehouse=FakeWarehouse(),
        cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
        frontier_config=load_frontier_config(FIXTURES / "frontier.yml"),
        manifest=_manifest(),
        project_name="jaffle_shop",
        api_url="http://127.0.0.1:3000",
        api_key="frn_test",
    )
    assert retry["created"] is True
    assert store.batches[0].upload_status == "UPLOADED"


def test_stale_baseline_blocks_later_proof(tmp_path: Path) -> None:
    store = _completed_store(tmp_path)
    with pytest.raises(ConfigError, match="BASELINE_STALE"):
        assert_baseline_fresh(store, "jaffle_shop", ignoring_batch_id="other")
    second = FakeCdcStore(
        {
            ORDERS: [
                {
                    "action": "INSERT",
                    "is_update": False,
                    "row_id": "ins-2",
                    "primary_key": "2",
                    "target_key": "4",
                }
            ],
            CUSTOMERS: [],
        }
    )
    second.batches = list(store.batches)
    second.events = list(store.events)
    second.proofs = dict(store.proofs)
    consume_all(load_cdc_config(FIXTURES / "frontier-cdc.yml"), store=second, project_name="jaffle_shop")
    with pytest.raises(ConfigError, match=BASELINE_STALE):
        prove_batch(
            store=second,
            warehouse=_warehouse(),
            cdc_config=load_cdc_config(FIXTURES / "frontier-cdc.yml"),
            frontier_config=load_frontier_config(FIXTURES / "frontier.yml"),
            manifest=_manifest(),
            compiled_root=None,
            project_name="jaffle_shop",
            output_dir=tmp_path,
        )
    later = [batch for batch in second.batches if batch.status == "CAPTURED"]
    assert later
    assert later[0].status == "CAPTURED"
