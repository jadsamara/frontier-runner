from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

from frontier.cli import main
from frontier.config import load_frontier_config
from frontier.semantic import (
    ManifestError,
    apply_pinned_manifest,
    fingerprint_document,
    local_override_from_config,
    pin_manifest,
    pinned_from_payload,
    validate_pinned_against_dbt,
    validate_pinned_document,
)
from tests.conftest import FIXTURES


JAFFLE_DOCUMENT = {
    "model": "customer_summary",
    "entity": "customer",
    "entityKey": "customer_id",
    "grain": "one_row_per_customer",
    "sources": [
        {
            "name": "stg_customers",
            "changeKey": "customer_id",
            "joinRoute": "direct",
            "mutationPolicy": "targeted_repair",
            "deletesRequireBeforeImage": False,
            "temporalMode": "none",
            "eventTimeColumn": None,
            "maximumLateness": None,
            "confidence": "high",
            "origin": "confirmed",
        },
        {
            "name": "stg_orders",
            "changeKey": "order_id",
            "joinRoute": "order_id -> customer_id",
            "mutationPolicy": "targeted_repair",
            "deletesRequireBeforeImage": True,
            "temporalMode": "none",
            "eventTimeColumn": None,
            "maximumLateness": None,
            "confidence": "high",
            "origin": "confirmed",
        },
    ],
}


def _active_payload(project: str = "jaffle_shop") -> dict:
    fingerprint = fingerprint_document(JAFFLE_DOCUMENT)
    return {
        "id": "11111111-1111-4111-8111-111111111111",
        "version": 4,
        "fingerprint": fingerprint,
        "project": project,
        "status": "active",
        "activatedAt": "2026-09-05T12:00:00.000Z",
        "targetModelUniqueId": f"model.{project}.customer_summary",
        "targetModel": JAFFLE_DOCUMENT["model"],
        "targetEntityType": JAFFLE_DOCUMENT["entity"],
        "entityKey": JAFFLE_DOCUMENT["entityKey"],
        "grain": JAFFLE_DOCUMENT["grain"],
        "sources": JAFFLE_DOCUMENT["sources"],
    }


class _FakeSaas(BaseHTTPRequestHandler):
    payload = _active_payload()
    status = 200
    seen_auth: str | None = None

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802
        type(self).seen_auth = self.headers.get("Authorization")
        body = json.dumps(self.payload).encode("utf-8")
        self.send_response(self.status)
        if self.status == 200:
            self.send_header("ETag", f'"{self.payload["fingerprint"]}"')
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"error":"No active semantic manifest","code":"MANIFEST_NOT_ACTIVE"}')


@pytest.fixture
def fake_saas():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeSaas)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}"
    server.shutdown()
    thread.join(timeout=2)


def test_inspect_uses_local_override_without_network(dbt_project: Path, capsys) -> None:
    assert main(["inspect", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "source: local_override" in out
    assert "target model: customer_summary" in out
    assert "route count: 2" in out


def test_ci_does_not_fall_back_to_local(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    (dbt_project / "target" / "frontier-artifact-sha").write_text("abc123\n")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    monkeypatch.delenv("FRONTIER_API_KEY", raising=False)
    monkeypatch.delenv("FRONTIER_DEMO_API_KEY", raising=False)
    assert main(["inspect", "--project-dir", str(dbt_project)]) == 1
    err = capsys.readouterr().err
    assert "MANIFEST_REQUIRED" in err


def test_manifest_file_pins_and_skips_saas(dbt_project: Path, capsys) -> None:
    config = load_frontier_config(dbt_project / "frontier.yml")
    pinned = local_override_from_config(config)
    path = dbt_project / "target" / "frontier-manifest.json"
    pin_manifest(path, pinned)
    assert (
        main(
            [
                "inspect",
                "--project-dir",
                str(dbt_project),
                "--manifest-file",
                str(path),
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "source: pinned_file" in out
    assert "fingerprint:" in out


def test_fetch_from_fake_saas(dbt_project: Path, fake_saas: str, monkeypatch, capsys) -> None:
    monkeypatch.setenv("FRONTIER_API_URL", fake_saas)
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "source: saas_active" in out
    assert "version: 4" in out
    pin = json.loads((dbt_project / "target" / "frontier-manifest.json").read_text())
    assert pin["fingerprint"] == fingerprint_document(JAFFLE_DOCUMENT)
    assert pin["version"] == 4
    assert _FakeSaas.seen_auth == "Bearer frn_test_key"


def test_saas_fetch_without_root_frontier_yml(
    dbt_project: Path,
    fake_saas: str,
    monkeypatch,
    capsys,
) -> None:
    (dbt_project / "frontier.yml").unlink()
    from frontier.local_config import LocalFrontierConfig, write_local_config

    write_local_config(
        dbt_project,
        LocalFrontierConfig(project="jaffle_shop", api_url=fake_saas),
        force=True,
    )
    monkeypatch.setenv("FRONTIER_API_URL", fake_saas)
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "source: saas_active" in out
    assert "version: 4" in out
    pin = json.loads((dbt_project / "target" / "frontier-manifest.json").read_text())
    assert pin["fingerprint"] == fingerprint_document(JAFFLE_DOCUMENT)
    assert pin["version"] == 4
    assert not (dbt_project / "frontier.yml").exists()


def test_local_manifest_mode_requires_frontier_yml(
    dbt_project: Path, monkeypatch, capsys
) -> None:
    (dbt_project / "frontier.yml").unlink()
    monkeypatch.delenv("FRONTIER_API_KEY", raising=False)
    monkeypatch.setenv("FRONTIER_ALLOW_LOCAL_MANIFEST", "1")
    assert main(["inspect", "--allow-local-manifest", "--project-dir", str(dbt_project)]) == 1
    assert "Missing Frontier config" in capsys.readouterr().err


def test_inspect_uses_fetched_saas_manifest(
    dbt_project: Path,
    fake_saas: str,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("FRONTIER_API_URL", fake_saas)
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    assert main(["inspect", "--project-dir", str(dbt_project)]) == 0
    out = capsys.readouterr().out
    assert "source: saas_active" in out
    assert "version: 4" in out


def test_conflict_fails_closed(dbt_project: Path) -> None:
    config = load_frontier_config(dbt_project / "frontier.yml")
    pinned = pinned_from_payload(
        {
            **_active_payload(),
            "document": {**JAFFLE_DOCUMENT, "model": "orders_summary"},
            "targetModel": "orders_summary",
            "fingerprint": fingerprint_document({**JAFFLE_DOCUMENT, "model": "orders_summary"}),
        },
        source="saas_active",
    )
    with pytest.raises(ManifestError, match="MANIFEST_LOCAL_REMOTE_CONFLICT"):
        apply_pinned_manifest(config, pinned)


def test_fingerprint_mismatch_on_pin_file(tmp_path: Path) -> None:
    payload = _active_payload()
    payload["fingerprint"] = "ff" * 32
    path = tmp_path / "frontier-manifest.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ManifestError, match="MANIFEST_FINGERPRINT_MISMATCH"):
        pinned_from_payload(json.loads(path.read_text()), source="pinned_file")


def test_jaffle_fingerprint_matches_saas_canonical_json() -> None:
    digest = fingerprint_document(JAFFLE_DOCUMENT)
    assert len(digest) == 64
    assert digest == fingerprint_document(dict(reversed(list(JAFFLE_DOCUMENT.items()))))


def test_draft_manifest_fails_closed() -> None:
    payload = {**_active_payload(), "status": "draft"}
    pinned = pinned_from_payload(payload, source="saas_active")
    with pytest.raises(ManifestError, match="MANIFEST_IS_DRAFT"):
        validate_pinned_document(pinned)


def test_low_confidence_route_fails_closed() -> None:
    document = {
        **JAFFLE_DOCUMENT,
        "sources": [
            JAFFLE_DOCUMENT["sources"][0],
            {**JAFFLE_DOCUMENT["sources"][1], "confidence": "low"},
        ],
    }
    pinned = pinned_from_payload(
        {**_active_payload(), "document": document, "fingerprint": fingerprint_document(document)},
        source="saas_active",
    )
    with pytest.raises(ManifestError, match="MANIFEST_LOW_CONFIDENCE"):
        validate_pinned_document(pinned)


def test_unconfirmed_inferred_route_fails_closed() -> None:
    document = {
        **JAFFLE_DOCUMENT,
        "sources": [
            JAFFLE_DOCUMENT["sources"][0],
            {**JAFFLE_DOCUMENT["sources"][1], "origin": "inferred"},
        ],
    }
    pinned = pinned_from_payload(
        {**_active_payload(), "document": document, "fingerprint": fingerprint_document(document)},
        source="saas_active",
    )
    with pytest.raises(ManifestError, match="MANIFEST_ROUTE_UNCONFIRMED"):
        validate_pinned_document(pinned)


def test_incomplete_temporal_policy_fails_closed() -> None:
    document = {
        **JAFFLE_DOCUMENT,
        "sources": [
            {
                **JAFFLE_DOCUMENT["sources"][0],
                "temporalMode": "event_time",
                "eventTimeColumn": None,
            },
            JAFFLE_DOCUMENT["sources"][1],
        ],
    }
    pinned = pinned_from_payload(
        {**_active_payload(), "document": document, "fingerprint": fingerprint_document(document)},
        source="saas_active",
    )
    with pytest.raises(ManifestError, match="MANIFEST_TEMPORAL_INCOMPLETE"):
        validate_pinned_document(pinned)


def test_missing_target_model_fails_closed() -> None:
    from frontier.dbt_artifacts import Manifest

    pinned = pinned_from_payload(_active_payload(), source="saas_active")
    manifest = Manifest(project_name="jaffle_shop", adapter_type="snowflake", nodes={}, sources={})
    with pytest.raises(ManifestError, match="MANIFEST_TARGET_MODEL_MISSING"):
        validate_pinned_against_dbt(pinned, manifest)


def test_entity_key_absent_from_compiled_sql_fails_closed() -> None:
    from frontier.dbt_artifacts import DbtNode, Manifest

    pinned = pinned_from_payload(_active_payload(), source="saas_active")
    node = DbtNode(
        unique_id="model.jaffle_shop.customer_summary",
        name="customer_summary",
        resource_type="model",
        database="DB",
        schema="SC",
        relation_name="DB.SC.customer_summary",
        depends_on=(),
        compiled_code="select order_id, count(*) as total from orders group by order_id",
    )
    sources = [
        DbtNode(
            unique_id=f"model.jaffle_shop.{name}",
            name=name,
            resource_type="model",
            database="DB",
            schema="SC",
            relation_name=f"DB.SC.{name}",
            depends_on=(),
        )
        for name in ("stg_customers", "stg_orders")
    ]
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={item.unique_id: item for item in (node, *sources)},
        sources={},
    )
    with pytest.raises(ManifestError, match="MANIFEST_ENTITY_KEY_ABSENT"):
        validate_pinned_against_dbt(pinned, manifest)


def test_incompatible_grain_fails_closed() -> None:
    from frontier.dbt_artifacts import DbtNode, Manifest

    pinned = pinned_from_payload(_active_payload(), source="saas_active")
    node = DbtNode(
        unique_id="model.jaffle_shop.customer_summary",
        name="customer_summary",
        resource_type="model",
        database="DB",
        schema="SC",
        relation_name="DB.SC.customer_summary",
        depends_on=(),
        compiled_code="select customer_id, order_id, count(*) as total from orders group by order_id",
    )
    sources = [
        DbtNode(
            unique_id=f"model.jaffle_shop.{name}",
            name=name,
            resource_type="model",
            database="DB",
            schema="SC",
            relation_name=f"DB.SC.{name}",
            depends_on=(),
        )
        for name in ("stg_customers", "stg_orders")
    ]
    manifest = Manifest(
        project_name="jaffle_shop",
        adapter_type="snowflake",
        nodes={item.unique_id: item for item in (node, *sources)},
        sources={},
    )
    with pytest.raises(ManifestError, match="MANIFEST_GRAIN_INCOMPATIBLE"):
        validate_pinned_against_dbt(pinned, manifest)


def test_revoked_api_key_fails_closed(dbt_project: Path, fake_saas: str, monkeypatch, capsys) -> None:
    _FakeSaas.status = 401
    monkeypatch.setenv("FRONTIER_API_URL", fake_saas)
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_revoked")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    try:
        assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 1
        assert "MANIFEST_API_KEY_REVOKED" in capsys.readouterr().err
    finally:
        _FakeSaas.status = 200


def test_missing_active_manifest_fails_closed(dbt_project: Path, fake_saas: str, monkeypatch, capsys) -> None:
    _FakeSaas.status = 404
    monkeypatch.setenv("FRONTIER_API_URL", fake_saas)
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    try:
        assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 1
        assert "MANIFEST_NOT_ACTIVE" in capsys.readouterr().err
    finally:
        _FakeSaas.status = 200


def test_cross_project_manifest_is_not_found(dbt_project: Path, fake_saas: str, monkeypatch, capsys) -> None:
    class _Hidden(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            del format, args

        def do_GET(self) -> None:  # noqa: N802
            body = b'{"error":"Not found"}'
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Hidden)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    monkeypatch.setenv("FRONTIER_API_URL", f"http://{host}:{port}")
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_other_project")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    try:
        assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 1
        err = capsys.readouterr().err
        assert "MANIFEST_NOT_FOUND" in err
        assert "MANIFEST_NOT_ACTIVE" not in err
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_saas_unavailable_fails_closed(dbt_project: Path, monkeypatch, capsys) -> None:
    monkeypatch.setenv("FRONTIER_API_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("FRONTIER_API_KEY", "frn_test_key")
    monkeypatch.delenv("FRONTIER_ALLOW_LOCAL_MANIFEST", raising=False)
    assert main(["manifest", "fetch", "--project-dir", str(dbt_project)]) == 1
    assert "MANIFEST_SAAS_UNAVAILABLE" in capsys.readouterr().err


def test_tampered_pin_fingerprint_fails_closed(dbt_project: Path, capsys) -> None:
    payload = _active_payload()
    payload["fingerprint"] = "ab" * 32
    path = dbt_project / "target" / "frontier-manifest.json"
    path.write_text(json.dumps(payload))
    assert (
        main(
            [
                "inspect",
                "--project-dir",
                str(dbt_project),
                "--manifest-file",
                str(path),
            ]
        )
        == 1
    )
    assert "MANIFEST_FINGERPRINT_MISMATCH" in capsys.readouterr().err


def test_init_template_is_the_only_jaffle_default() -> None:
    assert (FIXTURES / "frontier.yml").read_text().count("customer_summary") >= 1
