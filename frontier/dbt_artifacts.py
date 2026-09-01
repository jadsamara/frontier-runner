from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from frontier.config import ConfigError


@dataclass(frozen=True)
class DbtNode:
    unique_id: str
    name: str
    resource_type: str
    database: str | None
    schema: str | None
    relation_name: str | None
    depends_on: tuple[str, ...]
    original_file_path: str | None = None
    tags: tuple[str, ...] = ()
    source_name: str | None = None
    compiled_code: str | None = None
    package_name: str | None = None

    @property
    def relation(self) -> str:
        if self.relation_name:
            return self.relation_name
        parts = [part for part in (self.database, self.schema, self.name) if part]
        if not parts:
            raise ConfigError(f"Node {self.unique_id} has no relation name")
        return ".".join(parts)


@dataclass
class Manifest:
    project_name: str
    adapter_type: str | None
    nodes: dict[str, DbtNode]
    sources: dict[str, DbtNode]
    path: Path | None = None

    def get(self, unique_id: str) -> DbtNode | None:
        return self.nodes.get(unique_id) or self.sources.get(unique_id)

    def find_model(self, name: str) -> DbtNode:
        matches = [
            node
            for node in self.nodes.values()
            if node.resource_type == "model" and node.name == name
        ]
        if not matches:
            raise ConfigError(f"Model '{name}' was not found in the dbt manifest")
        if len(matches) > 1:
            ids = ", ".join(node.unique_id for node in matches)
            raise ConfigError(f"Model name '{name}' is ambiguous: {ids}")
        return matches[0]

    def find_by_name(self, name: str, resource_types: Iterable[str] | None = None) -> DbtNode:
        allowed = set(resource_types) if resource_types else None
        matches = [
            node
            for node in list(self.nodes.values()) + list(self.sources.values())
            if node.name == name and (allowed is None or node.resource_type in allowed)
        ]
        if not matches:
            raise ConfigError(f"'{name}' was not found in the dbt manifest")
        if len(matches) > 1:
            ids = ", ".join(node.unique_id for node in matches)
            raise ConfigError(f"Name '{name}' is ambiguous: {ids}")
        return matches[0]

    def upstream_models(self, unique_id: str) -> list[DbtNode]:
        start = self.get(unique_id)
        if start is None:
            raise ConfigError(f"Unknown node {unique_id}")
        seen: set[str] = set()
        ordered: list[DbtNode] = []
        stack = list(start.depends_on)
        while stack:
            current_id = stack.pop()
            if current_id in seen:
                continue
            seen.add(current_id)
            node = self.get(current_id)
            if node is None:
                continue
            if node.resource_type == "model":
                ordered.append(node)
            stack.extend(node.depends_on)
        return ordered

    def tests_for(self, unique_id: str) -> list[DbtNode]:
        return [
            node
            for node in self.nodes.values()
            if node.resource_type == "test" and unique_id in node.depends_on
        ]

    def singular_tests(self) -> list[DbtNode]:
        return [
            node
            for node in self.nodes.values()
            if node.resource_type == "test" and node.original_file_path
            and node.original_file_path.startswith("tests/")
        ]


def _node_from_raw(unique_id: str, raw: dict[str, Any]) -> DbtNode:
    depends = tuple((raw.get("depends_on") or {}).get("nodes") or [])
    return DbtNode(
        unique_id=str(raw.get("unique_id") or unique_id),
        name=str(raw.get("name") or unique_id.split(".")[-1]),
        resource_type=str(raw.get("resource_type") or "model"),
        database=raw.get("database"),
        schema=raw.get("schema"),
        relation_name=raw.get("relation_name"),
        depends_on=depends,
        original_file_path=raw.get("original_file_path"),
        tags=tuple(raw.get("tags") or []),
        source_name=raw.get("source_name"),
        compiled_code=raw.get("compiled_code"),
        package_name=raw.get("package_name"),
    )


def load_manifest(path: Path) -> Manifest:
    if not path.is_file():
        raise ConfigError(f"Missing dbt manifest: {path}")
    raw = json.loads(path.read_text())
    metadata = raw.get("metadata") or {}
    nodes = {
        unique_id: _node_from_raw(unique_id, node)
        for unique_id, node in (raw.get("nodes") or {}).items()
        if isinstance(node, dict)
    }
    sources = {
        unique_id: _node_from_raw(unique_id, node)
        for unique_id, node in (raw.get("sources") or {}).items()
        if isinstance(node, dict)
    }
    project_name = str(metadata.get("project_name") or "")
    if not project_name:
        raise ConfigError("Manifest metadata.project_name is required")
    return Manifest(
        project_name=project_name,
        adapter_type=metadata.get("adapter_type"),
        nodes=nodes,
        sources=sources,
        path=path,
    )


@dataclass(frozen=True)
class TestResult:
    unique_id: str
    name: str
    status: str
    failures: int | None = None
    message: str | None = None

    @property
    def passed(self) -> bool:
        return self.status.lower() in {"pass", "passed", "success"}


@dataclass
class RunResults:
    results: list[TestResult] = field(default_factory=list)
    path: Path | None = None

    def by_name(self, name: str) -> TestResult | None:
        for result in self.results:
            if result.name == name or result.unique_id.endswith(name):
                return result
        return None


def load_run_results(path: Path) -> RunResults:
    if not path.is_file():
        return RunResults(path=path)
    raw = json.loads(path.read_text())
    results: list[TestResult] = []
    for item in raw.get("results") or []:
        unique_id = str(item.get("unique_id") or "")
        if not unique_id:
            continue
        name = unique_id.split(".")[2] if unique_id.count(".") >= 2 else unique_id
        failures = item.get("failures")
        results.append(
            TestResult(
                unique_id=unique_id,
                name=name,
                status=str(item.get("status") or "unknown"),
                failures=int(failures) if failures is not None else None,
                message=item.get("message"),
            )
        )
    return RunResults(results=results, path=path)


def inspect_report(manifest: Manifest, model_name: str) -> dict[str, Any]:
    model = manifest.find_model(model_name)
    upstream = manifest.upstream_models(model.unique_id)
    reachable_sources: list[DbtNode] = []
    seen: set[str] = set()
    stack = [model.unique_id, *[node.unique_id for node in upstream]]
    while stack:
        current_id = stack.pop()
        if current_id in seen:
            continue
        seen.add(current_id)
        node = manifest.get(current_id)
        if node is None:
            continue
        for dep in node.depends_on:
            dep_node = manifest.get(dep)
            if dep_node is None:
                continue
            if dep_node.resource_type == "source":
                if dep not in {source.unique_id for source in reachable_sources}:
                    reachable_sources.append(dep_node)
            else:
                stack.append(dep)
    tests = [
        {
            "name": test.name,
            "uniqueId": test.unique_id,
            "path": test.original_file_path,
        }
        for test in manifest.tests_for(model.unique_id)
    ]
    return {
        "project": manifest.project_name,
        "adapter": manifest.adapter_type,
        "model": {
            "name": model.name,
            "uniqueId": model.unique_id,
            "relation": model.relation,
            "database": model.database,
            "schema": model.schema,
        },
        "upstreamModels": [
            {
                "name": node.name,
                "uniqueId": node.unique_id,
                "relation": node.relation,
            }
            for node in sorted(upstream, key=lambda node: node.unique_id)
        ],
        "sources": [
            {
                "name": node.name,
                "sourceName": node.source_name,
                "uniqueId": node.unique_id,
                "relation": node.relation,
                "database": node.database,
                "schema": node.schema,
            }
            for node in sorted(reachable_sources, key=lambda node: node.unique_id)
        ],
        "tests": tests,
        "relations": {
            "database": model.database,
            "schema": model.schema,
        },
    }


def format_inspect_report(report: dict[str, Any]) -> str:
    model = report["model"]
    lines = [
        f"Project: {report['project']}",
        f"Adapter: {report['adapter'] or 'unknown'}",
        f"Target model: {model['name']} ({model['relation']})",
        f"Database/schema: {model['database']}.{model['schema']}",
        "",
        "Upstream models:",
    ]
    if report["upstreamModels"]:
        for node in report["upstreamModels"]:
            lines.append(f"  - {node['name']} ({node['relation']})")
    else:
        lines.append("  (none)")

    lines.extend(["", "Sources:"])
    if report["sources"]:
        for source in report["sources"]:
            label = f"{source['sourceName']}.{source['name']}" if source.get("sourceName") else source["name"]
            lines.append(f"  - {label} ({source['relation']})")
    else:
        lines.append("  (none)")

    lines.extend(["", "Tests on target model:"])
    if report["tests"]:
        for test in report["tests"]:
            lines.append(f"  - {test['name']}")
    else:
        lines.append("  (none)")
    return "\n".join(lines)
