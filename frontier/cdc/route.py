from __future__ import annotations

from dataclasses import dataclass

from frontier.cdc.config import CdcConfig, CdcSourceConfig
from frontier.cdc.normalize import LogicalChangeEvent
from frontier.config import ConfigError


@dataclass(frozen=True)
class RoutedCandidate:
    entity_key: str
    event_count: int
    origin: str = "event"


@dataclass(frozen=True)
class RoutingResult:
    candidates: tuple[RoutedCandidate, ...]
    event_count: int
    missed_event_count: int = 0

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(item.entity_key for item in self.candidates)

    def event_counts(self) -> dict[str, int]:
        return {item.entity_key: item.event_count for item in self.candidates}


def source_for(config: CdcConfig, source_model: str) -> CdcSourceConfig:
    matches = [source for source in config.sources if source.source_model == source_model]
    if not matches:
        raise ConfigError("unresolved CDC event")
    return matches[0]


def target_keys_for_event(event: LogicalChangeEvent, source: CdcSourceConfig) -> tuple[str, ...]:
    """Map one logical event to target-entity keys using frontier-cdc.yml."""
    del source
    operation = event.operation.upper()
    keys: list[str] = []
    if operation == "INSERT":
        if not event.target_key_after:
            raise ConfigError("unresolved CDC event")
        keys.append(event.target_key_after)
    elif operation == "DELETE":
        if not event.target_key_before:
            raise ConfigError("delete is missing the required before target key")
        keys.append(event.target_key_before)
    elif operation == "UPDATE":
        before = event.target_key_before
        after = event.target_key_after
        if before and after and before != after:
            if not before or not after:
                raise ConfigError("delete is missing the required before target key")
            keys.extend([before, after])
        elif after:
            if not before:
                raise ConfigError("delete is missing the required before target key")
            keys.append(after)
        elif before:
            keys.append(before)
        else:
            raise ConfigError("unresolved CDC event")
    else:
        raise ConfigError("unresolved CDC event")
    unique: dict[str, str] = {}
    for key in keys:
        text = str(key).strip()
        if not text:
            raise ConfigError("unresolved CDC event")
        unique.setdefault(text, text)
    return tuple(unique.values())


def route_events(events: list[LogicalChangeEvent], config: CdcConfig) -> RoutingResult:
    contributed: dict[str, set[str]] = {}
    for event in events:
        source = source_for(config, event.source_model)
        if "DELETE" in source.require_before_image_for and event.operation == "DELETE":
            if not event.target_key_before:
                raise ConfigError("delete is missing the required before target key")
        if "KEY_CHANGE" in source.require_before_image_for and event.operation == "UPDATE":
            if event.target_key_before != event.target_key_after and not event.target_key_before:
                raise ConfigError("delete is missing the required before target key")
        keys = target_keys_for_event(event, source)
        if not keys:
            raise ConfigError("unresolved CDC event")
        for key in keys:
            contributed.setdefault(key, set()).add(event.event_id)
    candidates = tuple(
        RoutedCandidate(entity_key=key, event_count=len(event_ids), origin="event")
        for key, event_ids in contributed.items()
    )
    return RoutingResult(candidates=candidates, event_count=len(events), missed_event_count=0)
