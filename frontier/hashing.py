from __future__ import annotations

import hashlib
import hmac
import os
import re

from frontier.config import ConfigError, ModelConfig

HMAC_VERSION = "v1"
ENTITY_HASH_KEY_ENV = "FRONTIER_ENTITY_HASH_KEY"

_INTEGER_VALUE = re.compile(r"[+-]?\d+")
_TRAILING_ZERO_DECIMAL = re.compile(r"([+-]?\d+)\.0+")


def entity_hash_key_from_env() -> str:
    """Return the HMAC key. Never log or return this from CLI output helpers."""
    key = os.environ.get(ENTITY_HASH_KEY_ENV)
    if key is None or not key.strip():
        raise ConfigError(
            "FRONTIER_ENTITY_HASH_KEY is required unless --include-entity-ids is set"
        )
    return key


def normalize_entity_value(value: str) -> str:
    """Canonicalize equivalent IDs without changing business identity."""
    text = str(value).strip()
    if _INTEGER_VALUE.fullmatch(text):
        return str(int(text))
    decimal = _TRAILING_ZERO_DECIMAL.fullmatch(text)
    if decimal:
        return str(int(decimal.group(1)))
    return text


def entity_type_from_key(entity_key: str, model: ModelConfig) -> str:
    key = entity_key.strip()
    if key == model.key:
        return model.entity
    if key.endswith("_id") and len(key) > 3:
        return key[: -len("_id")]
    return key


def canonical_entity_message(
    *,
    project: str,
    entity_type: str,
    entity_key: str,
    value: str,
) -> str:
    parts = [
        HMAC_VERSION,
        project.strip(),
        entity_type.strip().lower(),
        entity_key.strip().lower(),
        normalize_entity_value(value),
    ]
    if any("|" in part for part in parts):
        raise ConfigError("Entity hash components cannot contain '|'")
    return "|".join(parts)


def hmac_entity_id(
    secret: str,
    *,
    project: str,
    entity_type: str,
    entity_key: str,
    value: str,
) -> str:
    message = canonical_entity_message(
        project=project,
        entity_type=entity_type,
        entity_key=entity_key,
        value=value,
    )
    return hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def hmac_equal(left: str, right: str) -> bool:
    if len(left) != len(right):
        return False
    return hmac.compare_digest(left, right)
