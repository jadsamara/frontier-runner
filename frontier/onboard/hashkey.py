from __future__ import annotations

import secrets

HASH_KEY_BYTES = 32


def generate_entity_hash_key() -> str:
    """Return a hex-encoded key with at least 256 bits of randomness."""
    return secrets.token_hex(HASH_KEY_BYTES)


def hash_key_prefix(key: str) -> str:
    value = key.strip()
    if len(value) <= 8:
        return "…"
    return f"{value[:8]}…"
