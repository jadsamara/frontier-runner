from __future__ import annotations

import hashlib
import re

import sqlglot
from sqlglot.errors import SqlglotError

_DIALECTS = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "postgres": "postgres",
    "redshift": "redshift",
}

_WHITESPACE = re.compile(r"\s+")


def sql_dialect(adapter_type: str | None) -> str | None:
    if not adapter_type:
        return None
    return _DIALECTS.get(adapter_type.strip().lower())


def _strip_comments_and_whitespace(sql: str) -> str:
    """Fallback when sqlglot cannot parse: drop comments and collapse whitespace.

    String literals are preserved. Keywords and identifiers outside quotes are
    lowercased so formatting-only edits do not change the fingerprint.
    """
    out: list[str] = []
    i = 0
    length = len(sql)
    in_single = False
    in_double = False
    while i < length:
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""
        if in_single:
            out.append(char)
            if char == "'" and nxt == "'":
                out.append(nxt)
                i += 2
                continue
            if char == "'":
                in_single = False
            i += 1
            continue
        if in_double:
            out.append(char)
            if char == '"' and nxt == '"':
                out.append(nxt)
                i += 2
                continue
            if char == '"':
                in_double = False
            i += 1
            continue
        if char == "'":
            in_single = True
            out.append(char)
            i += 1
            continue
        if char == '"':
            in_double = True
            out.append(char)
            i += 1
            continue
        if char == "-" and nxt == "-":
            i += 2
            while i < length and sql[i] not in "\n\r":
                i += 1
            out.append(" ")
            continue
        if char == "/" and nxt == "*":
            i += 2
            while i < length and not (sql[i] == "*" and i + 1 < length and sql[i + 1] == "/"):
                i += 1
            i = min(length, i + 2)
            out.append(" ")
            continue
        out.append(char.lower())
        i += 1
    return _WHITESPACE.sub(" ", "".join(out)).strip()


def normalize_sql(sql: str, *, dialect: str | None = None) -> str:
    text = sql.strip()
    if not text:
        return ""
    try:
        statements = sqlglot.parse(text, dialect=dialect)
    except SqlglotError:
        return _strip_comments_and_whitespace(text)
    rendered: list[str] = []
    for expression in statements:
        if expression is None:
            continue
        rendered.append(
            expression.sql(
                dialect=dialect,
                comments=False,
                pretty=False,
                normalize=True,
                normalize_functions="lower",
            ).strip()
        )
    if not rendered:
        return _strip_comments_and_whitespace(text)
    return "; ".join(rendered)


def sql_fingerprint(sql: str, *, dialect: str | None = None) -> str:
    normalized = normalize_sql(sql, dialect=dialect)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
