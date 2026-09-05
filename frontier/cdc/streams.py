from __future__ import annotations

from frontier.cdc.normalize import records_from_rows

METADATA_ACTION = "METADATA$ACTION"
METADATA_ISUPDATE = "METADATA$ISUPDATE"
METADATA_ROW_ID = "METADATA$ROW_ID"


def pair_stream_rows(rows):
    """Backward-compatible pairing used by older stream helpers."""
    from frontier.cdc.config import CdcSourceConfig
    from frontier.cdc.normalize import normalize_stream_records

    source = CdcSourceConfig(
        source_model="source",
        base_relation="DB.SCHEMA.BASE",
        stream_relation="DB.SCHEMA.STREAM",
        primary_key="ID",
        target_entity="entity",
        target_key="TARGET",
        require_before_image_for=(),
    )
    records = records_from_rows(rows)
    return normalize_stream_records(records, source=source, provider="snowflake_stream")
