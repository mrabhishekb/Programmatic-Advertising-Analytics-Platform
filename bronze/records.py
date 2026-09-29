"""The Bronze record: one row per change event, with a schema that never moves.

``before`` and ``after`` are stored as JSON **strings**, not as structs. That
looks lossy and is the most important decision in this layer.

Storing them as structs would bind every Bronze file to the shape the source
table had on the day it was written. Add a column in phase 17 and the old files
and the new ones no longer share a schema, so a reader either has to merge
schemas across the whole history - expensive, and it fails outright on a type
change - or the Bronze layer has to be rewritten, which contradicts the promise
that it is immutable.

Keeping the payload opaque moves that problem to where it can actually be
solved. Bronze answers "what arrived, and in what order"; Silver answers "what
does it mean", and can parse each file against the schema it needs today.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

#: Every change event, from every table, has exactly this shape.
SCHEMA = pa.schema(
    [
        # -- what happened --------------------------------------------------
        pa.field("op", pa.string(), nullable=False),
        pa.field("source_schema", pa.string()),
        pa.field("source_table", pa.string(), nullable=False),
        # Event time from the source database. Partitioning uses this, so a
        # replay lands in the same partition as the original run.
        pa.field("event_ts_ms", pa.int64()),
        # -- ordering -------------------------------------------------------
        # The LSN is the only true ordering across tables: two changes in one
        # transaction share a txId, and wall-clock timestamps can tie.
        pa.field("lsn", pa.int64()),
        pa.field("tx_id", pa.int64()),
        # -- payload, deliberately opaque -----------------------------------
        pa.field("key", pa.string()),
        pa.field("before_json", pa.string()),
        pa.field("after_json", pa.string()),
        # -- provenance, so any row can be traced back to its message --------
        pa.field("kafka_topic", pa.string(), nullable=False),
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("ingested_at_ms", pa.int64(), nullable=False),
    ]
)

#: Debezium's own codes, plus one of ours. A tombstone is the null-valued
#: message Debezium writes after a delete so log compaction can drop the key;
#: it carries no payload, so it cannot be told apart from a real delete once
#: written, and it gets its own code rather than being silently discarded.
OP_TOMBSTONE = "t"

_TABLE_FROM_TOPIC_PARTS = 3


def table_from_topic(topic: str) -> str:
    """``cdc.public.campaigns`` -> ``campaigns``.

    Used only for tombstones, which carry no payload to read the table from.
    """
    parts = topic.split(".")
    if len(parts) < _TABLE_FROM_TOPIC_PARTS:
        return topic
    return parts[-1]


def record_from_message(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    ingested_at_ms: int,
) -> dict[str, Any]:
    """Flatten one Kafka message into a Bronze row."""
    decoded_key = key.decode("utf-8", errors="replace") if key else None

    if value is None:
        return {
            "op": OP_TOMBSTONE,
            "source_schema": None,
            "source_table": table_from_topic(topic),
            "event_ts_ms": None,
            "lsn": None,
            "tx_id": None,
            "key": decoded_key,
            "before_json": None,
            "after_json": None,
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "ingested_at_ms": ingested_at_ms,
        }

    envelope = json.loads(value)
    source = envelope.get("source") or {}
    transaction = envelope.get("transaction") or {}

    return {
        "op": envelope.get("op") or "?",
        "source_schema": source.get("schema"),
        "source_table": source.get("table") or table_from_topic(topic),
        "event_ts_ms": envelope.get("ts_ms"),
        "lsn": _as_int(source.get("lsn")),
        "tx_id": _as_int(source.get("txId") or transaction.get("id", "").split(":")[0] or None),
        "key": decoded_key,
        # Re-serialised rather than passed through: the original bytes are a
        # fragment of a larger envelope, and round-tripping gives one canonical
        # encoding so identical payloads compare equal downstream.
        "before_json": _dump(envelope.get("before")),
        "after_json": _dump(envelope.get("after")),
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "ingested_at_ms": ingested_at_ms,
    }


def to_arrow(records: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(records, schema=SCHEMA)


def _dump(payload: Any) -> str | None:
    if payload is None:
        return None
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
