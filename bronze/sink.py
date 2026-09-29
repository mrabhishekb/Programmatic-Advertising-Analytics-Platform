"""Kafka -> Bronze: the CDC tail landing in object storage.

Reads the ``cdc.public.*`` topics and writes each batch as Parquet under
``cdc/<table>/dt=<date>/``. Nothing is interpreted on the way through: the
change envelope is preserved as-is (see ``bronze.records``), because Bronze's
job is to be a faithful, replayable record of what arrived.

## Delivery guarantee

At-least-once, on purpose. The order is always:

1. write the Parquet object
2. only then commit the Kafka offsets

Crash between the two and the same messages are read again on restart. That
duplicate is harmless because the object key is derived from the Kafka offsets
the file contains, so the replay writes the *same key* with the same content
rather than a second copy under a new name.

The other order - commit first, then write - would be at-most-once, and a crash
in the gap would lose change events permanently with nothing to detect it. For a
layer whose entire purpose is to be the durable record, that trade is the wrong
way round.
"""

from __future__ import annotations

import signal
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from bronze import layout, records
from bronze.storage import BronzeStore
from data_generator.logging_setup import get_logger

logger = get_logger(__name__)

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_GROUP = "adtech-bronze-sink"
DEFAULT_TOPIC_PATTERN = "^cdc\\.public\\..*"


@dataclass(slots=True)
class SinkStats:
    messages: int = 0
    tombstones: int = 0
    objects: int = 0
    bytes_written: int = 0
    flushes: int = 0
    by_table: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "messages": self.messages,
            "tombstones": self.tombstones,
            "objects": self.objects,
            "bytes_written": self.bytes_written,
            "flushes": self.flushes,
            "by_table": dict(sorted(self.by_table.items())),
        }


#: A buffered batch is keyed by everything that determines its object key, so a
#: flush never has to split a group across two files.
_GroupKey = tuple[str, str, int]  # (table, partition_date, kafka_partition)


class BronzeSink:
    def __init__(
        self,
        store: BronzeStore | None = None,
        *,
        bootstrap: str = DEFAULT_BOOTSTRAP,
        group_id: str = DEFAULT_GROUP,
        topic_pattern: str = DEFAULT_TOPIC_PATTERN,
        max_records: int = 5_000,
        max_seconds: float = 30.0,
        from_beginning: bool = True,
    ) -> None:
        self.store = store or BronzeStore()
        self.bootstrap = bootstrap
        self.group_id = group_id
        self.topic_pattern = topic_pattern
        self.max_records = max_records
        self.max_seconds = max_seconds
        self.from_beginning = from_beginning
        self.stats = SinkStats()
        self._buffer: dict[_GroupKey, list[dict[str, Any]]] = defaultdict(list)
        self._buffered = 0

    # -- buffering --------------------------------------------------------

    def add(self, record: dict[str, Any]) -> None:
        # Tombstones carry no event time, so they ride in the partition of the
        # moment they were ingested. They are ordering markers rather than data,
        # and holding them back to find a date would stall the flush.
        event_ts = record["event_ts_ms"] or record["ingested_at_ms"]
        key: _GroupKey = (
            record["source_table"],
            layout.partition_date_of(event_ts),
            record["kafka_partition"],
        )
        self._buffer[key].append(record)
        self._buffered += 1
        self.stats.messages += 1
        if record["op"] == records.OP_TOMBSTONE:
            self.stats.tombstones += 1

    @property
    def buffered(self) -> int:
        return self._buffered

    # -- flushing ---------------------------------------------------------

    def flush(self) -> list[layout.BronzeObject]:
        """Write every buffered group. Returns what was written, for logging."""
        if not self._buffer:
            return []

        written: list[layout.BronzeObject] = []
        for (table, partition_date, kafka_partition), rows in sorted(self._buffer.items()):
            offsets = [row["kafka_offset"] for row in rows]
            key = layout.cdc_object_key(
                table,
                partition_date,
                topic_partition=kafka_partition,
                first_offset=min(offsets),
                last_offset=max(offsets),
            )
            # Sorted by offset so the file reads in the order the changes
            # happened, which lets a reader take the last row per key without
            # sorting the whole partition first.
            rows.sort(key=lambda row: row["kafka_offset"])
            size = self.store.put_table(key, records.to_arrow(rows))

            written.append(
                layout.BronzeObject(
                    key=key, table=table, partition_date=partition_date, records=len(rows)
                )
            )
            self.stats.objects += 1
            self.stats.bytes_written += size
            self.stats.by_table[table] = self.stats.by_table.get(table, 0) + len(rows)

        self._buffer.clear()
        self._buffered = 0
        self.stats.flushes += 1
        return written

    # -- the loop ---------------------------------------------------------

    def run(self, *, max_messages: int = 0, idle_timeout: float = 0.0) -> SinkStats:
        """Consume until stopped, or until ``max_messages`` have been written.

        ``idle_timeout`` stops after that many seconds with nothing new, which
        is what makes a one-shot ``make bronze-sink`` terminate instead of
        hanging once it has drained the topics.
        """
        from confluent_kafka import Consumer, KafkaError

        stop = threading.Event()

        def request_stop(signum: int, _frame: Any) -> None:
            logger.info("stop requested", extra={"signal": signal.Signals(signum).name})
            stop.set()

        for received in (signal.SIGINT, signal.SIGTERM):
            signal.signal(received, request_stop)

        self.store.ping()

        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest" if self.from_beginning else "latest",
                # Committing is the sink's job, and it only happens after a
                # successful write. Auto-commit would acknowledge messages that
                # are still sitting in the buffer.
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([self.topic_pattern])
        logger.info(
            "bronze sink started",
            extra={
                "topics": self.topic_pattern,
                "destination": self.store.settings.describe(),
                "group": self.group_id,
            },
        )

        last_flush = time.monotonic()
        last_message = time.monotonic()

        try:
            while not stop.is_set():
                message = consumer.poll(timeout=1.0)
                now = time.monotonic()

                if message is None:
                    if self._should_flush(now - last_flush):
                        self._flush_and_commit(consumer)
                        last_flush = now
                    if idle_timeout and (now - last_message) >= idle_timeout:
                        logger.info("idle, stopping", extra={"seconds": round(idle_timeout, 1)})
                        break
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(message.error()))

                last_message = now
                self.add(
                    records.record_from_message(
                        topic=message.topic(),
                        partition=message.partition(),
                        offset=message.offset(),
                        key=message.key(),
                        value=message.value(),
                        ingested_at_ms=int(time.time() * 1000),
                    )
                )

                if self._buffered >= self.max_records or self._should_flush(now - last_flush):
                    self._flush_and_commit(consumer)
                    last_flush = now

                if max_messages and self.stats.messages >= max_messages:
                    break

            self._flush_and_commit(consumer)
        finally:
            consumer.close()

        logger.info("bronze sink stopped", extra=self.stats.as_dict())
        return self.stats

    def _should_flush(self, elapsed: float) -> bool:
        return self._buffered > 0 and elapsed >= self.max_seconds

    def _flush_and_commit(self, consumer: Any) -> None:
        """Write first, commit second. See the delivery guarantee above."""
        written = self.flush()
        if not written:
            return
        for item in written:
            logger.info(
                "wrote bronze object",
                extra={"key": item.key, "table": item.table, "records": item.records},
            )
        consumer.commit(asynchronous=False)
