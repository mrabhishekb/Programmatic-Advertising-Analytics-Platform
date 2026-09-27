"""Output sinks.

The generator streams rows rather than materialising the whole dataset, so a
sink is anything that can accept ``(table, columns, rows)`` batches. Batches are
always flushed in foreign-key dependency order, which is what allows PostgreSQL's
foreign keys to stay immediately enforced during the load: a click is never
written before the impression it points at.
"""

from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from data_generator.logging_setup import get_logger
from data_generator.models import MODEL_BY_TABLE, TABLE_NAMES, columns_of, row_of

logger = get_logger(__name__)


class RowWriter(ABC):
    """Destination for batches of rows."""

    @abstractmethod
    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None: ...

    def finish(self) -> None:  # pragma: no cover - default no-op
        return None

    def __enter__(self) -> RowWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.finish()


class NullWriter(RowWriter):
    """Counts rows and discards them. Used by tests and dry runs."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = dict.fromkeys(TABLE_NAMES, 0)

    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
        self.counts[table] = self.counts.get(table, 0) + len(rows)


class CsvWriter(RowWriter):
    """One CSV per table. Handy for inspecting a run without a database."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, Any] = {}
        self._writers: dict[str, Any] = {}

    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
        writer = self._writers.get(table)
        if writer is None:
            handle = (self.directory / f"{table}.csv").open("w", newline="", encoding="utf-8")
            writer = csv.writer(handle)
            writer.writerow(columns)
            self._handles[table] = handle
            self._writers[table] = writer
        writer.writerows(rows)

    def finish(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()
        self._writers.clear()


class PostgresCopyWriter(RowWriter):
    """Streams batches into PostgreSQL with ``COPY ... FROM STDIN``.

    COPY is an order of magnitude faster than multi-row INSERT for this volume.
    Foreign keys stay enforced on every row: the emitter flushes tables in
    dependency order inside a single transaction, so a parent row is always
    visible by the time its children are copied.
    """

    def __init__(self, connection: Any) -> None:
        self._connection = connection
        self.counts: dict[str, int] = dict.fromkeys(TABLE_NAMES, 0)

    def write_rows(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> None:
        if not rows:
            return
        column_list = ", ".join(columns)
        statement = f"COPY {table} ({column_list}) FROM STDIN"
        with self._connection.cursor() as cursor, cursor.copy(statement) as copy:
            for row in rows:
                copy.write_row(row)
        self.counts[table] += len(rows)

    def finish(self) -> None:
        self._connection.commit()


class BatchedEmitter:
    """Buffers model instances and flushes them in dependency order."""

    def __init__(self, writer: RowWriter, batch_size: int) -> None:
        self._writer = writer
        self._batch_size = batch_size
        self._buffers: dict[str, list[tuple]] = {table: [] for table in TABLE_NAMES}
        self._buffered = 0
        self.counts: dict[str, int] = dict.fromkeys(TABLE_NAMES, 0)

    def emit(self, rows: Sequence[object]) -> None:
        if not rows:
            return
        table = type(rows[0]).TABLE
        buffer = self._buffers[table]
        buffer.extend(row_of(row) for row in rows)
        self._buffered += len(rows)
        self.counts[table] += len(rows)
        if self._buffered >= self._batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buffered:
            return
        # TABLE_NAMES is in dependency order, so parents are always written first.
        for table in TABLE_NAMES:
            buffer = self._buffers[table]
            if not buffer:
                continue
            self._writer.write_rows(table, columns_of(MODEL_BY_TABLE[table]), buffer)
            buffer.clear()
        self._buffered = 0

    def close(self) -> None:
        self.flush()
        self._writer.finish()
