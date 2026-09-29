"""The Bronze layer: immutable raw data in object storage.

Two paths land here and neither interprets what it carries:

* the bulk export of rows that existed before CDC started
  (``scripts/export_snapshot.py``), and
* the CDC tail, every change since (``bronze.sink``).

Bronze is append-only. A correction is a later change event with a higher LSN,
never an edit to what is already written, which is what makes reprocessing a
matter of reading the same objects again.
"""

from bronze.layout import CDC_PREFIX, SNAPSHOT_PREFIX
from bronze.storage import BronzeStorageError, BronzeStore, S3Settings

__all__ = [
    "CDC_PREFIX",
    "SNAPSHOT_PREFIX",
    "BronzeStorageError",
    "BronzeStore",
    "S3Settings",
]
