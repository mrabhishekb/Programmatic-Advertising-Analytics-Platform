"""Structured logging.

Log lines are ``key=value`` formatted so they stay greppable by hand now and
parseable by a log pipeline later. Anything passed via ``extra=`` is rendered
automatically.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

_RESERVED = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {
    "message",
    "asctime",
    "taskName",
}


class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = (
            f"{self.formatTime(record, '%Y-%m-%dT%H:%M:%S')} {record.levelname:<7} "
            f"{record.name:<28} {record.getMessage()}"
        )
        extras = {key: value for key, value in record.__dict__.items() if key not in _RESERVED}
        if extras:
            base += " " + " ".join(f"{key}={_render(value)}" for key, value in extras.items())
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


def _render(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    if isinstance(value, int):
        return f"{value:,}"
    text = str(value)
    return f'"{text}"' if " " in text else text


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(KeyValueFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
