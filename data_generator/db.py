"""PostgreSQL connection settings.

Settings come from the environment (or ``.env``) so the same code runs against a
local container, CI, or a managed instance without edits.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_generator.config import PROJECT_ROOT


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    host: str = "localhost"
    port: int = 5432
    database: str = "adtech"
    user: str = "adtech"
    password: str = "adtech"

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> DatabaseSettings:
        load_dotenv_file(env_file or PROJECT_ROOT / ".env")
        return cls(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            database=os.environ.get("POSTGRES_DB", "adtech"),
            user=os.environ.get("POSTGRES_USER", "adtech"),
            password=os.environ.get("POSTGRES_PASSWORD", "adtech"),
        )

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )

    def describe(self) -> str:
        """Connection string without the password, for logs."""
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"


def load_dotenv_file(path: Path) -> None:
    """Load ``.env`` without overriding variables already set in the environment."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def connect(settings: DatabaseSettings | None = None, *, autocommit: bool = False) -> Any:
    """Open a psycopg connection. Imported lazily so CSV-only runs need no driver."""
    import psycopg

    resolved = settings or DatabaseSettings.from_env()
    return psycopg.connect(resolved.conninfo, autocommit=autocommit)
