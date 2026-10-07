"""Snowflake connection settings, and the four schemas the warehouse is built in.

Settings come from the environment like every other connection in this project,
so the same code runs against a trial account, a colleague's account, or CI
without edits.

Two things differ from the PostgreSQL settings next door.

**There is no usable default.** ``DatabaseSettings`` can default to
``localhost:5432`` because the stack brings that database up. Snowflake is
somebody's account, and guessing produces a confusing authentication error
instead of an honest "you have not configured this yet". ``configured`` exists
so callers can say that plainly, and so the test suite can skip rather than fail
on a machine with no account.

**Key-pair authentication is the default path.** Snowflake now blocks
password-only sign-in for programmatic users, so a password works only where
it has been explicitly allowed. Both are supported, and key-pair wins when a
key is present - that is the one that keeps working.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_generator.config import PROJECT_ROOT
from data_generator.db import load_dotenv_file

#: Landing copy of Silver, untransformed. Written by ``warehouse.load``, and the
#: only schema the Python side touches - everything above it is dbt's.
RAW_SCHEMA = "RAW"

#: Typed, renamed views over RAW. Thin by design: a staging model that does real
#: work is a transformation hiding in a layer meant to be mechanical.
STAGING_SCHEMA = "STAGING"

#: Dimensions and facts. The dimensional model proper.
CORE_SCHEMA = "CORE"

#: Aggregated data products, and what Power BI connects to.
ANALYTICS_SCHEMA = "ANALYTICS"

SCHEMAS = (RAW_SCHEMA, STAGING_SCHEMA, CORE_SCHEMA, ANALYTICS_SCHEMA)


class WarehouseNotConfigured(RuntimeError):
    """Raised when Snowflake is asked for but no credentials are set."""


@dataclass(frozen=True, slots=True)
class SnowflakeSettings:
    account: str = ""
    user: str = ""
    password: str = ""
    private_key_path: str = ""
    private_key_passphrase: str = ""
    role: str = "ADTECH_ENGINEER"
    warehouse: str = "ADTECH_WH"
    database: str = "ADTECH"
    schema: str = RAW_SCHEMA

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> SnowflakeSettings:
        load_dotenv_file(env_file or PROJECT_ROOT / ".env")
        return cls(
            account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            user=os.environ.get("SNOWFLAKE_USER", ""),
            password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", ""),
            private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", ""),
            role=os.environ.get("SNOWFLAKE_ROLE", "ADTECH_ENGINEER"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ADTECH_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "ADTECH"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", RAW_SCHEMA),
        )

    @property
    def configured(self) -> bool:
        """Whether there is enough here to attempt a connection."""
        return bool(self.account and self.user and (self.password or self.private_key_path))

    @property
    def uses_key_pair(self) -> bool:
        return bool(self.private_key_path)

    def describe(self) -> str:
        """Identifying detail without the secret, for logs and error messages."""
        if not self.configured:
            return "snowflake (not configured)"
        auth = "key-pair" if self.uses_key_pair else "password"
        return f"{self.user}@{self.account}/{self.database}.{self.schema} [{self.role}, {auth}]"

    def require(self) -> SnowflakeSettings:
        """Return self, or explain exactly what is missing and how to set it."""
        if self.configured:
            return self
        missing = [
            name
            for name, value in (
                ("SNOWFLAKE_ACCOUNT", self.account),
                ("SNOWFLAKE_USER", self.user),
                (
                    "SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH",
                    self.password or self.private_key_path,
                ),
            )
            if not value
        ]
        raise WarehouseNotConfigured(
            "Snowflake is not configured: "
            + ", ".join(missing)
            + " not set.\nAdd them to .env - see the Snowflake section of .env.example."
        )


def _load_private_key(settings: SnowflakeSettings) -> bytes:
    """Read and decrypt the private key into the DER bytes the connector wants.

    The connector takes a key, not a path, and specifically a DER-encoded one -
    handing it the PEM file's bytes fails with an error that does not mention
    encoding, which is a long way to go for a missing conversion.
    """
    from cryptography.hazmat.primitives import serialization

    path = Path(settings.private_key_path).expanduser()
    if not path.exists():
        raise WarehouseNotConfigured(
            f"SNOWFLAKE_PRIVATE_KEY_PATH points at {path}, which does not exist."
        )

    passphrase = (
        settings.private_key_passphrase.encode() if settings.private_key_passphrase else None
    )
    key = serialization.load_pem_private_key(path.read_bytes(), password=passphrase)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(settings: SnowflakeSettings | None = None, **overrides: Any) -> Any:
    """Open a Snowflake connection. The driver is imported lazily, as psycopg is.

    Nothing in phases 1-8 needs Snowflake, so importing the connector at module
    scope would make the whole lakehouse path depend on a driver it never calls.
    """
    import snowflake.connector

    resolved = (settings or SnowflakeSettings.from_env()).require()
    kwargs: dict[str, Any] = {
        "account": resolved.account,
        "user": resolved.user,
        "role": resolved.role,
        "warehouse": resolved.warehouse,
        "database": resolved.database,
        "schema": resolved.schema,
        **overrides,
    }
    if resolved.uses_key_pair:
        kwargs["private_key"] = _load_private_key(resolved)
    else:
        kwargs["password"] = resolved.password

    return snowflake.connector.connect(**kwargs)
