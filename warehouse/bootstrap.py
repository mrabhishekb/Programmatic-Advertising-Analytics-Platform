"""Creating the account objects phases 9-13 need, idempotently.

``bootstrap.sql`` holds the statements; this runs them. The split exists so the
SQL stays readable as SQL - the same reason the data quality checks are files
rather than string literals.

Run as ACCOUNTADMIN, which is the one place in this project that role is used:
creating a role, a warehouse and a database all require it. Everything after
this connects as the engineer role the script grants.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from data_generator.logging_setup import get_logger
from warehouse.settings import SnowflakeSettings, WarehouseNotConfigured, connect

logger = get_logger(__name__)

BOOTSTRAP_SQL = Path(__file__).resolve().parent / "bootstrap.sql"

#: Snowflake's unquoted identifier rules. Enforced because these names are
#: interpolated into SQL rather than bound as parameters - an identifier cannot
#: be a bind variable, so the only defence is refusing anything that is not one.
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

#: Snowflake's built-in roles. Creating one is an error, and granting it is
#: either redundant or a privilege escalation nobody asked this script to
#: perform - so both statements are dropped when SNOWFLAKE_ROLE names one.
#: Pointing the project at ACCOUNTADMIN is not what the rest of the setup
#: assumes, but it is what a trial account hands you, and failing on it would
#: make the first command anyone runs the one that does not work.
SYSTEM_ROLES = frozenset(
    {"ACCOUNTADMIN", "SECURITYADMIN", "USERADMIN", "SYSADMIN", "PUBLIC", "ORGADMIN"}
)


class InvalidIdentifier(ValueError):
    """Raised when a configured object name could not be a Snowflake identifier."""


def check_identifier(name: str, *, setting: str) -> str:
    if not IDENTIFIER.match(name):
        raise InvalidIdentifier(
            f"{setting}={name!r} is not a valid Snowflake identifier. "
            "Use letters, digits and underscores, starting with a letter."
        )
    return name


def statements(sql: str) -> list[str]:
    """Split a script into executable statements.

    Snowflake's connector runs one statement per call unless multi-statement is
    explicitly enabled, and enabling it would mean losing the per-statement
    error messages that make a failed grant obvious.
    """
    return [chunk.strip() for chunk in sql.split(";") if chunk.strip()]


def render(settings: SnowflakeSettings, *, sql_path: Path = BOOTSTRAP_SQL) -> list[str]:
    """The bootstrap statements with this account's object names filled in."""
    names = {
        "role": check_identifier(settings.role, setting="SNOWFLAKE_ROLE"),
        "warehouse": check_identifier(settings.warehouse, setting="SNOWFLAKE_WAREHOUSE"),
        "database": check_identifier(settings.database, setting="SNOWFLAKE_DATABASE"),
        "user": check_identifier(settings.user, setting="SNOWFLAKE_USER"),
    }
    # Comments are stripped first: they contain prose with apostrophes and the
    # word "ADTECH_WH", and leaving them in means splitting on a semicolon that
    # might appear inside one.
    body = "\n".join(
        line
        for line in sql_path.read_text(encoding="utf-8").splitlines()
        if not line.lstrip().startswith("--")
    )
    rendered = [statement.format(**names) for statement in statements(body)]

    if names["role"].upper() in SYSTEM_ROLES:
        rendered = [
            statement
            for statement in rendered
            if not statement.startswith(("CREATE ROLE", "GRANT ROLE"))
        ]
    return rendered


def run(settings: SnowflakeSettings | None = None, *, dry_run: bool = False) -> int:
    """Create the objects. Returns the number of statements executed."""
    resolved = (settings or SnowflakeSettings.from_env()).require()
    script = render(resolved)

    if dry_run:
        for statement in script:
            print(f"{statement};\n")
        return len(script)

    # ACCOUNTADMIN only for this: CREATE ROLE and CREATE WAREHOUSE are account
    # level. The database is not yet guaranteed to exist, so the connection
    # deliberately names neither it nor a schema.
    connection = connect(resolved, role="ACCOUNTADMIN", database=None, schema=None)
    try:
        with connection.cursor() as cursor:
            for statement in script:
                logger.info("bootstrap", extra={"statement": statement.split("\n")[0][:80]})
                cursor.execute(statement)
    finally:
        connection.close()

    return len(script)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Create the Snowflake role, warehouse, database, schemas and stage."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the statements with this account's names filled in, and connect to nothing.",
    )
    args = parser.parse_args(argv)

    try:
        count = run(dry_run=args.dry_run)
    except (WarehouseNotConfigured, InvalidIdentifier) as exc:
        print(f"\n{exc}\n", file=sys.stderr)
        return 2

    settings = SnowflakeSettings.from_env()
    verb = "would run" if args.dry_run else "ran"
    print(f"{verb} {count} statement(s) against {settings.describe()}")
    if not args.dry_run:
        print(
            f"\nReady. {settings.database} has RAW, STAGING, CORE and ANALYTICS, "
            f"and {settings.role} can write to all four.\nNext: make warehouse-load"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
