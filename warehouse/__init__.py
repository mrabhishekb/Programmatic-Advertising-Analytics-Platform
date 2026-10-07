"""The Snowflake warehouse: loading Silver into RAW, and the dbt project above it.

Named ``warehouse`` rather than ``snowflake`` on purpose. A top-level package
called ``snowflake`` shadows ``snowflake-connector-python``, so ``import
snowflake.connector`` would resolve to this directory and fail with a confusing
``ModuleNotFoundError`` naming a package that is definitely installed.
"""
