-- Account objects for the warehouse half of the project.
--
-- Run once, as ACCOUNTADMIN, with `make warehouse-bootstrap`. Everything here
-- is IF NOT EXISTS, so rerunning it is safe and is the fastest way to check an
-- account is in the shape the rest of phase 9 expects.
--
-- Identifiers are parameterised through the Python runner rather than hardcoded
-- so the names in .env stay the single source of truth; a second copy of
-- "ADTECH_WH" in a SQL file is a second place for it to be wrong.

-- A dedicated role rather than loading everything as ACCOUNTADMIN. On a trial
-- account that owns nothing else the distinction looks academic, but a pipeline
-- that runs as the account's superuser is the habit worth not forming - and the
-- grants below are what dbt will inherit.
CREATE ROLE IF NOT EXISTS {role};

-- XSMALL is deliberate. The whole dataset is a few gigabytes, and a larger
-- warehouse would finish the same work in the same wall-clock time while
-- consuming credits proportional to its size. Trial credits are finite.
--
-- AUTO_SUSPEND at 60s because the default of 600 means ten minutes of billing
-- after a two-minute load. This is the single biggest source of wasted credits
-- on a trial account.
CREATE WAREHOUSE IF NOT EXISTS {warehouse}
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    -- A runaway query should fail rather than bill until someone notices.
    -- Nothing in this project legitimately runs for twenty minutes.
    STATEMENT_TIMEOUT_IN_SECONDS = 1200
    COMMENT = 'Programmatic advertising platform: loads and dbt transformations';

-- The settings again, because IF NOT EXISTS above does nothing when the
-- warehouse is already there - which it is for anyone pointing this at a
-- trial's COMPUTE_WH. Without this, the warehouse keeps whatever AUTO_SUSPEND
-- it was created with, and the default of 600 bills ten idle minutes after
-- every two-minute load. That is the single largest credit leak on a trial.
ALTER WAREHOUSE {warehouse} SET
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    STATEMENT_TIMEOUT_IN_SECONDS = 1200;

CREATE DATABASE IF NOT EXISTS {database}
    COMMENT = 'Programmatic advertising platform, phases 9-13';

-- The four layers. Separate schemas rather than name prefixes in one schema so
-- that grants, and dbt's target schemas, have something to attach to.
CREATE SCHEMA IF NOT EXISTS {database}.RAW
    COMMENT = 'Landing copy of Iceberg Silver. Loaded by warehouse.load, never transformed in place.';
CREATE SCHEMA IF NOT EXISTS {database}.STAGING
    COMMENT = 'Typed, renamed views over RAW. Built by dbt.';
CREATE SCHEMA IF NOT EXISTS {database}.CORE
    COMMENT = 'Dimensions and fact tables. Built by dbt.';
CREATE SCHEMA IF NOT EXISTS {database}.ANALYTICS
    COMMENT = 'Aggregated data products. Built by dbt, read by BI.';

GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {role};
GRANT OPERATE ON WAREHOUSE {warehouse} TO ROLE {role};
GRANT USAGE ON DATABASE {database} TO ROLE {role};

-- ALL on the schemas: this role creates, replaces and drops tables in every
-- layer, which is what dbt does on every run.
GRANT ALL ON SCHEMA {database}.RAW TO ROLE {role};
GRANT ALL ON SCHEMA {database}.STAGING TO ROLE {role};
GRANT ALL ON SCHEMA {database}.CORE TO ROLE {role};
GRANT ALL ON SCHEMA {database}.ANALYTICS TO ROLE {role};

-- Future grants so tables created later are readable without rerunning this.
-- Without them, every dbt run would produce objects the role can write but a
-- reader cannot select from.
GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.RAW TO ROLE {role};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.STAGING TO ROLE {role};
GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.CORE TO ROLE {role};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.CORE TO ROLE {role};
GRANT SELECT ON FUTURE TABLES IN SCHEMA {database}.ANALYTICS TO ROLE {role};
GRANT SELECT ON FUTURE VIEWS IN SCHEMA {database}.ANALYTICS TO ROLE {role};

GRANT ROLE {role} TO USER {user};

-- An internal named stage, so loading needs no cloud storage of its own.
-- Snowflake external volumes require real S3, Azure or GCS; MinIO on a laptop
-- is not reachable from Snowflake, so PUT to an internal stage is the only
-- route that works without provisioning cloud infrastructure.
CREATE STAGE IF NOT EXISTS {database}.RAW.SILVER_STAGE
    FILE_FORMAT = (TYPE = PARQUET)
    COMMENT = 'Parquet exported from Iceberg Silver, uploaded by PUT.';

GRANT ALL ON STAGE {database}.RAW.SILVER_STAGE TO ROLE {role};
