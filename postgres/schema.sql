-- =============================================================================
-- Programmatic Advertising Data Platform - operational source schema
-- =============================================================================
-- This is the OLTP system of record that the rest of the platform reads from.
-- It is executed automatically on first container start (see docker-compose.yml)
-- and is safe to re-run by hand.
--
-- Conventions
--   * Every table has a UUID surrogate primary key supplied by the application.
--   * Enumerations are modelled as CHECK constraints rather than PostgreSQL ENUM
--     types: CHECK constraints survive Debezium -> Kafka -> Spark -> Snowflake as
--     plain strings and can be altered without a table rewrite.
--   * created_at / updated_at are business timestamps produced by the generator,
--     not database defaults, so that the full history is reproducible.
--   * Secondary indexes for the high-volume event tables live in indexes.sql and
--     are applied after bulk load.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS platform;

COMMENT ON SCHEMA public IS 'Operational AdTech source system (the CDC capture surface).';
COMMENT ON SCHEMA platform IS 'Platform bookkeeping. Deliberately outside the CDC capture surface.';

-- -----------------------------------------------------------------------------
-- Reference data
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_country (
    country_code    CHAR(2)      PRIMARY KEY,
    country_name    VARCHAR(100) NOT NULL UNIQUE,
    currency_code   CHAR(3)      NOT NULL,
    region          VARCHAR(50)  NOT NULL
);

COMMENT ON TABLE ref_country IS 'Grain: one row per supported country. Source of truth for the country -> currency mapping used across the platform.';

CREATE TABLE IF NOT EXISTS ref_industry (
    industry        VARCHAR(100) PRIMARY KEY,
    industry_group  VARCHAR(100) NOT NULL
);

COMMENT ON TABLE ref_industry IS 'Grain: one row per advertiser industry vertical.';

-- -----------------------------------------------------------------------------
-- 1. advertisers
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS advertisers (
    advertiser_id     UUID         PRIMARY KEY,
    advertiser_name   VARCHAR(255) NOT NULL,
    industry          VARCHAR(100) NOT NULL,
    billing_country   VARCHAR(100) NOT NULL,
    billing_currency  CHAR(3)      NOT NULL,
    account_status    VARCHAR(30)  NOT NULL,
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP    NOT NULL,
    CONSTRAINT ck_advertisers_status
        CHECK (account_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    CONSTRAINT ck_advertisers_currency
        CHECK (billing_currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_advertisers_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE advertisers IS 'Grain: one row per advertiser (brand) account.';

CREATE INDEX IF NOT EXISTS ix_advertisers_status     ON advertisers (account_status);
CREATE INDEX IF NOT EXISTS ix_advertisers_industry   ON advertisers (industry);
CREATE INDEX IF NOT EXISTS ix_advertisers_updated_at ON advertisers (updated_at);

-- -----------------------------------------------------------------------------
-- 2. campaigns
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id         UUID          PRIMARY KEY,
    advertiser_id       UUID          NOT NULL REFERENCES advertisers (advertiser_id),
    campaign_name       VARCHAR(255)  NOT NULL,
    campaign_objective  VARCHAR(50)   NOT NULL,
    campaign_status     VARCHAR(30)   NOT NULL,
    campaign_budget     DECIMAL(14,2) NOT NULL,
    daily_budget        DECIMAL(14,2) NOT NULL,
    start_date          DATE          NOT NULL,
    end_date            DATE          NOT NULL,
    bid_strategy        VARCHAR(50)   NOT NULL,
    created_at          TIMESTAMP     NOT NULL,
    updated_at          TIMESTAMP     NOT NULL,
    CONSTRAINT ck_campaigns_objective
        CHECK (campaign_objective IN ('AWARENESS', 'TRAFFIC', 'ENGAGEMENT', 'CONVERSIONS', 'APP_INSTALLS')),
    CONSTRAINT ck_campaigns_status
        CHECK (campaign_status IN ('DRAFT', 'ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED')),
    CONSTRAINT ck_campaigns_bid_strategy
        CHECK (bid_strategy IN ('CPC', 'CPM', 'CPA', 'TARGET_ROAS', 'MAX_CONVERSIONS')),
    CONSTRAINT ck_campaigns_dates
        CHECK (start_date < end_date),
    CONSTRAINT ck_campaigns_budget_positive
        CHECK (campaign_budget > 0 AND daily_budget > 0),
    CONSTRAINT ck_campaigns_daily_budget_within_total
        CHECK (daily_budget <= campaign_budget),
    CONSTRAINT ck_campaigns_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE campaigns IS 'Grain: one row per campaign. A campaign belongs to exactly one advertiser.';

CREATE INDEX IF NOT EXISTS ix_campaigns_advertiser ON campaigns (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_campaigns_status     ON campaigns (campaign_status);
CREATE INDEX IF NOT EXISTS ix_campaigns_dates      ON campaigns (start_date, end_date);
CREATE INDEX IF NOT EXISTS ix_campaigns_updated_at ON campaigns (updated_at);

-- -----------------------------------------------------------------------------
-- 3. line_items
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS line_items (
    line_item_id       UUID          PRIMARY KEY,
    campaign_id        UUID          NOT NULL REFERENCES campaigns (campaign_id),
    line_item_name     VARCHAR(255)  NOT NULL,
    line_item_status   VARCHAR(30)   NOT NULL,
    bid_amount         DECIMAL(12,4) NOT NULL,
    bid_currency       CHAR(3)       NOT NULL,
    optimization_goal  VARCHAR(50)   NOT NULL,
    target_cpm         DECIMAL(12,4) NOT NULL,
    frequency_cap      INTEGER       NOT NULL,
    start_date         DATE          NOT NULL,
    end_date           DATE          NOT NULL,
    created_at         TIMESTAMP     NOT NULL,
    updated_at         TIMESTAMP     NOT NULL,
    CONSTRAINT ck_line_items_status
        CHECK (line_item_status IN ('DRAFT', 'ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED')),
    CONSTRAINT ck_line_items_optimization_goal
        CHECK (optimization_goal IN ('CLICKS', 'CONVERSIONS', 'REVENUE', 'REACH', 'IMPRESSIONS')),
    CONSTRAINT ck_line_items_dates
        CHECK (start_date <= end_date),
    CONSTRAINT ck_line_items_prices_positive
        CHECK (bid_amount > 0 AND target_cpm > 0),
    CONSTRAINT ck_line_items_frequency_cap
        CHECK (frequency_cap > 0),
    CONSTRAINT ck_line_items_currency
        CHECK (bid_currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_line_items_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE line_items IS 'Grain: one row per line item (flight) inside a campaign.';

CREATE INDEX IF NOT EXISTS ix_line_items_campaign   ON line_items (campaign_id);
CREATE INDEX IF NOT EXISTS ix_line_items_status     ON line_items (line_item_status);
CREATE INDEX IF NOT EXISTS ix_line_items_updated_at ON line_items (updated_at);

-- -----------------------------------------------------------------------------
-- 4. creatives
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS creatives (
    creative_id       UUID         PRIMARY KEY,
    advertiser_id     UUID         NOT NULL REFERENCES advertisers (advertiser_id),
    creative_name     VARCHAR(255) NOT NULL,
    creative_type     VARCHAR(30)  NOT NULL,
    creative_format   VARCHAR(50)  NOT NULL,
    landing_page_url  TEXT         NOT NULL,
    creative_status   VARCHAR(30)  NOT NULL,
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP    NOT NULL,
    CONSTRAINT ck_creatives_type
        CHECK (creative_type IN ('BANNER', 'VIDEO', 'NATIVE', 'AUDIO')),
    CONSTRAINT ck_creatives_status
        CHECK (creative_status IN ('ACTIVE', 'PAUSED', 'REJECTED', 'EXPIRED')),
    CONSTRAINT ck_creatives_url
        CHECK (landing_page_url LIKE 'https://%'),
    CONSTRAINT ck_creatives_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE creatives IS 'Grain: one row per creative asset. A creative belongs to exactly one advertiser.';

CREATE INDEX IF NOT EXISTS ix_creatives_advertiser ON creatives (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_creatives_type       ON creatives (creative_type);
CREATE INDEX IF NOT EXISTS ix_creatives_status     ON creatives (creative_status);
CREATE INDEX IF NOT EXISTS ix_creatives_updated_at ON creatives (updated_at);

-- -----------------------------------------------------------------------------
-- 5. publishers
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id      UUID         PRIMARY KEY,
    publisher_name    VARCHAR(255) NOT NULL,
    publisher_type    VARCHAR(30)  NOT NULL,
    country           VARCHAR(100) NOT NULL,
    domain            VARCHAR(255) NOT NULL UNIQUE,
    publisher_status  VARCHAR(30)  NOT NULL,
    created_at        TIMESTAMP    NOT NULL,
    updated_at        TIMESTAMP    NOT NULL,
    CONSTRAINT ck_publishers_type
        CHECK (publisher_type IN ('WEBSITE', 'MOBILE_APP', 'CTV', 'AUDIO')),
    CONSTRAINT ck_publishers_status
        CHECK (publisher_status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    CONSTRAINT ck_publishers_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE publishers IS 'Grain: one row per supply-side publisher (site, app, CTV or audio property).';

CREATE INDEX IF NOT EXISTS ix_publishers_type       ON publishers (publisher_type);
CREATE INDEX IF NOT EXISTS ix_publishers_status     ON publishers (publisher_status);
CREATE INDEX IF NOT EXISTS ix_publishers_country    ON publishers (country);
CREATE INDEX IF NOT EXISTS ix_publishers_updated_at ON publishers (updated_at);

-- -----------------------------------------------------------------------------
-- 6. placements
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS placements (
    placement_id    UUID          PRIMARY KEY,
    publisher_id    UUID          NOT NULL REFERENCES publishers (publisher_id),
    placement_name  VARCHAR(255)  NOT NULL,
    placement_type  VARCHAR(50)   NOT NULL,
    ad_format       VARCHAR(50)   NOT NULL,
    floor_price     DECIMAL(12,4) NOT NULL,
    currency        CHAR(3)       NOT NULL,
    device_type     VARCHAR(30)   NOT NULL,
    created_at      TIMESTAMP     NOT NULL,
    updated_at      TIMESTAMP     NOT NULL,
    CONSTRAINT ck_placements_type
        CHECK (placement_type IN ('HEADER', 'SIDEBAR', 'IN_FEED', 'VIDEO_PRE_ROLL',
                                  'VIDEO_MID_ROLL', 'APP_BANNER', 'APP_INTERSTITIAL')),
    CONSTRAINT ck_placements_device
        CHECK (device_type IN ('DESKTOP', 'MOBILE', 'TABLET', 'CTV', 'SMART_SPEAKER')),
    CONSTRAINT ck_placements_floor_price
        CHECK (floor_price >= 0),
    CONSTRAINT ck_placements_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_placements_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE placements IS 'Grain: one row per ad slot offered by a publisher. floor_price is a CPM.';

CREATE INDEX IF NOT EXISTS ix_placements_publisher  ON placements (publisher_id);
CREATE INDEX IF NOT EXISTS ix_placements_type       ON placements (placement_type);
CREATE INDEX IF NOT EXISTS ix_placements_device     ON placements (device_type);
CREATE INDEX IF NOT EXISTS ix_placements_updated_at ON placements (updated_at);

-- -----------------------------------------------------------------------------
-- 7. audiences
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS audiences (
    audience_id        UUID         PRIMARY KEY,
    audience_name      VARCHAR(255) NOT NULL,
    audience_type      VARCHAR(50)  NOT NULL,
    min_age            INTEGER      NOT NULL,
    max_age            INTEGER      NOT NULL,
    gender             VARCHAR(20)  NOT NULL,
    country            VARCHAR(100) NOT NULL,
    interest_category  VARCHAR(100) NOT NULL,
    created_at         TIMESTAMP    NOT NULL,
    updated_at         TIMESTAMP    NOT NULL,
    CONSTRAINT ck_audiences_type
        CHECK (audience_type IN ('DEMOGRAPHIC', 'INTEREST', 'BEHAVIORAL', 'LOOKALIKE', 'RETARGETING')),
    CONSTRAINT ck_audiences_gender
        CHECK (gender IN ('MALE', 'FEMALE', 'ALL')),
    CONSTRAINT ck_audiences_age_range
        CHECK (min_age < max_age),
    CONSTRAINT ck_audiences_age_bounds
        CHECK (min_age >= 13 AND max_age <= 99),
    CONSTRAINT ck_audiences_updated_after_created
        CHECK (updated_at >= created_at)
);

COMMENT ON TABLE audiences IS 'Grain: one row per targetable audience segment.';

CREATE INDEX IF NOT EXISTS ix_audiences_type       ON audiences (audience_type);
CREATE INDEX IF NOT EXISTS ix_audiences_country    ON audiences (country);
CREATE INDEX IF NOT EXISTS ix_audiences_updated_at ON audiences (updated_at);

-- -----------------------------------------------------------------------------
-- 8. impressions  (high volume)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS impressions (
    impression_id        UUID          PRIMARY KEY,
    campaign_id          UUID          NOT NULL REFERENCES campaigns (campaign_id),
    line_item_id         UUID          NOT NULL REFERENCES line_items (line_item_id),
    advertiser_id        UUID          NOT NULL REFERENCES advertisers (advertiser_id),
    creative_id          UUID          NOT NULL REFERENCES creatives (creative_id),
    publisher_id         UUID          NOT NULL REFERENCES publishers (publisher_id),
    placement_id         UUID          NOT NULL REFERENCES placements (placement_id),
    audience_id          UUID          NOT NULL REFERENCES audiences (audience_id),
    impression_timestamp TIMESTAMP     NOT NULL,
    device_type          VARCHAR(30)   NOT NULL,
    operating_system     VARCHAR(50)   NOT NULL,
    browser              VARCHAR(50)   NOT NULL,
    country              VARCHAR(100)  NOT NULL,
    city                 VARCHAR(100)  NOT NULL,
    bid_price            DECIMAL(12,6) NOT NULL,
    clearing_price       DECIMAL(12,6) NOT NULL,
    currency             CHAR(3)       NOT NULL,
    viewability_score    DECIMAL(5,4)  NOT NULL,
    created_at           TIMESTAMP     NOT NULL,
    CONSTRAINT ck_impressions_prices_non_negative
        CHECK (bid_price >= 0 AND clearing_price >= 0),
    CONSTRAINT ck_impressions_clearing_below_bid
        CHECK (clearing_price <= bid_price),
    CONSTRAINT ck_impressions_viewability
        CHECK (viewability_score >= 0 AND viewability_score <= 1),
    CONSTRAINT ck_impressions_device
        CHECK (device_type IN ('DESKTOP', 'MOBILE', 'TABLET', 'CTV', 'SMART_SPEAKER')),
    CONSTRAINT ck_impressions_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_impressions_created_after_event
        CHECK (created_at >= impression_timestamp)
);

COMMENT ON TABLE impressions IS 'Grain: one row per served ad impression. bid_price/clearing_price are CPMs; cost of the single impression is clearing_price / 1000.';

-- -----------------------------------------------------------------------------
-- 9. clicks  (high volume)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clicks (
    click_id         UUID         PRIMARY KEY,
    impression_id    UUID         NOT NULL REFERENCES impressions (impression_id),
    campaign_id      UUID         NOT NULL REFERENCES campaigns (campaign_id),
    line_item_id     UUID         NOT NULL REFERENCES line_items (line_item_id),
    advertiser_id    UUID         NOT NULL REFERENCES advertisers (advertiser_id),
    creative_id      UUID         NOT NULL REFERENCES creatives (creative_id),
    click_timestamp  TIMESTAMP    NOT NULL,
    device_type      VARCHAR(30)  NOT NULL,
    country          VARCHAR(100) NOT NULL,
    created_at       TIMESTAMP    NOT NULL,
    CONSTRAINT ck_clicks_device
        CHECK (device_type IN ('DESKTOP', 'MOBILE', 'TABLET', 'CTV', 'SMART_SPEAKER')),
    CONSTRAINT ck_clicks_created_after_event
        CHECK (created_at >= click_timestamp)
);

COMMENT ON TABLE clicks IS 'Grain: one row per click. Always derived from an existing impression; campaign/line item/advertiser/creative are inherited from that impression.';

-- -----------------------------------------------------------------------------
-- 10. conversions  (high volume)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS conversions (
    conversion_id             UUID          PRIMARY KEY,
    click_id                  UUID          NOT NULL REFERENCES clicks (click_id),
    impression_id             UUID          NOT NULL REFERENCES impressions (impression_id),
    campaign_id               UUID          NOT NULL REFERENCES campaigns (campaign_id),
    advertiser_id             UUID          NOT NULL REFERENCES advertisers (advertiser_id),
    conversion_type           VARCHAR(50)   NOT NULL,
    conversion_timestamp      TIMESTAMP     NOT NULL,
    conversion_value          DECIMAL(14,2) NOT NULL,
    currency                  CHAR(3)       NOT NULL,
    attribution_window_hours  INTEGER       NOT NULL,
    created_at                TIMESTAMP     NOT NULL,
    CONSTRAINT ck_conversions_type
        CHECK (conversion_type IN ('PURCHASE', 'SIGNUP', 'LEAD', 'APP_INSTALL', 'SUBSCRIPTION')),
    CONSTRAINT ck_conversions_value_non_negative
        CHECK (conversion_value >= 0),
    CONSTRAINT ck_conversions_window
        CHECK (attribution_window_hours > 0),
    CONSTRAINT ck_conversions_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_conversions_created_after_event
        CHECK (created_at >= conversion_timestamp)
);

COMMENT ON TABLE conversions IS 'Grain: one row per conversion, attributed last-click. Always derived from an existing click.';

-- -----------------------------------------------------------------------------
-- 11. spend_transactions  (high volume)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS spend_transactions (
    spend_transaction_id  UUID          PRIMARY KEY,
    campaign_id           UUID          NOT NULL REFERENCES campaigns (campaign_id),
    line_item_id          UUID          NOT NULL REFERENCES line_items (line_item_id),
    advertiser_id         UUID          NOT NULL REFERENCES advertisers (advertiser_id),
    publisher_id          UUID          NOT NULL REFERENCES publishers (publisher_id),
    impression_id         UUID          NULL REFERENCES impressions (impression_id),
    spend_timestamp       TIMESTAMP     NOT NULL,
    spend_amount          DECIMAL(14,6) NOT NULL,
    currency              CHAR(3)       NOT NULL,
    billing_type          VARCHAR(30)   NOT NULL,
    created_at            TIMESTAMP     NOT NULL,
    CONSTRAINT ck_spend_billing_type
        CHECK (billing_type IN ('CPM', 'CPC', 'CPA')),
    CONSTRAINT ck_spend_amount_non_negative
        CHECK (spend_amount >= 0),
    CONSTRAINT ck_spend_currency
        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_spend_created_after_event
        CHECK (created_at >= spend_timestamp),
    -- CPM spend is rolled up to the hour so impression_id is NULL there; CPC and CPA
    -- spend is always traceable to the impression that started the chain.
    CONSTRAINT ck_spend_impression_by_billing_type
        CHECK ((billing_type = 'CPM' AND impression_id IS NULL)
            OR (billing_type IN ('CPC', 'CPA') AND impression_id IS NOT NULL))
);

COMMENT ON TABLE spend_transactions IS 'Grain: one row per billable spend event. CPM = one row per (campaign, line item, publisher, hour); CPC = one row per click; CPA = one row per conversion.';

-- -----------------------------------------------------------------------------
-- Platform bookkeeping
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS platform.generation_run (
    run_id             UUID         PRIMARY KEY,
    scale_profile      VARCHAR(30)  NOT NULL,
    seed               BIGINT       NOT NULL,
    simulation_end     DATE         NOT NULL,
    started_at         TIMESTAMP    NOT NULL,
    finished_at        TIMESTAMP,
    duration_seconds   DOUBLE PRECISION,
    row_counts         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    validation_status  VARCHAR(20),
    generator_version  VARCHAR(30)  NOT NULL
);

COMMENT ON TABLE platform.generation_run IS 'Grain: one row per data generator execution. Records the seed and realised volumes so any run can be reproduced.';

CREATE TABLE IF NOT EXISTS platform.data_quality_result (
    result_id     BIGSERIAL    PRIMARY KEY,
    run_id        UUID         NOT NULL REFERENCES platform.generation_run (run_id) ON DELETE CASCADE,
    check_name    VARCHAR(120) NOT NULL,
    check_type    VARCHAR(40)  NOT NULL,
    target_table  VARCHAR(80)  NOT NULL,
    severity      VARCHAR(20)  NOT NULL,
    status        VARCHAR(20)  NOT NULL,
    observed      BIGINT       NOT NULL,
    threshold     BIGINT,
    details       TEXT,
    executed_at   TIMESTAMP    NOT NULL DEFAULT now(),
    CONSTRAINT ck_dq_status CHECK (status IN ('PASS', 'WARN', 'FAIL')),
    CONSTRAINT ck_dq_severity CHECK (severity IN ('ERROR', 'WARNING'))
);

COMMENT ON TABLE platform.data_quality_result IS 'Grain: one row per data quality check execution.';

CREATE INDEX IF NOT EXISTS ix_dq_result_run ON platform.data_quality_result (run_id, status);

CREATE TABLE IF NOT EXISTS platform.cdc_heartbeat (
    id       INTEGER   PRIMARY KEY,
    beat_at  TIMESTAMP NOT NULL DEFAULT now(),
    beats    BIGINT    NOT NULL DEFAULT 0
);

COMMENT ON TABLE platform.cdc_heartbeat IS
'Single-row table the CDC connector updates on a timer. A replication slot only releases write-ahead log once the consumer confirms how far it has read, and it can only confirm a position it has actually seen. If the captured tables sit idle while the rest of the database is busy, the slot never advances and the WAL grows without bound until the disk fills. Writing a heartbeat keeps a steady trickle of activity flowing so the slot always has something to confirm.';

INSERT INTO platform.cdc_heartbeat (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Change-data-capture readiness
-- -----------------------------------------------------------------------------
-- REPLICA IDENTITY FULL makes PostgreSQL write the complete pre-image of every
-- UPDATE/DELETE into the WAL, which is what lets a downstream CDC reader emit a
-- populated "before" payload. Setting it now means the source database never has
-- to be reconfigured or restarted when the CDC phase is wired up.
-- It is only applied to the low-volume mutable tables: the event tables are
-- append-only, so the extra WAL volume would buy nothing.

ALTER TABLE advertisers REPLICA IDENTITY FULL;
ALTER TABLE campaigns   REPLICA IDENTITY FULL;
ALTER TABLE line_items  REPLICA IDENTITY FULL;
ALTER TABLE creatives   REPLICA IDENTITY FULL;
ALTER TABLE publishers  REPLICA IDENTITY FULL;
ALTER TABLE placements  REPLICA IDENTITY FULL;
ALTER TABLE audiences   REPLICA IDENTITY FULL;
