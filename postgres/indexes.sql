-- =============================================================================
-- Secondary indexes for the high-volume event tables.
-- =============================================================================
-- These are applied AFTER the bulk load (the loader runs this file automatically
-- unless --skip-indexes is passed). Building them once at the end is dramatically
-- cheaper than maintaining eight B-trees during a multi-million row COPY.
--
-- The file is idempotent, so re-running the loader is safe.
-- =============================================================================

CREATE INDEX IF NOT EXISTS ix_impressions_campaign   ON impressions (campaign_id);
CREATE INDEX IF NOT EXISTS ix_impressions_line_item  ON impressions (line_item_id);
CREATE INDEX IF NOT EXISTS ix_impressions_advertiser ON impressions (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_impressions_creative   ON impressions (creative_id);
CREATE INDEX IF NOT EXISTS ix_impressions_publisher  ON impressions (publisher_id);
CREATE INDEX IF NOT EXISTS ix_impressions_placement  ON impressions (placement_id);
CREATE INDEX IF NOT EXISTS ix_impressions_audience   ON impressions (audience_id);
CREATE INDEX IF NOT EXISTS ix_impressions_timestamp  ON impressions (impression_timestamp);
-- Supports the "campaign performance over a date range" access pattern that every
-- downstream layer uses.
CREATE INDEX IF NOT EXISTS ix_impressions_campaign_ts ON impressions (campaign_id, impression_timestamp);

CREATE INDEX IF NOT EXISTS ix_clicks_impression ON clicks (impression_id);
CREATE INDEX IF NOT EXISTS ix_clicks_campaign   ON clicks (campaign_id);
CREATE INDEX IF NOT EXISTS ix_clicks_line_item  ON clicks (line_item_id);
CREATE INDEX IF NOT EXISTS ix_clicks_advertiser ON clicks (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_clicks_creative   ON clicks (creative_id);
CREATE INDEX IF NOT EXISTS ix_clicks_timestamp  ON clicks (click_timestamp);

CREATE INDEX IF NOT EXISTS ix_conversions_click      ON conversions (click_id);
CREATE INDEX IF NOT EXISTS ix_conversions_impression ON conversions (impression_id);
CREATE INDEX IF NOT EXISTS ix_conversions_campaign   ON conversions (campaign_id);
CREATE INDEX IF NOT EXISTS ix_conversions_advertiser ON conversions (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_conversions_timestamp  ON conversions (conversion_timestamp);

CREATE INDEX IF NOT EXISTS ix_spend_campaign   ON spend_transactions (campaign_id);
CREATE INDEX IF NOT EXISTS ix_spend_line_item  ON spend_transactions (line_item_id);
CREATE INDEX IF NOT EXISTS ix_spend_advertiser ON spend_transactions (advertiser_id);
CREATE INDEX IF NOT EXISTS ix_spend_publisher  ON spend_transactions (publisher_id);
CREATE INDEX IF NOT EXISTS ix_spend_impression ON spend_transactions (impression_id);
CREATE INDEX IF NOT EXISTS ix_spend_timestamp  ON spend_transactions (spend_timestamp);
