-- =============================================================================
-- Reference data seed
-- =============================================================================
-- GENERATED FILE - do not edit by hand.
-- Source: data_generator/reference/geography.yml, data_generator/reference/business.yml
-- Regenerate with: python scripts/render_seed_sql.py
--
-- Seeding these tables from the same YAML the generator reads means the database
-- and the generator can never disagree about which countries exist or which
-- currency a country bills in. The data quality suite uses ref_country to verify
-- that every impression resolves to a known market.
-- =============================================================================


INSERT INTO ref_country (country_code, country_name, currency_code, region) VALUES
    ('US', 'United States', 'USD', 'NORTH_AMERICA'),
    ('CA', 'Canada', 'CAD', 'NORTH_AMERICA'),
    ('GB', 'United Kingdom', 'GBP', 'EUROPE'),
    ('DE', 'Germany', 'EUR', 'EUROPE'),
    ('FR', 'France', 'EUR', 'EUROPE'),
    ('ES', 'Spain', 'EUR', 'EUROPE'),
    ('IT', 'Italy', 'EUR', 'EUROPE'),
    ('NL', 'Netherlands', 'EUR', 'EUROPE'),
    ('SE', 'Sweden', 'SEK', 'EUROPE'),
    ('PL', 'Poland', 'PLN', 'EUROPE'),
    ('BR', 'Brazil', 'BRL', 'LATAM'),
    ('MX', 'Mexico', 'MXN', 'LATAM'),
    ('IN', 'India', 'INR', 'APAC'),
    ('JP', 'Japan', 'JPY', 'APAC'),
    ('AU', 'Australia', 'AUD', 'APAC'),
    ('SG', 'Singapore', 'SGD', 'APAC'),
    ('AE', 'United Arab Emirates', 'AED', 'MEA'),
    ('ZA', 'South Africa', 'ZAR', 'MEA')
ON CONFLICT (country_code) DO UPDATE SET
    country_name  = EXCLUDED.country_name,
    currency_code = EXCLUDED.currency_code,
    region        = EXCLUDED.region;

INSERT INTO ref_industry (industry, industry_group) VALUES
    ('Retail', 'Commerce'),
    ('Ecommerce', 'Commerce'),
    ('Consumer Electronics', 'Commerce'),
    ('Fashion', 'Commerce'),
    ('Automotive', 'Consumer'),
    ('Travel', 'Consumer'),
    ('Food and Beverage', 'Consumer'),
    ('Financial Services', 'Regulated'),
    ('Insurance', 'Regulated'),
    ('Healthcare', 'Regulated'),
    ('Pharmaceuticals', 'Regulated'),
    ('Telecommunications', 'Services'),
    ('Software', 'Technology'),
    ('Gaming', 'Technology'),
    ('Education', 'Services'),
    ('Media and Streaming', 'Media'),
    ('Real Estate', 'Services'),
    ('Nonprofit', 'Public Sector')
ON CONFLICT (industry) DO UPDATE SET
    industry_group = EXCLUDED.industry_group;
