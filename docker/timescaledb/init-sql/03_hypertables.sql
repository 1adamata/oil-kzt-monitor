-- =========================================================
-- Table 1: raw_oil_prices
-- Every individual oil price tick, exactly as ingested.
-- =========================================================
CREATE TABLE md.raw_oil_prices (
    event_time_utc  TIMESTAMPTZ      NOT NULL,
    instrument_id   TEXT             NOT NULL,
    price           DOUBLE PRECISION NOT NULL,
    source          TEXT             NOT NULL,
    quality_flag    SMALLINT         NOT NULL DEFAULT 0,
    ingest_time_utc TIMESTAMPTZ      NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'md.raw_oil_prices',
    by_range('event_time_utc', INTERVAL '7 days')
);

CREATE INDEX ix_raw_oil_prices_instrument_time
    ON md.raw_oil_prices (instrument_id, event_time_utc DESC);


-- =========================================================
-- Table 2: raw_fx_rates
-- Every individual FX rate tick.
-- =========================================================
CREATE TABLE md.raw_fx_rates (
    event_time_utc  TIMESTAMPTZ      NOT NULL,
    instrument_id   TEXT             NOT NULL,
    rate            DOUBLE PRECISION NOT NULL,
    source          TEXT             NOT NULL,
    quality_flag    SMALLINT         NOT NULL DEFAULT 0,
    ingest_time_utc TIMESTAMPTZ      NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'md.raw_fx_rates',
    by_range('event_time_utc', INTERVAL '7 days')
);

CREATE INDEX ix_raw_fx_rates_instrument_time
    ON md.raw_fx_rates (instrument_id, event_time_utc DESC);


-- =========================================================
-- Table 3: curated_aligned_bars
-- Oil and FX joined into common time buckets, ready for analytics.
-- =========================================================
CREATE TABLE md.curated_aligned_bars (
    bucket_time     TIMESTAMPTZ      NOT NULL,
    horizon         TEXT             NOT NULL,
    oil_symbol      TEXT             NOT NULL,
    oil_price       DOUBLE PRECISION,
    oil_quality     SMALLINT,
    usdkzt          DOUBLE PRECISION,
    fx_quality      SMALLINT,
    oil_logret      DOUBLE PRECISION,
    kzt_logret      DOUBLE PRECISION,
    ingest_time_utc TIMESTAMPTZ      NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'md.curated_aligned_bars',
    by_range('bucket_time', INTERVAL '7 days')
);

CREATE INDEX ix_curated_aligned_bars_horizon_symbol_time
    ON md.curated_aligned_bars (horizon, oil_symbol, bucket_time DESC);


-- =========================================================
-- Table 4: relationship_metrics
-- Computed metrics: correlation, beta, spread z-score, BOCPD probability, etc.
-- =========================================================
CREATE TABLE md.relationship_metrics (
    bucket_time TIMESTAMPTZ      NOT NULL,
    horizon     TEXT             NOT NULL,
    oil_symbol  TEXT             NOT NULL,
    metric      TEXT             NOT NULL,
    value       DOUBLE PRECISION,
    meta        JSONB
);

SELECT create_hypertable(
    'md.relationship_metrics',
    by_range('bucket_time', INTERVAL '7 days')
);

CREATE INDEX ix_relationship_metrics_lookup
    ON md.relationship_metrics (metric, horizon, oil_symbol, bucket_time DESC);


-- =========================================================
-- Table 5: alerts
-- History of fired alerts with severity and resolution time.
-- =========================================================
CREATE TABLE md.alerts (
    fired_at    TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    kind        TEXT        NOT NULL,
    severity    TEXT        NOT NULL,
    oil_symbol  TEXT,
    horizon     TEXT,
    description TEXT,
    payload     JSONB
);

SELECT create_hypertable(
    'md.alerts',
    by_range('fired_at', INTERVAL '30 days')
);

CREATE INDEX ix_alerts_fired_at
    ON md.alerts (fired_at DESC);