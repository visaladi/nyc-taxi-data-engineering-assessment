-- =============================================================================
-- NYC Yellow Taxi - analytical star schema (PostgreSQL 14+)
--
--   fact_taxi_trips  (one row per completed trip, partitioned by pickup month)
--     ├── dim_date          (pickup_date_key, dropoff_date_key)   role-playing
--     ├── dim_time          (pickup_time_key, dropoff_time_key)   role-playing
--     ├── dim_location      (pickup_location_key, dropoff_location_key)
--     ├── dim_vendor
--     ├── dim_rate_code
--     └── dim_payment_type
--
-- etl_run_log is an operational (non-dimensional) audit table used for monitoring.
--
-- The script is idempotent: it is run on every pipeline start.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_date: calendar dimension, pre-generated (static, no ETL needed)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER     PRIMARY KEY,           -- YYYYMMDD
    full_date       DATE        NOT NULL UNIQUE,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(9)  NOT NULL,
    year_month      CHAR(7)     NOT NULL,              -- 'YYYY-MM'
    day_of_month    SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,              -- ISO 8601: 1 = Monday ... 7 = Sunday
    day_name        VARCHAR(9)  NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
);

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name, year_month,
                      day_of_month, day_of_week, day_name, week_of_year, is_weekend)
SELECT TO_CHAR(d, 'YYYYMMDD')::INTEGER,
       d::DATE,
       EXTRACT(YEAR    FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH   FROM d),
       TRIM(TO_CHAR(d, 'Month')),
       TO_CHAR(d, 'YYYY-MM'),
       EXTRACT(DAY     FROM d),
       EXTRACT(ISODOW  FROM d),
       TRIM(TO_CHAR(d, 'Day')),
       EXTRACT(WEEK    FROM d),
       EXTRACT(ISODOW  FROM d) IN (6, 7)
FROM generate_series(DATE '2015-01-01', DATE '2030-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- -----------------------------------------------------------------------------
-- dim_time: minute-of-day dimension (1,440 rows), pre-generated
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_time (
    time_key        SMALLINT    PRIMARY KEY,           -- HHMM, e.g. 1745
    hour_of_day     SMALLINT    NOT NULL,
    minute_of_hour  SMALLINT    NOT NULL,
    hour_label      CHAR(5)     NOT NULL,              -- '17:00'
    am_pm           CHAR(2)     NOT NULL,
    day_part        VARCHAR(16) NOT NULL
);

INSERT INTO dim_time (time_key, hour_of_day, minute_of_hour, hour_label, am_pm, day_part)
SELECT h * 100 + m,
       h,
       m,
       LPAD(h::TEXT, 2, '0') || ':00',
       CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END,
       CASE
           WHEN h BETWEEN 0  AND 5  THEN 'Overnight'
           WHEN h BETWEEN 6  AND 9  THEN 'Morning rush'
           WHEN h BETWEEN 10 AND 15 THEN 'Midday'
           WHEN h BETWEEN 16 AND 19 THEN 'Evening rush'
           ELSE 'Night'
       END
FROM generate_series(0, 23) AS h
CROSS JOIN generate_series(0, 59) AS m
ON CONFLICT (time_key) DO NOTHING;

-- -----------------------------------------------------------------------------
-- dim_location: TLC taxi zones (loaded by the pipeline from taxi_zone_lookup.csv)
-- The TLC LocationID is a stable, TLC-managed code, so it is used as the key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_location (
    location_key    SMALLINT    PRIMARY KEY,           -- TLC LocationID
    borough         VARCHAR(32) NOT NULL,
    zone            VARCHAR(64) NOT NULL,
    service_zone    VARCHAR(32) NOT NULL
);

-- "Unknown" members so facts never violate the FK, even before zones are loaded.
INSERT INTO dim_location (location_key, borough, zone, service_zone) VALUES
    (264, 'Unknown', 'Unknown',        'N/A'),
    (265, 'N/A',     'Outside of NYC', 'N/A')
ON CONFLICT (location_key) DO NOTHING;

-- -----------------------------------------------------------------------------
-- Small code dimensions (values from the TLC yellow trip data dictionary)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_key      SMALLINT    PRIMARY KEY,
    vendor_name     VARCHAR(64) NOT NULL
);

INSERT INTO dim_vendor (vendor_key, vendor_name) VALUES
    (0, 'Unknown'),
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'Curb Mobility, LLC (formerly VeriFone)'),
    (6, 'Myle Technologies Inc'),
    (7, 'Helix')
ON CONFLICT (vendor_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_key   SMALLINT    PRIMARY KEY,
    rate_code_desc  VARCHAR(32) NOT NULL
);

INSERT INTO dim_rate_code (rate_code_key, rate_code_desc) VALUES
    (1,  'Standard rate'),
    (2,  'JFK'),
    (3,  'Newark'),
    (4,  'Nassau or Westchester'),
    (5,  'Negotiated fare'),
    (6,  'Group ride'),
    (99, 'Unknown')
ON CONFLICT (rate_code_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_key  SMALLINT    PRIMARY KEY,
    payment_type_desc VARCHAR(32) NOT NULL,
    payment_group     VARCHAR(16) NOT NULL            -- Card / Cash / Other
);

INSERT INTO dim_payment_type (payment_type_key, payment_type_desc, payment_group) VALUES
    (0, 'Flex fare / not recorded', 'Other'),
    (1, 'Credit card',              'Card'),
    (2, 'Cash',                     'Cash'),
    (3, 'No charge',                'Other'),
    (4, 'Dispute',                  'Other'),
    (5, 'Unknown',                  'Other'),
    (6, 'Voided trip',              'Other')
ON CONFLICT (payment_type_key) DO NOTHING;

-- -----------------------------------------------------------------------------
-- fact_taxi_trips: grain = one completed trip.
-- Range-partitioned by pickup month so each source file maps to exactly one
-- partition: monthly reloads stay cheap and queries filtered by date prune.
--
-- Referential integrity is enforced by the ETL rather than by FOREIGN KEY
-- constraints (standard warehouse practice): row-level FK triggers made the bulk
-- COPY of ~3M rows/month roughly 10x slower. Instead the pipeline
--   1. maps every unknown code to the dimension's 'Unknown' member, and
--   2. runs a set-based orphan check (etl/load.py) before committing each load.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    trip_key                BIGINT GENERATED BY DEFAULT AS IDENTITY,
    pickup_date_key         INTEGER       NOT NULL,    -- -> dim_date.date_key
    pickup_time_key         SMALLINT      NOT NULL,    -- -> dim_time.time_key
    dropoff_date_key        INTEGER       NOT NULL,    -- -> dim_date.date_key
    dropoff_time_key        SMALLINT      NOT NULL,    -- -> dim_time.time_key
    pickup_location_key     SMALLINT      NOT NULL,    -- -> dim_location.location_key
    dropoff_location_key    SMALLINT      NOT NULL,    -- -> dim_location.location_key
    vendor_key              SMALLINT      NOT NULL,    -- -> dim_vendor.vendor_key
    rate_code_key           SMALLINT      NOT NULL,    -- -> dim_rate_code.rate_code_key
    payment_type_key        SMALLINT      NOT NULL,    -- -> dim_payment_type.payment_type_key
    -- degenerate attributes
    pickup_ts               TIMESTAMP     NOT NULL,
    dropoff_ts              TIMESTAMP     NOT NULL,
    store_and_fwd_flag      BOOLEAN,
    -- measures
    passenger_count         SMALLINT,
    trip_distance_miles     NUMERIC(8, 2) NOT NULL,
    trip_duration_minutes   NUMERIC(8, 2) NOT NULL,
    fare_amount             NUMERIC(10, 2) NOT NULL,
    extra                   NUMERIC(10, 2) NOT NULL,
    mta_tax                 NUMERIC(10, 2) NOT NULL,
    tip_amount              NUMERIC(10, 2) NOT NULL,
    tolls_amount            NUMERIC(10, 2) NOT NULL,
    improvement_surcharge   NUMERIC(10, 2) NOT NULL,
    congestion_surcharge    NUMERIC(10, 2) NOT NULL,
    airport_fee             NUMERIC(10, 2) NOT NULL,
    total_amount            NUMERIC(10, 2) NOT NULL,
    -- lineage
    source_file             VARCHAR(64)   NOT NULL,
    loaded_at               TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (pickup_date_key, trip_key)
) PARTITION BY RANGE (pickup_date_key);

COMMENT ON TABLE fact_taxi_trips IS 'One row per completed yellow taxi trip. Partitioned by pickup month.';

-- Indexes on the parent cascade to every partition.
CREATE INDEX IF NOT EXISTS ix_fact_trips_source_file      ON fact_taxi_trips (source_file);
CREATE INDEX IF NOT EXISTS ix_fact_trips_pickup_time      ON fact_taxi_trips (pickup_time_key);
CREATE INDEX IF NOT EXISTS ix_fact_trips_pickup_location  ON fact_taxi_trips (pickup_location_key);
CREATE INDEX IF NOT EXISTS ix_fact_trips_payment_type     ON fact_taxi_trips (payment_type_key);

-- Creates the monthly partition for p_month if it does not exist yet; returns its name.
CREATE OR REPLACE FUNCTION ensure_fact_partition(p_month DATE) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE
    v_start DATE := date_trunc('month', p_month)::DATE;
    v_end   DATE := (date_trunc('month', p_month) + INTERVAL '1 month')::DATE;
    v_name  TEXT := format('fact_taxi_trips_%s', to_char(v_start, 'YYYY_MM'));
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF fact_taxi_trips FOR VALUES FROM (%s) TO (%s)',
        v_name, to_char(v_start, 'YYYYMMDD'), to_char(v_end, 'YYYYMMDD')
    );
    RETURN v_name;
END;
$$;

-- -----------------------------------------------------------------------------
-- agg_trips_hourly: aggregate fact table (date x pickup hour x pickup borough x
-- payment type) with additive measures only. ~25k rows instead of ~6M, so the
-- dashboard answers in milliseconds. Refreshed by the pipeline after each load.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS agg_trips_hourly AS
SELECT f.pickup_date_key,
       t.hour_of_day,
       l.borough                     AS pickup_borough,
       f.payment_type_key,
       COUNT(*)                      AS trips,
       SUM(f.total_amount)           AS revenue,
       SUM(f.fare_amount)            AS fare,
       SUM(f.tip_amount)             AS tips,
       SUM(f.trip_distance_miles)    AS distance_miles,
       SUM(f.trip_duration_minutes)  AS duration_minutes
FROM fact_taxi_trips f
JOIN dim_time t     ON t.time_key = f.pickup_time_key
JOIN dim_location l ON l.location_key = f.pickup_location_key
GROUP BY f.pickup_date_key, t.hour_of_day, l.borough, f.payment_type_key
WITH NO DATA;

-- Unique index: required for REFRESH ... CONCURRENTLY (readers are never blocked).
CREATE UNIQUE INDEX IF NOT EXISTS ux_agg_trips_hourly
    ON agg_trips_hourly (pickup_date_key, hour_of_day, pickup_borough, payment_type_key);

-- -----------------------------------------------------------------------------
-- etl_run_log: one row per (pipeline run, source file). Feeds monitoring and the
-- "pipeline health" section of the dashboard.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id              UUID         PRIMARY KEY,
    flow_run_id         VARCHAR(64),
    source_file         VARCHAR(64)  NOT NULL,
    status              VARCHAR(16)  NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    rows_extracted      BIGINT,
    rows_rejected       BIGINT,
    rows_loaded         BIGINT,
    rejected_by_rule    JSONB,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS ix_etl_run_log_started_at ON etl_run_log (started_at DESC);
