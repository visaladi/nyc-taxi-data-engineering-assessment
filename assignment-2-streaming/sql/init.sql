-- =============================================================================
-- Streaming sink schema (TimescaleDB / PostgreSQL 16). Idempotent.
--
--   trip_events_raw   append-only raw events (hypertable, partitioned by event time)
--   trip_window_agg   sliding-window aggregates, upserted incrementally by the processor
--   v_trip_window_metrics  derived ratios for dashboards
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------------------------------------
-- Raw events. event_id is deterministic (derived from the source row by the
-- producer), so replays and redeliveries are de-duplicated by the primary key.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trip_events_raw (
    event_id          UUID          NOT NULL,
    event_time        TIMESTAMPTZ   NOT NULL,          -- trip drop-off = when the event happened
    pickup_time       TIMESTAMPTZ   NOT NULL,
    vendor_id         SMALLINT,
    pu_location_id    SMALLINT,
    do_location_id    SMALLINT,
    pu_borough        TEXT          NOT NULL,
    passenger_count   SMALLINT,
    trip_distance     NUMERIC(8, 2),
    payment_type      SMALLINT,
    fare_amount       NUMERIC(10, 2),
    tip_amount        NUMERIC(10, 2),
    total_amount      NUMERIC(10, 2),
    produced_at       TIMESTAMPTZ,                     -- wall clock at the producer
    ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    kafka_partition   SMALLINT      NOT NULL,
    kafka_offset      BIGINT        NOT NULL,
    is_late           BOOLEAN       NOT NULL DEFAULT FALSE,   -- arrived after its windows closed
    PRIMARY KEY (event_id, event_time)                 -- hypertable PKs must include the time column
);

SELECT create_hypertable('trip_events_raw', by_range('event_time', INTERVAL '1 day'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_trip_events_raw_borough_time ON trip_events_raw (pu_borough, event_time DESC);

-- Columnar compression for chunks that are no longer written to. (No policy is added locally
-- because the replayed data is from 2023 and would be compressed immediately; see README.)
ALTER TABLE trip_events_raw SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pu_borough',
    timescaledb.compress_orderby = 'event_time DESC'
);

-- -----------------------------------------------------------------------------
-- Sliding-window aggregates: one row per (window, pickup borough), plus an 'ALL'
-- row per window. Measures are additive so the processor can upsert increments.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trip_window_agg (
    window_start      TIMESTAMPTZ   NOT NULL,
    window_end        TIMESTAMPTZ   NOT NULL,
    pu_borough        TEXT          NOT NULL,
    trip_count        BIGINT        NOT NULL DEFAULT 0,
    passenger_count   BIGINT        NOT NULL DEFAULT 0,
    total_distance    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    total_fare        NUMERIC(14, 2) NOT NULL DEFAULT 0,
    total_tip         NUMERIC(14, 2) NOT NULL DEFAULT 0,
    total_revenue     NUMERIC(14, 2) NOT NULL DEFAULT 0,
    update_count      INTEGER       NOT NULL DEFAULT 1,   -- how many micro-batches touched the window
    is_final          BOOLEAN       NOT NULL DEFAULT FALSE,  -- watermark passed window_end
    first_updated_at  TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, pu_borough)
);

CREATE INDEX IF NOT EXISTS ix_trip_window_agg_open ON trip_window_agg (window_end) WHERE NOT is_final;

CREATE OR REPLACE VIEW v_trip_window_metrics AS
SELECT window_start,
       window_end,
       pu_borough,
       trip_count,
       total_revenue,
       ROUND(total_revenue / NULLIF(trip_count, 0), 2)    AS avg_revenue_per_trip,
       ROUND(total_fare / NULLIF(total_distance, 0), 2)   AS fare_per_mile,
       ROUND(total_distance / NULLIF(trip_count, 0), 2)   AS avg_distance,
       ROUND(100 * total_tip / NULLIF(total_fare, 0), 1)  AS tip_pct_of_fare,
       is_final,
       update_count,
       last_updated_at
FROM trip_window_agg;
