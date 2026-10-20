-- =============================================================================
-- Business metrics on the star schema.
-- Run with:  docker compose exec -T postgres psql -U taxi -d taxi_dw < sql/analytics_queries.sql
-- (The Streamlit dashboard runs the same logic with date / borough filters, see dashboard/queries.py.)
-- =============================================================================

\echo '--- 1. Average fare amount per mile, by month ---'
-- Distance-weighted: SUM(fare) / SUM(miles). A plain AVG(fare / miles) is dominated
-- by very short trips (a $3 minimum fare over 0.05 miles = $60/mile).
SELECT d.year_month,
       COUNT(*)                                                            AS trips,
       ROUND(SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS avg_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_date d ON d.date_key = f.pickup_date_key
GROUP BY ROLLUP (d.year_month)
ORDER BY d.year_month NULLS LAST;

\echo '--- 1b. Average fare amount per mile, by pickup borough ---'
SELECT l.borough,
       COUNT(*)                                                            AS trips,
       ROUND(SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS avg_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_location l ON l.location_key = f.pickup_location_key
GROUP BY l.borough
ORDER BY trips DESC;

\echo '--- 2. Peak hours for taxi rides (by pickup hour) ---'
WITH hourly AS (
    SELECT t.hour_of_day,
           COUNT(*) AS trips
    FROM fact_taxi_trips f
    JOIN dim_time t ON t.time_key = f.pickup_time_key
    GROUP BY t.hour_of_day
)
SELECT RANK() OVER (ORDER BY trips DESC)              AS peak_rank,
       LPAD(hour_of_day::TEXT, 2, '0') || ':00'       AS pickup_hour,
       trips,
       ROUND(100.0 * trips / SUM(trips) OVER (), 2)   AS pct_of_trips
FROM hourly
ORDER BY peak_rank;

\echo '--- 2b. Peak hour on weekdays vs weekends ---'
WITH hourly AS (
    SELECT CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
           t.hour_of_day,
           COUNT(*) AS trips
    FROM fact_taxi_trips f
    JOIN dim_date d ON d.date_key = f.pickup_date_key
    JOIN dim_time t ON t.time_key = f.pickup_time_key
    GROUP BY 1, 2
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY day_type ORDER BY trips DESC) AS rnk
    FROM hourly
)
SELECT day_type, LPAD(hour_of_day::TEXT, 2, '0') || ':00' AS peak_hour, trips
FROM ranked
WHERE rnk <= 3
ORDER BY day_type, rnk;

\echo '--- 3. Total revenue by payment type ---'
SELECT p.payment_type_desc,
       COUNT(*)                                                     AS trips,
       SUM(f.total_amount)                                          AS total_revenue,
       ROUND(100.0 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (), 2) AS pct_of_revenue,
       ROUND(AVG(f.total_amount), 2)                                AS avg_ticket
FROM fact_taxi_trips f
JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
GROUP BY p.payment_type_desc
ORDER BY total_revenue DESC;

\echo '--- Pipeline runs (latest 10) ---'
SELECT source_file, status, started_at, finished_at - started_at AS duration,
       rows_extracted, rows_rejected, rows_loaded
FROM etl_run_log
ORDER BY started_at DESC
LIMIT 10;
