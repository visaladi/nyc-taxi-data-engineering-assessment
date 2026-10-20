"""Parameterised dashboard queries.

They read `agg_trips_hourly` - the hourly aggregate of the star schema refreshed by the pipeline -
so every chart answers in milliseconds. All measures there are additive, so ratios are computed
from sums (e.g. fare per mile = SUM(fare) / SUM(miles)) and stay exact at any filter level.
The same metrics written directly against the fact table are in sql/analytics_queries.sql.

Parameters: :start_key / :end_key (YYYYMMDD pickup date keys, inclusive) and :borough
('All' disables the borough filter).
"""

_FILTERS = """
    a.pickup_date_key BETWEEN :start_key AND :end_key
    AND (:borough = 'All' OR a.pickup_borough = :borough)
"""

DATE_BOUNDS = """
SELECT MIN(pickup_date_key) AS min_key, MAX(pickup_date_key) AS max_key
FROM agg_trips_hourly
"""

BOROUGHS = """
SELECT DISTINCT pickup_borough AS borough
FROM agg_trips_hourly
WHERE pickup_borough NOT IN ('Unknown', 'N/A')
ORDER BY 1
"""

KPIS = f"""
SELECT COALESCE(SUM(a.trips), 0)                                       AS trips,
       COALESCE(SUM(a.revenue), 0)                                     AS revenue,
       SUM(a.fare) / NULLIF(SUM(a.distance_miles), 0)                  AS fare_per_mile,
       SUM(a.distance_miles) / NULLIF(SUM(a.trips), 0)                 AS avg_distance,
       SUM(a.duration_minutes) / NULLIF(SUM(a.trips), 0)               AS avg_duration,
       SUM(a.tips) FILTER (WHERE a.payment_type_key = 1)
         / NULLIF(SUM(a.fare) FILTER (WHERE a.payment_type_key = 1), 0) AS card_tip_rate
FROM agg_trips_hourly a
WHERE {_FILTERS}
"""

# 1. Average fare amount per mile (distance-weighted), by pickup borough.
FARE_PER_MILE_BY_BOROUGH = f"""
SELECT a.pickup_borough                                  AS borough,
       SUM(a.trips)                                      AS trips,
       SUM(a.fare) / NULLIF(SUM(a.distance_miles), 0)    AS fare_per_mile
FROM agg_trips_hourly a
WHERE {_FILTERS}
GROUP BY a.pickup_borough
HAVING SUM(a.trips) >= 100
ORDER BY fare_per_mile DESC
"""

# 2. Peak hours: trips by pickup hour.
TRIPS_BY_HOUR = f"""
SELECT a.hour_of_day,
       SUM(a.trips) AS trips
FROM agg_trips_hourly a
WHERE {_FILTERS}
GROUP BY a.hour_of_day
ORDER BY a.hour_of_day
"""

# 2b. Peak hours by day of week (heatmap); averaged per calendar day so partial weeks don't skew it.
TRIPS_BY_WEEKDAY_HOUR = f"""
SELECT d.day_of_week,
       d.day_name,
       a.hour_of_day,
       SUM(a.trips)::NUMERIC / COUNT(DISTINCT a.pickup_date_key) AS avg_trips_per_day
FROM agg_trips_hourly a
JOIN dim_date d ON d.date_key = a.pickup_date_key
WHERE {_FILTERS}
GROUP BY d.day_of_week, d.day_name, a.hour_of_day
"""

# 3. Total revenue by payment type.
REVENUE_BY_PAYMENT_TYPE = f"""
SELECT p.payment_type_desc,
       SUM(a.trips)   AS trips,
       SUM(a.revenue) AS revenue
FROM agg_trips_hourly a
JOIN dim_payment_type p ON p.payment_type_key = a.payment_type_key
WHERE {_FILTERS}
GROUP BY p.payment_type_desc
ORDER BY revenue DESC
"""

PIPELINE_RUNS = """
SELECT source_file,
       status,
       started_at,
       ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::NUMERIC, 1) AS duration_s,
       rows_extracted,
       rows_rejected,
       rows_loaded,
       error_message
FROM etl_run_log
ORDER BY started_at DESC
LIMIT 20
"""
