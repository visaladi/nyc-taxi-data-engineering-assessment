"""Load: PostgreSQL access - schema management, dimension upserts, fact loads and the run log."""

from __future__ import annotations

import io
import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json, execute_values

from etl.config import Settings
from etl.transform import FACT_COLUMNS, CleaningReport, DimensionKeys

logger = logging.getLogger("taxi_etl.load")


class DataQualityError(RuntimeError):
    """Post-load verification failed; the load transaction is rolled back."""


# Row count + orphan count for one source file. The fact table has no FK constraints (they make
# bulk COPY ~10x slower), so referential integrity is verified here, set-based, before commit.
_VERIFY_LOAD_SQL = """
SELECT COUNT(*) AS loaded_rows,
       COUNT(*) FILTER (
           WHERE pd.date_key IS NULL OR dd.date_key IS NULL
              OR pt.time_key IS NULL OR dt.time_key IS NULL
              OR pl.location_key IS NULL OR dl.location_key IS NULL
              OR v.vendor_key IS NULL OR r.rate_code_key IS NULL OR p.payment_type_key IS NULL
       ) AS orphan_rows
FROM fact_taxi_trips f
LEFT JOIN dim_date pd         ON pd.date_key = f.pickup_date_key
LEFT JOIN dim_date dd         ON dd.date_key = f.dropoff_date_key
LEFT JOIN dim_time pt         ON pt.time_key = f.pickup_time_key
LEFT JOIN dim_time dt         ON dt.time_key = f.dropoff_time_key
LEFT JOIN dim_location pl     ON pl.location_key = f.pickup_location_key
LEFT JOIN dim_location dl     ON dl.location_key = f.dropoff_location_key
LEFT JOIN dim_vendor v        ON v.vendor_key = f.vendor_key
LEFT JOIN dim_rate_code r     ON r.rate_code_key = f.rate_code_key
LEFT JOIN dim_payment_type p  ON p.payment_type_key = f.payment_type_key
WHERE f.source_file = %s
  AND f.pickup_date_key BETWEEN %s AND %s
"""


@contextmanager
def connect(settings: Settings) -> Iterator[PgConnection]:
    conn = psycopg2.connect(**settings.psycopg2_kwargs())
    try:
        yield conn
    finally:
        conn.close()


def apply_schema(conn: PgConnection, schema_file: Path) -> None:
    """Runs sql/schema.sql (idempotent DDL + static dimension seeds) in one transaction."""
    with conn, conn.cursor() as cur:
        cur.execute(schema_file.read_text(encoding="utf-8"))


def upsert_locations(conn: PgConnection, zones: pd.DataFrame) -> int:
    rows = [
        (int(r.location_key), r.borough, r.zone, r.service_zone)
        for r in zones.itertuples(index=False)
    ]
    with conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_location (location_key, borough, zone, service_zone)
            VALUES %s
            ON CONFLICT (location_key) DO UPDATE
               SET borough      = EXCLUDED.borough,
                   zone         = EXCLUDED.zone,
                   service_zone = EXCLUDED.service_zone
             WHERE (dim_location.borough, dim_location.zone, dim_location.service_zone)
                   IS DISTINCT FROM (EXCLUDED.borough, EXCLUDED.zone, EXCLUDED.service_zone)
            """,
            rows,
        )
    return len(rows)


def fetch_dimension_keys(conn: PgConnection) -> DimensionKeys:
    def keys(query: str) -> frozenset[int]:
        cur.execute(query)
        return frozenset(row[0] for row in cur.fetchall())

    with conn, conn.cursor() as cur:
        return DimensionKeys(
            locations=keys("SELECT location_key FROM dim_location"),
            vendors=keys("SELECT vendor_key FROM dim_vendor"),
            rate_codes=keys("SELECT rate_code_key FROM dim_rate_code"),
            payment_types=keys("SELECT payment_type_key FROM dim_payment_type"),
        )


def load_fact_month(
    conn: PgConnection, fact: pd.DataFrame, source_file: str, month: str, chunk_size: int
) -> int:
    """Idempotently (re)loads one source file into its monthly partition.

    Everything happens in a single transaction: the file's previous rows are deleted, the new
    rows are bulk-loaded with COPY, then row count and referential integrity are verified.
    Readers keep seeing the old data until commit, and a failure at any point leaves the table
    untouched.
    """
    copy_stmt = sql.SQL("COPY fact_taxi_trips ({}) FROM STDIN WITH (FORMAT csv)").format(
        sql.SQL(", ").join(map(sql.Identifier, FACT_COLUMNS))
    )
    month_start = date.fromisoformat(f"{month}-01")
    key_range = (int(month_start.strftime("%Y%m01")), int(month_start.strftime("%Y%m31")))

    with conn, conn.cursor() as cur:
        cur.execute("SELECT ensure_fact_partition(%s)", (month_start,))
        partition = cur.fetchone()[0]

        cur.execute(
            "DELETE FROM fact_taxi_trips WHERE source_file = %s AND pickup_date_key BETWEEN %s AND %s",
            (source_file, *key_range),
        )
        if cur.rowcount:
            logger.info("load.replaced_previous_rows", extra={"source_file": source_file, "rows": cur.rowcount})

        for start in range(0, len(fact), chunk_size):
            buffer = io.StringIO()
            fact.iloc[start : start + chunk_size].to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            cur.copy_expert(copy_stmt.as_string(cur), buffer)
            logger.debug(
                "load.chunk_copied",
                extra={"source_file": source_file, "rows_copied": min(start + chunk_size, len(fact))},
            )

        cur.execute(_VERIFY_LOAD_SQL, (source_file, *key_range))
        loaded, orphans = cur.fetchone()
        if loaded != len(fact):
            raise DataQualityError(
                f"{source_file}: expected {len(fact)} rows in fact_taxi_trips after load, found {loaded}"
            )
        if orphans:
            raise DataQualityError(f"{source_file}: {orphans} fact rows reference missing dimension members")

    # Refresh planner statistics and clean up dead tuples left by a reload (needs autocommit).
    # The data is already committed at this point, so a maintenance failure is only a warning.
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("VACUUM (ANALYZE) {}").format(sql.Identifier(partition)))
    except psycopg2.Error as exc:
        logger.warning("load.vacuum_failed", extra={"partition": partition, "error": str(exc).strip()})
    finally:
        conn.autocommit = False
    return loaded


def refresh_aggregates(conn: PgConnection) -> int:
    """Rebuilds agg_trips_hourly; CONCURRENTLY (non-blocking for readers) once it has data."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'agg_trips_hourly'")
        populated = cur.fetchone()[0]
        cur.execute(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY agg_trips_hourly"
            if populated
            else "REFRESH MATERIALIZED VIEW agg_trips_hourly"
        )
        cur.execute("SELECT count(*) FROM agg_trips_hourly")
        return cur.fetchone()[0]


def start_run(conn: PgConnection, source_file: str, flow_run_id: str | None) -> uuid.UUID:
    run_id = uuid.uuid4()
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO etl_run_log (run_id, flow_run_id, source_file, status) VALUES (%s, %s, %s, 'running')",
            (str(run_id), flow_run_id, source_file),
        )
    return run_id


def finish_run(
    conn: PgConnection,
    run_id: uuid.UUID,
    status: str,
    report: CleaningReport | None = None,
    rows_loaded: int | None = None,
    error: str | None = None,
) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE etl_run_log
               SET status           = %s,
                   finished_at      = now(),
                   rows_extracted   = %s,
                   rows_rejected    = %s,
                   rows_loaded      = %s,
                   rejected_by_rule = %s,
                   error_message    = %s
             WHERE run_id = %s
            """,
            (
                status,
                report.rows_in if report else None,
                report.rows_rejected if report else None,
                rows_loaded,
                Json(report.rejected_by_rule) if report else None,
                error[:2000] if error else None,
                str(run_id),
            ),
        )
