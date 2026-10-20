"""TimescaleDB sink: raw event inserts, incremental window upserts and window finalisation."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor
from psycopg2.extras import execute_values

from streaming.events import TripEvent
from streaming.windowing import Window, WindowDelta

log = logging.getLogger("taxi.sink")

_INSERT_RAW = """
INSERT INTO trip_events_raw (
    event_id, event_time, pickup_time, vendor_id, pu_location_id, do_location_id, pu_borough,
    passenger_count, trip_distance, payment_type, fare_amount, tip_amount, total_amount,
    produced_at, kafka_partition, kafka_offset, is_late
) VALUES %s
ON CONFLICT (event_id, event_time) DO NOTHING
RETURNING event_id
"""

# Measures are additive, so concurrent consumers (and successive micro-batches) can each add
# their own increment to the same window row.
_UPSERT_WINDOWS = """
INSERT INTO trip_window_agg (
    window_start, window_end, pu_borough, trip_count, passenger_count,
    total_distance, total_fare, total_tip, total_revenue
) VALUES %s
ON CONFLICT (window_start, window_end, pu_borough) DO UPDATE SET
    trip_count      = trip_window_agg.trip_count      + EXCLUDED.trip_count,
    passenger_count = trip_window_agg.passenger_count + EXCLUDED.passenger_count,
    total_distance  = trip_window_agg.total_distance  + EXCLUDED.total_distance,
    total_fare      = trip_window_agg.total_fare      + EXCLUDED.total_fare,
    total_tip       = trip_window_agg.total_tip       + EXCLUDED.total_tip,
    total_revenue   = trip_window_agg.total_revenue   + EXCLUDED.total_revenue,
    update_count    = trip_window_agg.update_count + 1,
    last_updated_at = now()
"""

# SKIP LOCKED: a window row currently being upserted by another replica is simply finalised by
# a later batch. Without it, this multi-row UPDATE (physical row order) and concurrent upserts
# (key order) can deadlock each other.
_FINALIZE_WINDOWS = """
UPDATE trip_window_agg AS w SET is_final = TRUE, last_updated_at = now()
FROM (
    SELECT window_start, window_end, pu_borough
    FROM trip_window_agg
    WHERE NOT is_final AND window_end <= %s
    ORDER BY window_start, window_end, pu_borough
    FOR UPDATE SKIP LOCKED
) AS due
WHERE (w.window_start, w.window_end, w.pu_borough) = (due.window_start, due.window_end, due.pu_borough)
"""


@dataclass(frozen=True)
class RawRecord:
    """A consumed, validated event plus its Kafka coordinates and enrichment."""

    event: TripEvent
    borough: str
    partition: int
    offset: int
    is_late: bool = False

    # Attributes used by windowing.aggregate()
    @property
    def event_ts(self) -> int:
        return self.event.event_ts

    @property
    def passenger_count(self) -> int | None:
        return self.event.passenger_count

    @property
    def trip_distance(self) -> float:
        return self.event.trip_distance

    @property
    def fare_amount(self) -> float:
        return self.event.fare_amount

    @property
    def tip_amount(self) -> float:
        return self.event.tip_amount

    @property
    def total_amount(self) -> float:
        return self.event.total_amount


def _utc(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


class TimescaleSink:
    def __init__(self, conn_kwargs: dict) -> None:
        self._conn_kwargs = conn_kwargs
        self._conn: PgConnection | None = None

    def connect(self, timeout_s: float = 120) -> None:
        """Connects, retrying while the database is still starting."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                self._conn = psycopg2.connect(**self._conn_kwargs)
                log.info("sink.connected", extra={"host": self._conn_kwargs["host"]})
                return
            except psycopg2.OperationalError as exc:
                if time.monotonic() > deadline:
                    raise
                log.warning("sink.connect_retry", extra={"error": str(exc).strip()})
                time.sleep(3)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[PgCursor]:
        """One DB transaction; on a connection-level error the connection is discarded so the
        next attempt reconnects. (When the server goes away, psycopg2 raises OperationalError
        on the statement and then InterfaceError from the implicit rollback.)"""
        if self._conn is None or self._conn.closed:
            self.connect(timeout_s=10)
        assert self._conn is not None
        try:
            with self._conn, self._conn.cursor() as cur:
                yield cur
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if self._conn is not None and self._conn.closed:
                self._conn = None
            raise

    @staticmethod
    def insert_raw(cur: PgCursor, records: list[RawRecord]) -> set[str]:
        """Inserts raw events; returns the ids that were new (not already stored)."""
        if not records:
            return set()
        rows = [
            (
                r.event.event_id, r.event.event_time, r.event.pickup_time, r.event.vendor_id,
                r.event.pu_location_id, r.event.do_location_id, r.borough, r.event.passenger_count,
                r.event.trip_distance, r.event.payment_type, r.event.fare_amount, r.event.tip_amount,
                r.event.total_amount, r.event.produced_at, r.partition, r.offset, r.is_late,
            )
            for r in records
        ]
        inserted = execute_values(cur, _INSERT_RAW, rows, page_size=len(rows), fetch=True)
        return {str(row[0]) for row in inserted}

    @staticmethod
    def upsert_windows(cur: PgCursor, deltas: dict[tuple[Window, str], WindowDelta]) -> int:
        if not deltas:
            return 0
        # Sorted so concurrent consumers lock rows in the same order (no deadlocks).
        rows = [
            (
                _utc(window.start), _utc(window.end), borough, d.trip_count, d.passenger_count,
                round(d.total_distance, 2), round(d.total_fare, 2), round(d.total_tip, 2),
                round(d.total_revenue, 2),
            )
            for (window, borough), d in sorted(deltas.items(), key=lambda item: (item[0][0], item[0][1]))
        ]
        execute_values(cur, _UPSERT_WINDOWS, rows, page_size=len(rows))
        return len(rows)

    @staticmethod
    def finalize_windows(cur: PgCursor, watermark: int | None) -> int:
        if watermark is None:
            return 0
        cur.execute(_FINALIZE_WINDOWS, (_utc(watermark),))
        return cur.rowcount
