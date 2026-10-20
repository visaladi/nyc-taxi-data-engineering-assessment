"""Stream producer: replays NYC taxi trips from a parquet file into Kafka as live events.

* Trips are emitted in event-time order (drop-off time) at 10-50 events/s - either a constant
  rate or a sine "traffic wave" between PRODUCER_MIN_RATE and PRODUCER_MAX_RATE.
* A configurable share of events is held back for a random 1..N seconds before sending, to
  simulate mobile devices with bad connectivity -> real out-of-order arrival for the processor.
* Records are keyed by pickup zone, so all trips of a zone land on the same partition (ordering
  per zone) while load spreads over all partitions.
* Reliability: idempotent producer (acks=all, enable.idempotence) so broker-side retries never
  duplicate or reorder records; deterministic event ids make replays de-duplicable downstream.
* Backpressure: when librdkafka's local queue is full, produce() raises BufferError; the
  producer then blocks on poll() until deliveries free space instead of dropping data.

    python -m streaming.producer            (configured via environment, see config.py)
"""

from __future__ import annotations

import heapq
import math
import random
import re
import signal
import time
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from confluent_kafka import KafkaError, Message, Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from streaming.config import ProducerSettings
from streaming.events import TripEvent, make_event_id
from streaming.logging_utils import setup_logging

log = setup_logging("taxi.producer")

EVENTS_SENT = Counter("taxi_producer_events_sent_total", "Events acknowledged by the Kafka cluster")
DELIVERY_FAILURES = Counter("taxi_producer_delivery_failures_total", "Events that failed delivery", ["reason"])
DELIVERY_LATENCY = Histogram(
    "taxi_producer_delivery_latency_seconds",
    "Time from produce() to broker acknowledgement",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
BUFFER_FULL = Counter("taxi_producer_buffer_full_total", "Times the local queue was full (backpressure)")
OUT_OF_ORDER = Counter("taxi_producer_out_of_order_injected_total", "Events deliberately delayed")
HELD_BACK = Gauge("taxi_producer_held_back_events", "Delayed events waiting to be sent")
QUEUE_LENGTH = Gauge("taxi_producer_queue_length", "Messages in librdkafka's queue awaiting delivery")
TARGET_RATE = Gauge("taxi_producer_target_rate", "Current target send rate (events/s)")
SOURCE_ROWS = Gauge("taxi_producer_source_rows", "Rows available in the source after filtering")

# Source column (lower-cased) -> event field.
_COLUMNS = {
    "tpep_pickup_datetime": "pickup",
    "tpep_dropoff_datetime": "dropoff",
    "vendorid": "vendor_id",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}


# --------------------------------------------------------------------------------------
# Source
# --------------------------------------------------------------------------------------
def load_trips(path: Path, max_events: int = 0) -> pa.Table:
    """Reads the trip file, drops unusable rows and sorts by drop-off (event) time."""
    parquet = pq.ParquetFile(path)
    by_lower = {name.lower(): name for name in parquet.schema_arrow.names}
    table = parquet.read(columns=[by_lower[c] for c in _COLUMNS])
    table = table.rename_columns([_COLUMNS[name.lower()] for name in table.column_names])
    table = table.append_column("source_row", pa.array(np.arange(table.num_rows)))

    valid = pc.and_(
        pc.and_(pc.is_valid(table["pickup"]), pc.is_valid(table["dropoff"])),
        pc.greater(table["dropoff"], table["pickup"]),
    )
    # Keep the file's own month only: TLC files contain a few trips with years-off clocks.
    month = re.search(r"(\d{4})-(\d{2})", path.name)
    if month:
        start = datetime(int(month.group(1)), int(month.group(2)), 1)
        end = (start + timedelta(days=32)).replace(day=1)
        dropoff_type = table.schema.field("dropoff").type
        valid = pc.and_(
            valid,
            pc.and_(
                pc.greater_equal(table["dropoff"], pa.scalar(start, dropoff_type)),
                pc.less(table["dropoff"], pa.scalar(end, dropoff_type)),
            ),
        )
    table = table.filter(valid).sort_by("dropoff")
    if max_events:
        table = table.slice(0, max_events)
    return table


def synthetic_rows(seed: int) -> Iterator[dict]:
    """Endless synthetic trips (used when no source file is available)."""
    rng = random.Random(seed)
    clock = datetime(2023, 1, 1)
    row = 0
    while True:
        clock += timedelta(seconds=rng.expovariate(1 / 0.9))
        distance = round(rng.gammavariate(1.8, 1.9) + 0.1, 2)
        fare = round(3 + 2.8 * distance + rng.uniform(1, 6), 2)
        tip = round(fare * rng.uniform(0.1, 0.3), 2) if rng.random() < 0.75 else 0.0
        yield {
            "pickup": clock - timedelta(minutes=distance * rng.uniform(2.5, 5) + 2),
            "dropoff": clock,
            "vendor_id": rng.choice([1, 2]),
            "pu_location_id": rng.randint(1, 263),
            "do_location_id": rng.randint(1, 263),
            "passenger_count": rng.choice([1, 1, 1, 2, 3]),
            "trip_distance": distance,
            "payment_type": 1 if tip else rng.choice([1, 2]),
            "fare_amount": fare,
            "tip_amount": tip,
            "total_amount": round(fare + tip + 5.5, 2),
            "source_row": row,
        }
        row += 1


def iter_source(settings: ProducerSettings) -> tuple[str, Iterator[dict]]:
    if settings.source_file.is_file():
        table = load_trips(settings.source_file, settings.max_events)
        SOURCE_ROWS.set(table.num_rows)
        log.info("source.loaded", extra={"file": str(settings.source_file), "rows": table.num_rows})

        def rows() -> Iterator[dict]:
            for batch in table.to_batches(max_chunksize=2_000):
                yield from batch.to_pylist()

        return settings.source_file.name, rows()

    log.warning("source.missing_using_synthetic", extra={"file": str(settings.source_file)})
    return "synthetic", synthetic_rows(settings.seed)


def _opt_int(value) -> int | None:
    return None if value is None or (isinstance(value, float) and math.isnan(value)) else int(value)


def to_event(source: str, row: dict) -> TripEvent:
    return TripEvent(
        event_id=make_event_id(source, row["source_row"]),
        event_time=row["dropoff"].replace(tzinfo=timezone.utc),
        pickup_time=row["pickup"].replace(tzinfo=timezone.utc),
        vendor_id=_opt_int(row["vendor_id"]),
        pu_location_id=_opt_int(row["pu_location_id"]),
        do_location_id=_opt_int(row["do_location_id"]),
        passenger_count=_opt_int(row["passenger_count"]),
        trip_distance=float(row["trip_distance"] or 0.0),
        payment_type=_opt_int(row["payment_type"]),
        fare_amount=float(row["fare_amount"] or 0.0),
        tip_amount=float(row["tip_amount"] or 0.0),
        total_amount=float(row["total_amount"] or 0.0),
    )


# --------------------------------------------------------------------------------------
# Pacing
# --------------------------------------------------------------------------------------
class Pacer:
    """Spaces sends to follow the configured rate profile without bursting after a stall."""

    def __init__(self, settings: ProducerSettings) -> None:
        self._settings = settings
        self._started = time.monotonic()
        self._next_send = self._started

    def current_rate(self) -> float:
        s = self._settings
        if s.rate_profile == "constant":
            return s.rate
        phase = 2 * math.pi * (time.monotonic() - self._started) / s.wave_period_s
        return s.min_rate + (s.max_rate - s.min_rate) * (1 + math.sin(phase)) / 2

    def wait(self) -> None:
        rate = self.current_rate()
        TARGET_RATE.set(rate)
        now = time.monotonic()
        if self._next_send > now:
            time.sleep(self._next_send - now)
            now = self._next_send
        self._next_send = now + 1 / rate


# --------------------------------------------------------------------------------------
# Producer
# --------------------------------------------------------------------------------------
class TripProducer:
    def __init__(self, settings: ProducerSettings) -> None:
        self.settings = settings
        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "client.id": "taxi-trip-producer",
                "acks": "all",                       # wait for all in-sync replicas (min.insync.replicas=2)
                "enable.idempotence": True,          # no duplicates / reordering on internal retries
                "compression.type": "lz4",
                "linger.ms": 20,                     # small batching window: latency vs throughput
                "batch.size": 64 * 1024,
                "queue.buffering.max.messages": 100_000,  # bounded local buffer -> BufferError = backpressure
                "delivery.timeout.ms": 120_000,
            }
        )
        self.pacer = Pacer(settings)
        self.rng = random.Random(settings.seed)
        self._held: list[tuple[float, int, TripEvent]] = []  # (release_at, seq, event) min-heap
        self._seq = 0
        self._running = True

    def stop(self, *_: object) -> None:
        log.info("producer.stopping")
        self._running = False

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            DELIVERY_FAILURES.labels(reason=err.name()).inc()
            log.error("delivery.failed", extra={"error": err.str(), "key": msg.key()})
            return
        EVENTS_SENT.inc()
        latency = msg.latency()
        if latency is not None:
            DELIVERY_LATENCY.observe(latency)

    def _send(self, event: TripEvent) -> None:
        self.pacer.wait()
        value = event.stamped(datetime.now(timezone.utc)).to_bytes()
        key = str(event.pu_location_id if event.pu_location_id is not None else "unknown").encode()
        while True:
            try:
                # The record timestamp is left to Kafka (produce time). Using the 2023 event time
                # would make the broker's 7-day time-based retention delete the data immediately.
                self.producer.produce(
                    self.settings.kafka.topic,
                    key=key,
                    value=value,
                    headers=[("schema_version", b"1"), ("source", b"nyc-tlc-yellow")],
                    on_delivery=self._on_delivery,
                )
                break
            except BufferError:
                BUFFER_FULL.inc()
                self.producer.poll(0.5)  # block until in-flight deliveries free queue space
        self.producer.poll(0)  # serve delivery callbacks
        QUEUE_LENGTH.set(len(self.producer))

    def _release_due(self, force: bool = False) -> None:
        now = time.monotonic()
        while self._held and (force or self._held[0][0] <= now):
            _, _, event = heapq.heappop(self._held)
            self._send(event)
        HELD_BACK.set(len(self._held))

    def _maybe_hold(self, event: TripEvent) -> bool:
        """Delays a share of events to create out-of-order arrival."""
        if self.rng.random() >= self.settings.out_of_order_ratio:
            return False
        release_at = time.monotonic() + self.rng.uniform(1, self.settings.max_delay_s)
        heapq.heappush(self._held, (release_at, self._seq, event))
        self._seq += 1
        OUT_OF_ORDER.inc()
        return True

    def run(self) -> None:
        source, rows = iter_source(self.settings)
        log.info(
            "producer.started",
            extra={
                "topic": self.settings.kafka.topic,
                "source": source,
                "rate_profile": self.settings.rate_profile,
                "min_rate": self.settings.min_rate,
                "max_rate": self.settings.max_rate,
                "out_of_order_ratio": self.settings.out_of_order_ratio,
            },
        )
        sent_since_log, last_log = 0, time.monotonic()
        for row in rows:
            if not self._running:
                break
            self._release_due()
            event = to_event(source, row)
            if not self._maybe_hold(event):
                self._send(event)
            sent_since_log += 1
            if time.monotonic() - last_log >= 30:
                log.info("producer.progress", extra={"events_last_30s": sent_since_log, "held_back": len(self._held)})
                sent_since_log, last_log = 0, time.monotonic()

        self._release_due(force=True)
        remaining = self.producer.flush(30)
        log.info("producer.finished", extra={"undelivered": remaining})


def main() -> None:
    settings = ProducerSettings()
    if not 0 < settings.min_rate <= settings.max_rate:
        raise SystemExit("PRODUCER_MIN_RATE must be > 0 and <= PRODUCER_MAX_RATE")
    start_http_server(settings.metrics_port)
    producer = TripProducer(settings)
    signal.signal(signal.SIGTERM, producer.stop)
    signal.signal(signal.SIGINT, producer.stop)
    producer.run()


if __name__ == "__main__":
    main()
