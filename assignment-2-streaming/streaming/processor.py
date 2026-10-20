"""Stream processor: consumes trip events, maintains sliding-window aggregates, writes TimescaleDB.

Per micro-batch (up to PROCESSOR_BATCH_SIZE messages):

  1. parse + validate       invalid payloads -> DLQ topic (with error headers), never block the stream
  2. enrich                 pickup zone -> borough
  3. classify lateness      against the event-time watermark *before* this batch
  4. advance watermarks     per partition (outlier timestamps cannot jump it forward)
  5. one DB transaction     a) insert raw events  ON CONFLICT DO NOTHING RETURNING event_id
                            b) aggregate only the *newly inserted*, on-time events into windows
                            c) upsert window increments
                            d) mark windows final once the watermark passed their end
  6. commit Kafka offsets   only after the DB commit

Delivery semantics: at-least-once delivery from Kafka + idempotent sink = effectively-once
results. If the process dies between (5) and (6), the batch is redelivered, every raw insert
conflicts, nothing is re-aggregated, and the offsets are committed on the second pass.

Backpressure: the consumer is pull-based - it only fetches the next batch after the current
one is committed. If TimescaleDB slows down or is unavailable, the processor retries with
exponential backoff (bounded below max.poll.interval.ms) and Kafka simply buffers; consumer
lag grows and is alerted on. The librdkafka prefetch queue is bounded so memory stays flat.

    python -m streaming.processor           (configured via environment, see config.py)
"""

from __future__ import annotations

import csv
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from streaming import windowing
from streaming.config import ProcessorSettings
from streaming.events import InvalidEventError, TripEvent
from streaming.logging_utils import setup_logging
from streaming.sink import RawRecord, TimescaleSink

log = setup_logging("taxi.processor")

MESSAGES_CONSUMED = Counter("taxi_processor_messages_consumed_total", "Kafka messages consumed")
EVENTS = Counter(
    "taxi_processor_events_total", "Events by processing outcome", ["outcome"]
)  # inserted | duplicate | late | invalid
BATCH_DURATION = Histogram(
    "taxi_processor_batch_duration_seconds",
    "Wall time to process and commit one micro-batch",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
BATCH_SIZE = Histogram(
    "taxi_processor_batch_size", "Messages per micro-batch", buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000)
)
E2E_LATENCY = Histogram(
    "taxi_processor_end_to_end_latency_seconds",
    "Producer send -> committed in TimescaleDB",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)
WINDOWS_UPSERTED = Counter("taxi_processor_window_rows_upserted_total", "Window aggregate rows upserted")
WINDOWS_FINALIZED = Counter("taxi_processor_windows_finalized_total", "Window rows closed by the watermark")
WATERMARK = Gauge("taxi_processor_watermark_timestamp_seconds", "Current event-time watermark (epoch s)")
MAX_EVENT_TIME = Gauge("taxi_processor_max_event_timestamp_seconds", "Newest event time processed (epoch s)")
CLOCK_OUTLIERS = Counter("taxi_processor_clock_outliers_total", "Events ignored for watermark advancement")
DB_ERRORS = Counter("taxi_processor_db_errors_total", "Failed database transactions (retried)")
DB_BACKPRESSURE = Gauge("taxi_processor_db_backpressure", "1 while the processor is blocked retrying the DB")
DLQ_MESSAGES = Counter("taxi_processor_dlq_messages_total", "Messages routed to the dead-letter topic")
COMMIT_FAILURES = Counter("taxi_processor_commit_failures_total", "Failed Kafka offset commits")
CONSUMER_LAG = Gauge("taxi_processor_consumer_lag", "Log-end offset minus consumer position", ["partition"])
ASSIGNED_PARTITIONS = Gauge("taxi_processor_assigned_partitions", "Partitions assigned to this instance")

LAG_REFRESH_SECONDS = 10


def load_zone_boroughs(path: Path) -> dict[int, str]:
    if not path.is_file():
        log.warning("zones.missing", extra={"file": str(path)})
        return {}
    with path.open(newline="", encoding="utf-8") as fh:
        return {int(row["LocationID"]): row["Borough"].strip() or "Unknown" for row in csv.DictReader(fh)}


class StreamProcessor:
    def __init__(self, settings: ProcessorSettings) -> None:
        self.settings = settings
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "group.id": settings.group_id,
                "client.id": "taxi-stream-processor",
                "enable.auto.commit": False,          # offsets are committed after the DB commit
                "auto.offset.reset": "earliest",
                "partition.assignment.strategy": "cooperative-sticky",  # incremental rebalances
                "max.poll.interval.ms": 300_000,
                "session.timeout.ms": 45_000,
                "queued.max.messages.kbytes": 16_384,  # bound the prefetch buffer (memory backpressure)
                "fetch.max.bytes": 16 * 1024 * 1024,
            }
        )
        self.dlq = Producer(
            {
                "bootstrap.servers": settings.kafka.bootstrap_servers,
                "client.id": "taxi-stream-processor-dlq",
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        self.sink = TimescaleSink(settings.pg_kwargs())
        self.spec = windowing.SlidingWindowSpec(settings.window_size_s, settings.window_slide_s)
        self.watermarks = windowing.WatermarkTracker(settings.allowed_lateness_s, settings.max_watermark_jump_s)
        self.zones = load_zone_boroughs(settings.zone_lookup_file)
        self._running = True
        self._last_lag_refresh = 0.0

    # ------------------------------------------------------------------ lifecycle
    def stop(self, *_: object) -> None:
        log.info("processor.stopping")
        self._running = False

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("partitions.assigned", extra={"partitions": [p.partition for p in partitions]})
        ASSIGNED_PARTITIONS.set(len(consumer.assignment()))

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        # Called between batches (from within consume()), so no batch is half-processed here.
        revoked = [p.partition for p in partitions]
        log.info("partitions.revoked", extra={"partitions": revoked})
        self.watermarks.forget(revoked)
        for partition in revoked:
            try:
                CONSUMER_LAG.remove(str(partition))
            except KeyError:
                pass

    def run(self) -> None:
        start_http_server(self.settings.metrics_port)
        self.sink.connect()
        self.consumer.subscribe(
            [self.settings.kafka.topic],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_revoke,
        )
        log.info(
            "processor.started",
            extra={
                "topic": self.settings.kafka.topic,
                "group_id": self.settings.group_id,
                "window_size_s": self.spec.size_s,
                "window_slide_s": self.spec.slide_s,
                "allowed_lateness_s": self.settings.allowed_lateness_s,
                "zones_loaded": len(self.zones),
            },
        )
        try:
            while self._running:
                messages = self.consumer.consume(self.settings.batch_size, self.settings.poll_timeout_s)
                if messages:
                    self._process_batch(messages)
                self._refresh_lag()
        finally:
            self.consumer.close()  # leaves the group cleanly -> fast rebalance
            self.dlq.flush(10)
            self.sink.close()
            log.info("processor.stopped")

    # ------------------------------------------------------------------ batch processing
    def _process_batch(self, messages: list[Message]) -> None:
        started = time.perf_counter()
        MESSAGES_CONSUMED.inc(len(messages))
        BATCH_SIZE.observe(len(messages))

        records: dict[str, RawRecord] = {}  # keyed by event_id: de-duplicates within the batch
        dlq_sent = 0
        for msg in messages:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka.message_error", extra={"error": msg.error().str()})
                continue
            try:
                event = TripEvent.from_bytes(msg.value())
            except InvalidEventError as exc:
                self._send_to_dlq(msg, str(exc))
                dlq_sent += 1
                continue
            records.setdefault(event.event_id, self._enrich(msg, event))

        watermark_before = self.watermarks.watermark
        batch = [self._classify(record, watermark_before) for record in records.values()]
        for record in batch:
            if not self.watermarks.observe(record.partition, record.event_ts):
                CLOCK_OUTLIERS.inc()
        watermark_after = self.watermarks.watermark

        inserted, upserted, finalized = self._persist_with_retry(batch, watermark_before, watermark_after)

        if dlq_sent:
            self.dlq.flush(10)  # DLQ copies must be durable before their offsets are committed
        self._commit()

        # Metrics: every valid event is exactly one of duplicate / late / inserted.
        new = [r for r in batch if r.event.event_id in inserted]
        late = sum(r.is_late for r in new)
        EVENTS.labels("inserted").inc(len(new) - late)
        EVENTS.labels("late").inc(late)
        EVENTS.labels("duplicate").inc(len(batch) - len(new))
        EVENTS.labels("invalid").inc(dlq_sent)
        WINDOWS_UPSERTED.inc(upserted)
        WINDOWS_FINALIZED.inc(finalized)
        if watermark_after is not None:
            WATERMARK.set(watermark_after)
        if batch:
            MAX_EVENT_TIME.set(max(r.event_ts for r in batch))
        now = datetime.now(timezone.utc)
        for record in batch:
            if record.event.produced_at is not None:
                E2E_LATENCY.observe(max((now - record.event.produced_at).total_seconds(), 0))
        BATCH_DURATION.observe(time.perf_counter() - started)

    def _enrich(self, msg: Message, event: TripEvent) -> RawRecord:
        borough = self.zones.get(event.pu_location_id or -1, "Unknown")
        return RawRecord(event=event, borough=borough, partition=msg.partition(), offset=msg.offset())

    def _classify(self, record: RawRecord, watermark: int | None) -> RawRecord:
        if not self.spec.is_late(record.event_ts, watermark):
            return record
        return RawRecord(record.event, record.borough, record.partition, record.offset, is_late=True)

    def _persist(
        self, batch: list[RawRecord], watermark_before: int | None, watermark_after: int | None
    ) -> tuple[set[str], int, int]:
        with self.sink.transaction() as cur:
            inserted = self.sink.insert_raw(cur, batch)
            fresh = [r for r in batch if r.event.event_id in inserted and not r.is_late]
            deltas = windowing.aggregate(fresh, self.spec, watermark_before)
            upserted = self.sink.upsert_windows(cur, deltas)
            finalized = self.sink.finalize_windows(cur, watermark_after)
        return inserted, upserted, finalized

    def _persist_with_retry(
        self, batch: list[RawRecord], watermark_before: int | None, watermark_after: int | None
    ) -> tuple[set[str], int, int]:
        """Retries transient DB failures with exponential backoff.

        The total retry budget stays below max.poll.interval.ms; past it the process exits and
        restarts, the group rebalances and the uncommitted batch is reprocessed idempotently.
        """
        deadline = time.monotonic() + self.settings.max_db_retry_s
        delay = 0.5
        while True:
            try:
                result = self._persist(batch, watermark_before, watermark_after)
                DB_BACKPRESSURE.set(0)
                return result
            # OperationalError: connection loss, deadlock, serialization failure, DB starting up.
            # InterfaceError: the connection was already closed underneath us.
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                DB_ERRORS.inc()
                DB_BACKPRESSURE.set(1)
                if time.monotonic() + delay > deadline:
                    log.critical("sink.retry_budget_exhausted", extra={"error": str(exc).strip()})
                    raise
                log.warning("sink.write_failed_retrying", extra={"error": str(exc).strip(), "retry_in_s": delay})
                time.sleep(delay)
                delay = min(delay * 2, 10)

    def _commit(self) -> None:
        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as exc:
            # E.g. a rebalance revoked the partitions; the new owner re-reads from the last
            # committed offset and the idempotent sink absorbs the duplicates.
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                COMMIT_FAILURES.inc()
                log.warning("kafka.commit_failed", extra={"error": str(exc)})

    def _send_to_dlq(self, msg: Message, reason: str) -> None:
        DLQ_MESSAGES.inc()
        self.dlq.produce(
            self.settings.kafka.dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=[
                ("error", reason.encode()),
                ("source_topic", msg.topic().encode()),
                ("source_partition", str(msg.partition()).encode()),
                ("source_offset", str(msg.offset()).encode()),
            ],
        )
        self.dlq.poll(0)
        log.warning("event.invalid_sent_to_dlq", extra={"reason": reason, "partition": msg.partition(), "offset": msg.offset()})

    def _refresh_lag(self) -> None:
        """Per-partition lag as seen by this instance (kafka-exporter reports the group view)."""
        if time.monotonic() - self._last_lag_refresh < LAG_REFRESH_SECONDS:
            return
        self._last_lag_refresh = time.monotonic()
        try:
            assignment = self.consumer.assignment()
            ASSIGNED_PARTITIONS.set(len(assignment))
            for tp in self.consumer.position(assignment):
                _, high = self.consumer.get_watermark_offsets(tp, timeout=2, cached=False)
                position = tp.offset if tp.offset >= 0 else high
                CONSUMER_LAG.labels(str(tp.partition)).set(max(high - position, 0))
        except KafkaException as exc:
            log.warning("kafka.lag_refresh_failed", extra={"error": str(exc)})


def main() -> None:
    processor = StreamProcessor(ProcessorSettings())
    signal.signal(signal.SIGTERM, processor.stop)
    signal.signal(signal.SIGINT, processor.stop)
    processor.run()


if __name__ == "__main__":
    main()
