"""Verification console for both assignments.

Runs live health and correctness checks against every running component and serves a small
HTML/CSS page that displays them.

    python verification-ui/server.py            # then open http://localhost:8765

Standard library only (talks to PostgreSQL/TimescaleDB and Kafka through `docker exec`, and to
Prometheus / Grafana / Streamlit / the producer over HTTP), so it runs with any Python >= 3.10.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent
STATIC = ROOT / "static"

PORT = int(os.getenv("VERIFY_PORT", "8765"))
BATCH_PG = os.getenv("BATCH_PG_CONTAINER", "taxi-batch-postgres")
BATCH_DB = os.getenv("BATCH_PG_DB", "taxi_dw")
STREAM_PG = os.getenv("STREAM_PG_CONTAINER", "taxi-timescaledb")
STREAM_DB = os.getenv("STREAM_PG_DB", "taxi_stream")
PG_USER = os.getenv("PG_USER", "taxi")
PROMETHEUS = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
GRAFANA = os.getenv("GRAFANA_URL", "http://localhost:3300")
GRAFANA_AUTH = os.getenv("GRAFANA_AUTH", "admin:admin")
STREAMLIT = os.getenv("STREAMLIT_URL", "http://localhost:8501")
PRODUCER = os.getenv("PRODUCER_METRICS_URL", "http://localhost:8000")
PREFECT = os.getenv("PREFECT_UI_URL", "http://127.0.0.1:4200")
KAFKA_UI = os.getenv("KAFKA_UI_URL", "http://localhost:8088")
TOPIC = "taxi-trips-stream"
GROUP = "taxi-stream-processor"

LINKS = [
    {"label": "Streamlit dashboard", "url": STREAMLIT, "note": "Assignment 1"},
    {"label": "Prefect UI", "url": f"{PREFECT}/runs", "note": "Assignment 1 · flow runs"},
    {"label": "Kafka UI", "url": KAFKA_UI, "note": "Assignment 2 · topics & groups"},
    {"label": "Grafana", "url": f"{GRAFANA}/d/taxi-streaming", "note": "Assignment 2 · admin/admin"},
    {"label": "Prometheus alerts", "url": f"{PROMETHEUS}/alerts", "note": "Assignment 2"},
    {"label": "Prometheus targets", "url": f"{PROMETHEUS}/targets", "note": "Assignment 2"},
    {"label": "Producer metrics", "url": f"{PRODUCER}/metrics", "note": "Assignment 2"},
]

PASS, WARN, FAIL, INFO = "pass", "warn", "fail", "info"


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
class CheckError(RuntimeError):
    pass


def sh(args: list[str], timeout: float = 90) -> str:
    proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0:
        raise CheckError((proc.stderr or proc.stdout).strip()[:300] or f"exit code {proc.returncode}")
    return proc.stdout


def psql(container: str, db: str, sql: str, timeout: float = 120) -> list[list[str]]:
    out = sh(["docker", "exec", container, "psql", "-U", PG_USER, "-d", db, "-At", "-F", "|", "-c", sql], timeout)
    return [line.split("|") for line in out.strip().splitlines() if line]


def http_get(url: str, auth: str | None = None, timeout: float = 5) -> str:
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode()


def prom(query: str) -> list[tuple[dict, float]]:
    url = f"{PROMETHEUS}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    data = json.loads(http_get(url))["data"]["result"]
    return [(r["metric"], float(r["value"][1])) for r in data]


def prom_scalar(query: str) -> float | None:
    result = prom(query)
    return result[0][1] if result else None


def containers() -> dict[str, str]:
    out = sh(["docker", "ps", "-a", "--format", "{{.Names}}|{{.Status}}"], timeout=20)
    return dict(line.split("|", 1) for line in out.strip().splitlines() if "|" in line)


def fmt_int(value: float | int | str) -> str:
    return f"{int(float(value)):,}"


Check = tuple[str, str, str]  # (status, value, detail)


def run(name: str, fn: Callable[[], Check]) -> dict:
    started = time.perf_counter()
    try:
        status, value, detail = fn()
    except Exception as exc:  # every check reports its own failure instead of breaking the page
        status, value, detail = FAIL, "error", f"{type(exc).__name__}: {exc}"[:400]
    return {
        "name": name,
        "status": status,
        "value": value,
        "detail": detail,
        "ms": round((time.perf_counter() - started) * 1000),
    }


def container_check(names: list[str], states: dict[str, str]) -> Check:
    missing = [n for n in names if n not in states]
    down = [f"{n}: {states[n]}" for n in names if n in states and not states[n].startswith("Up")]
    unhealthy = [n for n in names if n in states and "unhealthy" in states[n]]
    if missing or down or unhealthy:
        problems = [f"{n}: not created" for n in missing] + down + [f"{n}: unhealthy" for n in unhealthy]
        return FAIL, f"{len(names) - len(missing) - len(down)}/{len(names)} up", "; ".join(problems)
    return PASS, f"{len(names)}/{len(names)} up", ", ".join(f"{n} ({states[n]})" for n in names)


# --------------------------------------------------------------------------------------
# Assignment 1 - batch ETL
# --------------------------------------------------------------------------------------
def batch_checks() -> list[tuple[str, Callable[[], Check]]]:
    q = lambda sql: psql(BATCH_PG, BATCH_DB, sql)  # noqa: E731
    lock = threading.Lock()
    cache: dict[str, list[list[str]]] = {}

    def fact_stats() -> list[list[str]]:
        """One pass over the fact table, shared by three checks (it holds ~6M rows)."""
        with lock:
            if "rows" not in cache:
                cache["rows"] = q("""SELECT source_file, count(*),
                        count(*) FILTER (WHERE fare_amount < 0 OR total_amount < 0),
                        count(*) FILTER (WHERE trip_distance_miles <= 0),
                        count(*) FILTER (WHERE dropoff_ts <= pickup_ts),
                        count(*) FILTER (WHERE trip_duration_minutes > 360)
                    FROM fact_taxi_trips GROUP BY source_file ORDER BY source_file""")
            return cache["rows"]

    def c_container() -> Check:
        return container_check([BATCH_PG], containers())

    def c_schema() -> Check:
        expected = ["dim_date", "dim_time", "dim_location", "dim_vendor", "dim_rate_code",
                    "dim_payment_type", "fact_taxi_trips", "etl_run_log"]
        rows = q("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        present = {r[0] for r in rows}
        matview = q("SELECT ispopulated FROM pg_matviews WHERE matviewname = 'agg_trips_hourly'")
        missing = [t for t in expected if t not in present]
        if missing or not matview:
            return FAIL, "incomplete", f"missing: {missing + ([] if matview else ['agg_trips_hourly'])}"
        return PASS, "star schema present", "1 fact + 6 dimensions + run log + hourly aggregate"

    def c_dimensions() -> Check:
        (row,) = q("""SELECT (SELECT count(*) FROM dim_date), (SELECT count(*) FROM dim_time),
                             (SELECT count(*) FROM dim_location), (SELECT count(*) FROM dim_vendor),
                             (SELECT count(*) FROM dim_rate_code), (SELECT count(*) FROM dim_payment_type)""")
        names = ["date", "time", "location", "vendor", "rate_code", "payment_type"]
        detail = ", ".join(f"dim_{n}={fmt_int(v)}" for n, v in zip(names, row))
        ok = int(row[1]) == 1440 and int(row[2]) >= 265 and all(int(v) > 0 for v in row)
        return (PASS if ok else FAIL), f"{sum(int(v) for v in row):,} rows", detail

    def c_partitions() -> Check:
        partitions = q("""SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                          WHERE i.inhparent = 'fact_taxi_trips'::regclass ORDER BY 1""")
        rows = fact_stats()
        if not rows:
            return FAIL, "0 rows", "fact_taxi_trips is empty - run the flow"
        total = sum(int(r[1]) for r in rows)
        return PASS, f"{total:,} trips", f"partitions: {', '.join(p[0] for p in partitions)} · " +             " · ".join(f"{r[0]}: {fmt_int(r[1])}" for r in rows)

    def c_runs() -> Check:
        rows = q("""SELECT DISTINCT ON (source_file) source_file, status, rows_extracted, rows_rejected,
                           rows_loaded, round(extract(epoch FROM finished_at - started_at))
                    FROM etl_run_log ORDER BY source_file, started_at DESC""")
        if not rows:
            return FAIL, "no runs", "etl_run_log is empty"
        bad = [r[0] for r in rows if r[1] != "success"]
        detail = " · ".join(
            f"{r[0]}: {r[1]}, {fmt_int(r[2] or 0)} in / {fmt_int(r[3] or 0)} rejected / "
            f"{fmt_int(r[4] or 0)} loaded in {r[5] or '?'}s" for r in rows)
        return (FAIL if bad else PASS), f"{len(rows) - len(bad)}/{len(rows)} files succeeded", detail

    def c_reconcile() -> Check:
        logged = {r[0]: int(r[1]) for r in q("""SELECT DISTINCT ON (source_file) source_file, rows_loaded
                FROM etl_run_log WHERE status = 'success' ORDER BY source_file, started_at DESC""")}
        actual = {r[0]: int(r[1]) for r in fact_stats()}
        diffs = [f"{f}: log {logged.get(f)} vs table {actual.get(f)}"
                 for f in sorted(set(logged) | set(actual)) if logged.get(f) != actual.get(f)]
        if diffs:
            return FAIL, "mismatch", "; ".join(diffs)
        return PASS, "run log = table", f"{len(actual)} source files reconcile exactly"

    def c_orphans() -> Check:
        (row,) = q("""SELECT count(*) FROM fact_taxi_trips f
            LEFT JOIN dim_date pd ON pd.date_key = f.pickup_date_key
            LEFT JOIN dim_time pt ON pt.time_key = f.pickup_time_key
            LEFT JOIN dim_location pl ON pl.location_key = f.pickup_location_key
            LEFT JOIN dim_location dl ON dl.location_key = f.dropoff_location_key
            LEFT JOIN dim_vendor v ON v.vendor_key = f.vendor_key
            LEFT JOIN dim_rate_code r ON r.rate_code_key = f.rate_code_key
            LEFT JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
            WHERE pd.date_key IS NULL OR pt.time_key IS NULL OR pl.location_key IS NULL
               OR dl.location_key IS NULL OR v.vendor_key IS NULL OR r.rate_code_key IS NULL
               OR p.payment_type_key IS NULL""")
        orphans = int(row[0])
        return (PASS if orphans == 0 else FAIL), f"{orphans:,} orphans", "every fact row joins to all 6 dimensions"

    def c_quality() -> Check:
        row = [sum(int(r[i]) for r in fact_stats()) for i in range(2, 6)]
        bad = sum(row)
        detail = f"negative amounts={row[0]}, non-positive distance={row[1]}, " \
                 f"dropoff<=pickup={row[2]}, duration>6h={row[3]}"
        return (PASS if bad == 0 else FAIL), f"{bad} violations", detail

    def c_aggregate() -> Check:
        (row,) = q("SELECT (SELECT coalesce(sum(trips),0) FROM agg_trips_hourly), (SELECT count(*) FROM fact_taxi_trips)")
        agg, fact = int(row[0]), int(row[1])
        status = PASS if agg == fact and fact > 0 else FAIL
        return status, f"{agg:,} = {fact:,}" if agg == fact else f"{agg:,} ≠ {fact:,}", \
            "SUM(trips) in agg_trips_hourly vs COUNT(*) in fact_taxi_trips"

    def c_metrics() -> Check:
        (fpm,) = q("SELECT round(sum(fare) / nullif(sum(distance_miles), 0), 2) FROM agg_trips_hourly")
        peak = q("SELECT hour_of_day, sum(trips) FROM agg_trips_hourly GROUP BY 1 ORDER BY 2 DESC LIMIT 3")
        pay = q("""SELECT p.payment_type_desc, round(sum(a.revenue) / 1e6, 1),
                          round(100 * sum(a.revenue) / sum(sum(a.revenue)) OVER (), 1)
                   FROM agg_trips_hourly a JOIN dim_payment_type p USING (payment_type_key)
                   GROUP BY 1 ORDER BY 2 DESC LIMIT 2""")
        detail = (f"peak hours: {', '.join(f'{int(h):02d}:00' for h, _ in peak)} · revenue: "
                  + ", ".join(f"{d} ${m}M ({s}%)" for d, m, s in pay))
        return INFO, f"${fpm[0]} per mile", detail

    def c_logs() -> Check:
        log = REPO / "assignment-1-batch-etl" / "logs" / "pipeline.jsonl"
        alerts = REPO / "assignment-1-batch-etl" / "logs" / "alerts.jsonl"
        if not log.exists():
            return FAIL, "no log file", str(log)
        events = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line.strip()]
        last = next((e for e in reversed(events) if e.get("event", "").startswith("pipeline.")), None)
        errors = sum(1 for e in events if e.get("level") == "ERROR")
        n_alerts = len(alerts.read_text(encoding="utf-8").splitlines()) if alerts.exists() else 0
        detail = f"{len(events)} structured events, {errors} ERROR records, {n_alerts} alerts in alerts.jsonl"
        if last is None:
            return WARN, "no pipeline events", detail
        status = PASS if last["event"] in ("pipeline.succeeded", "pipeline.finished") and not last.get("failed_months") else WARN
        return status, f"last: {last['event']}", f"{detail} · last at {last['ts']}"

    def c_rejects() -> Check:
        files = sorted((REPO / "data" / "rejected").glob("*.parquet"))
        if not files:
            return WARN, "none", "no rejected-row files in data/rejected"
        return PASS, f"{len(files)} files", ", ".join(f"{f.name} ({f.stat().st_size // 1024} KB)" for f in files)

    def c_streamlit() -> Check:
        body = http_get(f"{STREAMLIT}/_stcore/health").strip()
        return (PASS if body == "ok" else FAIL), body, f"{STREAMLIT} is serving the dashboard"

    return [
        ("PostgreSQL container", c_container),
        ("Star schema objects", c_schema),
        ("Dimension tables populated", c_dimensions),
        ("Fact table partitions", c_partitions),
        ("Latest pipeline run per file", c_runs),
        ("Run log reconciles with fact table", c_reconcile),
        ("Referential integrity (orphan check)", c_orphans),
        ("Data-quality rules hold in the fact table", c_quality),
        ("Aggregate table consistent with fact", c_aggregate),
        ("Business metrics (fare/mile, peak hours, revenue)", c_metrics),
        ("Structured logs & alerts", c_logs),
        ("Rejected rows kept for audit", c_rejects),
        ("Streamlit dashboard reachable", c_streamlit),
    ]


# --------------------------------------------------------------------------------------
# Assignment 2 - Kafka cluster
# --------------------------------------------------------------------------------------
def kafka_checks() -> list[tuple[str, Callable[[], Check]]]:
    states: dict[str, str] = {}

    def c_containers() -> Check:
        states.update(containers())
        return container_check(["kafka-1", "kafka-2", "kafka-3"], states)

    def c_init() -> Check:
        status = states.get("kafka-init") or containers().get("kafka-init", "not created")
        ok = status.startswith("Exited (0)")
        return (PASS if ok else FAIL), status, "one-shot topic creation job"

    def c_brokers() -> Check:
        n = prom_scalar("kafka_brokers")
        if n is None:
            return FAIL, "no data", "kafka-exporter metric missing"
        return (PASS if n == 3 else FAIL), f"{int(n)} / 3 brokers", "from kafka-exporter (cluster metadata)"

    def c_quorum() -> Check:
        out = sh(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-metadata-quorum.sh",
                  "--bootstrap-server", "kafka-1:19092", "describe", "--status"])
        info = dict(re.findall(r"^(\w+):\s+(.*)$", out, re.M))
        voters = re.findall(r'"id":\s*(\d+)', info.get("CurrentVoters", ""))
        return (PASS if len(voters) == 3 else WARN), f"leader node {info.get('LeaderId', '?')}", \
            f"KRaft voters: {', '.join(voters) or info.get('CurrentVoters', '?')} · epoch {info.get('LeaderEpoch', '?')}"

    def c_topic() -> Check:
        out = sh(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-topics.sh",
                  "--bootstrap-server", "kafka-1:19092", "--describe", "--topic", TOPIC])
        header = re.search(r"PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)\s+Configs:\s*(.*)", out)
        if not header:
            return FAIL, "not found", out[:200]
        partitions, rf, configs = int(header.group(1)), int(header.group(2)), header.group(3)
        min_isr = re.search(r"min\.insync\.replicas=(\d+)", configs)
        isr_sizes = [len(i.split(",")) for i in re.findall(r"Isr:\s*([\d,]+)", out)]
        leaders = re.findall(r"Leader:\s*(-?\d+)", out)
        ok = partitions == 6 and rf == 3 and min_isr and min_isr.group(1) == "2" and min(isr_sizes) == rf
        detail = (f"{partitions} partitions, RF {rf}, min.insync.replicas={min_isr.group(1) if min_isr else '?'}, "
                  f"ISR sizes {isr_sizes}, leaders on brokers {sorted(set(leaders))}")
        return (PASS if ok else WARN), f"{partitions} partitions · RF {rf}", detail

    def c_urp() -> Check:
        urp = prom_scalar(f'sum(kafka_topic_partition_under_replicated_partition{{topic="{TOPIC}"}})')
        no_leader = prom_scalar('count(kafka_topic_partition_leader{topic=~"taxi-.*"} == -1)') or 0
        if urp is None:
            return FAIL, "no data", "kafka-exporter metric missing"
        ok = urp == 0 and no_leader == 0
        return (PASS if ok else FAIL), f"{int(urp)} under-replicated", f"{int(no_leader)} partitions without leader"

    def c_dlq() -> Check:
        out = sh(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-topics.sh",
                  "--bootstrap-server", "kafka-1:19092", "--list"])
        topics = out.split()
        ok = "taxi-trips-dlq" in topics
        return (PASS if ok else FAIL), "present" if ok else "missing", f"topics: {', '.join(t for t in topics if not t.startswith('__'))}"

    def c_group() -> Check:
        out = sh(["docker", "exec", "kafka-1", "/opt/kafka/bin/kafka-consumer-groups.sh",
                  "--bootstrap-server", "kafka-1:19092", "--describe", "--group", GROUP])
        rows = [line.split() for line in out.splitlines() if line.startswith(GROUP)]
        if not rows:
            return FAIL, "no members", out.strip()[:200]
        lag = sum(int(r[5]) for r in rows if r[5].isdigit())
        members = {r[6] for r in rows if len(r) > 6 and r[6] != "-"}
        status = PASS if lag < 1000 else WARN if lag < 5000 else FAIL
        per_member: dict[str, list[str]] = {}
        for r in rows:
            if len(r) > 6:
                per_member.setdefault(r[6][-8:], []).append(r[2])
        detail = " · ".join(f"…{m}: partitions {','.join(sorted(p))}" for m, p in per_member.items())
        return status, f"{len(members)} consumers · lag {lag:,}", detail

    return [
        ("Broker containers", c_containers),
        ("Topic creation job (kafka-init)", c_init),
        ("Brokers in cluster metadata", c_brokers),
        ("KRaft controller quorum", c_quorum),
        (f"Topic {TOPIC} configuration", c_topic),
        ("Replication health", c_urp),
        ("Dead-letter topic", c_dlq),
        ("Consumer group assignment & lag", c_group),
    ]


# --------------------------------------------------------------------------------------
# Assignment 2 - streaming pipeline
# --------------------------------------------------------------------------------------
def stream_checks() -> list[tuple[str, Callable[[], Check]]]:
    q = lambda sql: psql(STREAM_PG, STREAM_DB, sql)  # noqa: E731

    def c_containers() -> Check:
        states = containers()
        processors = sorted(n for n in states if "stream-processor" in n)
        return container_check([STREAM_PG, "taxi-producer", *processors] if processors
                               else [STREAM_PG, "taxi-producer", "stream-processor"], states)

    def c_producer_rate() -> Check:
        rate = prom_scalar("sum(rate(taxi_producer_events_sent_total[1m]))")
        target = prom_scalar("max(taxi_producer_target_rate)")
        total = prom_scalar("sum(taxi_producer_events_sent_total)")
        if rate is None:
            return FAIL, "no data", "producer not scraped"
        status = PASS if 8 <= rate <= 55 else WARN
        return status, f"{rate:.1f} events/s", \
            f"target now {target or 0:.1f}/s (10–50 wave) · {fmt_int(total or 0)} acknowledged so far"

    def c_producer_health() -> Check:
        failures = prom_scalar("sum(taxi_producer_delivery_failures_total)") or 0
        held = prom_scalar("sum(taxi_producer_held_back_events)") or 0
        ooo = prom_scalar("sum(taxi_producer_out_of_order_injected_total)") or 0
        p95 = prom_scalar("histogram_quantile(0.95, sum by (le) (rate(taxi_producer_delivery_latency_seconds_bucket[5m])))")
        return (PASS if failures == 0 else WARN), f"{int(failures)} delivery failures", \
            f"acks=all p95 {(p95 or 0) * 1000:.0f} ms · {fmt_int(ooo)} events delayed for out-of-order · {int(held)} held now"

    def c_consume_rate() -> Check:
        rate = prom_scalar("sum(rate(taxi_processor_messages_consumed_total[1m]))")
        instances = prom_scalar('count(up{job="stream-processor"} == 1)') or 0
        if rate is None:
            return FAIL, "no data", "processor not scraped"
        return (PASS if rate > 0 else FAIL), f"{rate:.1f} events/s", f"{int(instances)} processor instance(s) scraped"

    def c_lag() -> Check:
        lag = prom_scalar(f'sum(kafka_consumergroup_lag{{consumergroup="{GROUP}", topic="{TOPIC}"}})')
        backpressure = prom_scalar("max(taxi_processor_db_backpressure)") or 0
        if lag is None:
            return FAIL, "no data", "kafka-exporter has no lag for the group"
        status = PASS if lag < 1000 and backpressure == 0 else WARN if lag < 5000 else FAIL
        return status, f"lag {int(lag):,}", f"DB backpressure: {'BLOCKED' if backpressure else 'none'}"

    def c_latency() -> Check:
        p50 = prom_scalar("histogram_quantile(0.5, sum by (le) (rate(taxi_processor_end_to_end_latency_seconds_bucket[1m])))")
        p95 = prom_scalar("histogram_quantile(0.95, sum by (le) (rate(taxi_processor_end_to_end_latency_seconds_bucket[1m])))")
        batch = prom_scalar("histogram_quantile(0.95, sum by (le) (rate(taxi_processor_batch_duration_seconds_bucket[1m])))")
        if p95 is None or p95 != p95:  # NaN when idle
            return WARN, "no traffic", "no recent events"
        return (PASS if p95 < 10 else WARN), f"p95 {p95:.2f} s", \
            f"p50 {p50:.2f} s · micro-batch p95 {(batch or 0) * 1000:.0f} ms (last 1 min; includes producer hold-back)"

    def c_outcomes() -> Check:
        rows = prom("sum by (outcome) (taxi_processor_events_total)")
        counts = {m.get("outcome", "?"): int(v) for m, v in rows}
        total = sum(counts.values())
        late_pct = 100 * counts.get("late", 0) / max(counts.get("inserted", 0) + counts.get("late", 0), 1)
        return (PASS if late_pct < 5 else WARN), f"{late_pct:.2f}% late", \
            " · ".join(f"{k}={v:,}" for k, v in sorted(counts.items())) + f" (since processor start, {total:,} total)"

    def c_raw() -> Check:
        (row,) = q("""SELECT count(*), count(DISTINCT event_id), count(*) FILTER (WHERE is_late),
                             count(DISTINCT kafka_partition), min(event_time), max(event_time)
                      FROM trip_events_raw""")
        total, distinct = int(row[0]), int(row[1])
        if total == 0:
            return FAIL, "0 events", "nothing stored yet"
        status = PASS if total == distinct else FAIL
        return status, f"{total:,} events", \
            f"{distinct:,} distinct ids (no duplicates = idempotent sink) · {fmt_int(row[2])} late · " \
            f"{row[3]} partitions · event time {row[4][:16]} → {row[5][:16]}"

    def c_windows() -> Check:
        (row,) = q("""SELECT count(*), count(*) FILTER (WHERE is_final), count(DISTINCT pu_borough),
                             max(window_end) FILTER (WHERE is_final)
                      FROM trip_window_agg""")
        latest = q("""SELECT window_start, window_end, trip_count, total_revenue FROM trip_window_agg
                      WHERE pu_borough = 'ALL' AND is_final ORDER BY window_end DESC LIMIT 1""")
        if int(row[0]) == 0:
            return FAIL, "0 windows", "no aggregates yet"
        detail = f"{fmt_int(row[1])} final · {row[2]} borough keys incl. ALL"
        if latest:
            s, e, trips, rev = latest[0]
            detail += f" · last final window {s[11:16]}–{e[11:16]} ({s[:10]}): {fmt_int(trips)} trips, ${float(rev):,.0f}"
        return PASS, f"{fmt_int(row[0])} window rows", detail

    def c_window_sizes() -> Check:
        rows = q("""SELECT DISTINCT extract(epoch FROM window_end - window_start)::int,
                           (extract(epoch FROM window_start)::int % 300)
                    FROM trip_window_agg""")
        sizes = {int(r[0]) for r in rows}
        aligned = all(int(r[1]) == 0 for r in rows)
        ok = sizes == {900} and aligned
        return (PASS if ok else FAIL), "15 min / 5 min slide" if ok else f"sizes {sizes}", \
            "every window is 900 s long and starts on a 300 s boundary"

    def c_window_consistency() -> Check:
        (row,) = q("""SELECT count(*), count(*) FILTER (WHERE a.trip_count <> b.trips)
                      FROM trip_window_agg a
                      JOIN (SELECT window_start, window_end, sum(trip_count) trips FROM trip_window_agg
                            WHERE pu_borough <> 'ALL' GROUP BY 1, 2) b USING (window_start, window_end)
                      WHERE a.pu_borough = 'ALL'""")
        windows, bad = int(row[0]), int(row[1])
        return (PASS if bad == 0 and windows else FAIL), f"{bad} mismatches", \
            f"'ALL' row equals the sum of boroughs in all {windows:,} windows"

    def c_dlq() -> Check:
        n = prom_scalar("sum(taxi_processor_dlq_messages_total)") or 0
        outliers = prom_scalar("sum(taxi_processor_clock_outliers_total)") or 0
        return INFO, f"{int(n)} DLQ messages", f"{int(outliers)} clock outliers ignored for the watermark (since start)"

    return [
        ("Pipeline containers", c_containers),
        ("Producer send rate (10–50 ev/s)", c_producer_rate),
        ("Producer delivery health", c_producer_health),
        ("Processor consumption", c_consume_rate),
        ("Consumer lag & backpressure", c_lag),
        ("End-to-end latency", c_latency),
        ("Event outcomes (inserted / late / duplicate / invalid)", c_outcomes),
        ("Raw events in TimescaleDB", c_raw),
        ("Sliding-window aggregates", c_windows),
        ("Window size & slide", c_window_sizes),
        ("Window totals consistent", c_window_consistency),
        ("Dead-letter queue", c_dlq),
    ]


# --------------------------------------------------------------------------------------
# Assignment 2 - observability
# --------------------------------------------------------------------------------------
def observability_checks() -> list[tuple[str, Callable[[], Check]]]:

    def c_targets() -> Check:
        targets = json.loads(http_get(f"{PROMETHEUS}/api/v1/targets"))["data"]["activeTargets"]
        down = [f"{t['labels']['job']} ({t['lastError']})" for t in targets if t["health"] != "up"]
        jobs = sorted({t["labels"]["job"] for t in targets})
        return (FAIL if down else PASS), f"{len(targets) - len(down)}/{len(targets)} up", \
            ("down: " + "; ".join(down)) if down else "jobs: " + ", ".join(jobs)

    def c_rules() -> Check:
        groups = json.loads(http_get(f"{PROMETHEUS}/api/v1/rules"))["data"]["groups"]
        rules = [r for g in groups for r in g["rules"]]
        errors = [r["name"] for r in rules if r.get("health") != "ok"]
        return (FAIL if errors or not rules else PASS), f"{len(rules)} alert rules", \
            ("unhealthy: " + ", ".join(errors)) if errors else f"{len(groups)} groups evaluated without errors"

    def c_alerts() -> Check:
        alerts = json.loads(http_get(f"{PROMETHEUS}/api/v1/alerts"))["data"]["alerts"]
        firing = [a for a in alerts if a["state"] == "firing"]
        serious = [a for a in firing if a["labels"].get("severity") in ("warning", "critical")]
        detail = ", ".join(f"{a['labels']['alertname']} ({a['labels'].get('severity')}, {a['state']})" for a in alerts)
        return (WARN if serious else PASS), f"{len(firing)} firing", detail or "no pending or firing alerts"

    def c_grafana() -> Check:
        health = json.loads(http_get(f"{GRAFANA}/api/health"))
        return (PASS if health.get("database") == "ok" else FAIL), f"v{health.get('version')}", f"database: {health.get('database')}"

    def c_datasources() -> Check:
        results = []
        for uid in ("prometheus", "timescaledb"):
            body = json.loads(http_get(f"{GRAFANA}/api/datasources/uid/{uid}/health", auth=GRAFANA_AUTH))
            results.append((uid, body.get("status"), body.get("message")))
        ok = all(s == "OK" for _, s, _ in results)
        return (PASS if ok else FAIL), f"{sum(s == 'OK' for _, s, _ in results)}/2 healthy", \
            " · ".join(f"{u}: {m}" for u, _, m in results)

    def c_dashboard() -> Check:
        dash = json.loads(http_get(f"{GRAFANA}/api/dashboards/uid/taxi-streaming", auth=GRAFANA_AUTH))["dashboard"]
        panels = [p for p in dash["panels"] if p["type"] != "row"]
        rows = [p["title"] for p in dash["panels"] if p["type"] == "row"]
        return PASS, f"{len(panels)} panels", f"“{dash['title']}” · rows: {', '.join(rows)}"

    # Optional UIs: reported as warnings (not failures) when they are not started.
    def c_prefect() -> Check:
        try:
            http_get(f"{PREFECT}/api/health")
        except OSError as exc:
            return WARN, "not running", f"start with ./run_all.sh or `prefect server start` ({exc})"
        url = f"{PREFECT}/api/flow_runs/filter"
        req = urllib.request.Request(url, data=b'{"limit": 200}', headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            runs = json.loads(resp.read())
        states = {}
        for r in runs:
            states[r["state_name"]] = states.get(r["state_name"], 0) + 1
        return PASS, f"{len(runs)} flow runs", ", ".join(f"{k}: {v}" for k, v in sorted(states.items())) or "no runs yet"

    def c_kafka_ui() -> Check:
        try:
            # The first call after Kafka UI starts is slow (JVM warm-up), hence the longer timeout.
            body = json.loads(http_get(f"{KAFKA_UI}/api/clusters", timeout=15))
        except OSError as exc:
            return WARN, "not running", f"start with ./run_all.sh or `docker compose --profile ui up -d` ({exc})"
        details = ", ".join(f"{c['name']}: {c['status']}, {c.get('brokerCount')} brokers, "
                            f"{c.get('topicCount')} topics" for c in body)
        ok = body and all(c["status"] == "online" for c in body)
        return (PASS if ok else WARN), "online" if ok else "offline", details

    return [
        ("Prometheus scrape targets", c_targets),
        ("Prometheus alert rules loaded", c_rules),
        ("Active alerts", c_alerts),
        ("Grafana server", c_grafana),
        ("Grafana datasources", c_datasources),
        ("Grafana dashboard provisioned", c_dashboard),
        ("Prefect UI (flow-run history)", c_prefect),
        ("Kafka UI", c_kafka_ui),
    ]


GROUPS = [
    ("batch", "Assignment 1 · Batch ETL & star schema", batch_checks),
    ("kafka", "Assignment 2 · Kafka cluster", kafka_checks),
    ("stream", "Assignment 2 · Streaming pipeline & TimescaleDB", stream_checks),
    ("obs", "Assignment 2 · Monitoring & observability", observability_checks),
]


def run_all() -> dict:
    """Runs every check concurrently (they are independent); results keep their display order."""
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=16) as pool:
        planned = [
            (gid, title, [pool.submit(run, name, fn) for name, fn in plan()])
            for gid, title, plan in GROUPS
        ]
        groups = [{"id": gid, "title": title, "checks": [f.result() for f in futures]}
                  for gid, title, futures in planned]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.perf_counter() - started, 1),
        "groups": groups,
        "links": LINKS,
    }


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC), **kwargs)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/checks"):
            body = json.dumps(run_all()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, fmt: str, *args) -> None:
        if "/api/checks" in (args[0] if args else ""):
            print(f"{self.log_date_time_string()}  {args[0]}  {args[1] if len(args) > 1 else ''}")


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print(f"Verification console on http://localhost:{PORT}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
