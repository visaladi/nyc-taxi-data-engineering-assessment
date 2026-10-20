# Assessment requirements review

Reviewed on 20 October 2026 against `Data_Engineering_Assessments.docx.pdf` supplied by the user.

## Verdict

All ten detailed tasks have corresponding implementation artifacts. Assignment 1 has strong implementation and fresh live execution evidence. Assignment 2 runs successfully under the observed workload, but three streaming correctness and delivery reliability gaps remain. **Do not claim all requirements and evaluation criteria are fully met until those gaps are resolved and validated.** Docker is available in this review; see the current validation results below.

The PDF was treated as the assessment specification, not as authorization to perform actions beyond this review. Application source files were not changed.

## Requirement checklist

“Present” means supported by inspected code/configuration. The checklist records implementation coverage; fresh live evidence is recorded separately below and supersedes earlier unverified runtime notes. A clean-machine setup was not rerun.

| Requirement from PDF | Result | Evidence |
|---|---|---|
| Shared NYC Yellow Taxi data; at least two months | Present | `data/raw/yellow_tripdata_2023-01.parquet` and `yellow_tripdata_2023-02.parquet` exist; setup uses the TLC download endpoint. |
| Assignment 1 stack: Bash, Python 3.10+, modern open-source orchestration, PostgreSQL/DuckDB, Streamlit/Superset | Present | Bash setup, Python environment, Prefect flow, PostgreSQL Compose service, Streamlit dashboard. |
| A1 Task 1: `setup.sh` creates a venv, installs dependencies, starts PostgreSQL with Docker, downloads two months using curl/wget | Present | `assignment-1-batch-etl/setup.sh` implements all steps and defaults to January and February 2023. Clean setup was not rerun. |
| A1 Task 2: `schema.sql`, central `fact_taxi_trips`, at least three dimensions, analytical star schema | Present | `assignment-1-batch-etl/sql/schema.sql`: fact table, six dimensions, monthly partitions and aggregate materialized view. |
| A1 Task 3: Python extraction/cleaning/transformation/loading with orchestration | Present; unit tests pass | `etl/{extract,transform,load}.py`, `flows/taxi_etl_flow.py`; Prefect tasks/subflows, retries, transactional loads. |
| A1 Task 4: structured logs, pipeline start/end times, row counts, explicit errors, orchestration failure status/alerts | Present | `etl/logging_utils.py`, flow hooks, run audit table and existing `logs/pipeline.jsonl`. Failure hooks were inspected, not failure-injection tested. |
| A1 Task 5: SQL for average fare per mile, peak hours and revenue by payment type; single-page dashboard | Present | `sql/analytics_queries.sql`, `dashboard/queries.py`, `dashboard/app.py`. Dashboard health and business metrics were freshly verified; a fresh visual interaction review was not performed. |
| Assignment 2 stack: Kafka via Compose, Python streaming, analytical storage, Prometheus/Grafana, architecture documentation | Present, with library interpretation to disclose | Kafka x3, native Python consumer using `confluent-kafka`, TimescaleDB, Prometheus, Grafana, README diagrams. The PDF names native Kafka-Python; this uses a different Python Kafka client. |
| A2 Task 1: multi-broker Compose cluster and multi-partition `taxi-trips-stream` topic | Present | `assignment-2-streaming/docker-compose.yml`, `kafka/create-topics.sh`: three brokers, six partitions, replication factor three, minimum ISR two. Live verification confirms all brokers healthy and full ISR on all six partitions. |
| A2 Task 2: Python producer reads downloaded Parquet/CSV and sends 10–50 events/second | Present | `streaming/producer.py`, `streaming/config.py`: Parquet reader, pacing and default 10–50 events/s wave. Live acknowledged throughput was 46.6 events/s. Delivery failure handling needs improvement (finding 3). |
| A2 Task 3: consumer, sliding windows, raw event table and aggregation upserts | Present, correctness gaps | `streaming/{processor,windowing,sink}.py`, `sql/init.sql`. Fifteen-minute windows sliding every five minutes; transactional raw inserts plus aggregate increments. See finding 1. |
| A2 Task 4: application metrics to Prometheus; Grafana cluster health, lag and throughput | Present | Metrics in producer/processor, Kafka exporter, Prometheus scrape config and provisioned Grafana JSON containing all requested panel categories. Live verification confirms 4/4 targets up, 13 alert rules, 0 firing alerts, 2/2 Grafana datasources healthy and 25 provisioned panels. |
| A2 Task 5: README cloud production design for 50,000 events/second, including architecture diagram | Present | `assignment-2-streaming/README.md`, section 7: AWS architecture, sizing, Kafka partitions/replication/storage, Flink, ClickHouse, lake, operations and security. The requirement is a design document; a local 50,000 events/s benchmark is not required by the PDF. |

## Findings to resolve or explicitly explain

### 1. High: watermark and final-window behavior is not stable across restarts, assignments or replicas

Evidence: `streaming/processor.py:114`, `:124`, `:233`; `streaming/windowing.py:56`; `streaming/sink.py:34`.

Watermarks exist only in process memory. Assignment does not register all assigned partitions before observation, and each replica has its own watermark. A newly observed slow partition can move the watermark backward. A restart resets it entirely, allowing a new late event to be treated as on time. Aggregate upserts do not check `is_final`, so already-final windows can change. The README acknowledges the replica finalization limitation, but this still limits the claimed out-of-order/final-window guarantees.

Reproduced with the actual windowing module, without Kafka:

```text
WatermarkTracker(600), observe partition 0 at time 3600:
  watermark = 3000; event at time 0 is late
Construct a new tracker (restart):
  watermark = None; the same event time is no longer classified late
Observe partition 0 at 3600, then previously unseen partition 1 at 1200:
  watermark changes from 3000 to 600
```

Recommended correction: coordinate and durably restore partition progress and finalization, including assignment/idleness handling. Ensure late-event classification and aggregate writes use a consistent finalization policy. Add meaningful restart, reassignment and multi-consumer integration coverage.

### 2. High: DLQ delivery is not confirmed before source offsets are committed

Evidence: `streaming/processor.py:202`, `:271`, `:281`.

The processor calls `self.dlq.flush(10)` then commits source offsets, but ignores the flush return value and registers no delivery callback to capture failed DLQ acknowledgements. A timeout with pending messages, or an asynchronous delivery failure, can therefore commit past an invalid source event without a durable DLQ copy.

Recommended correction: require successful DLQ delivery acknowledgements before committing corresponding source offsets. Keep offsets uncommitted and retry/fail when delivery remains pending or fails.

### 3. Medium: producer delivery failures do not fail or replay the producer

Evidence: `streaming/producer.py:227`, `:303`.

The delivery callback logs/counts failure and returns. At shutdown, pending messages from `flush(30)` are logged but do not cause a nonzero exit. A run can exit successfully with missing events, and Compose's `restart: on-failure` will not recover that successful exit.

Recommended correction: track failed acknowledgements and pending deliveries, preserve/replay failed source records or fail the process so a deterministic replay can recover them. Verify behavior under broker outages and exhausted delivery timeouts.

## Validation performed

Fresh validation on **20 October 2026**, with the live snapshot completed at **10:18:40 Asia/Colombo**:

- Assignment 1: **15 unit tests passed**. One non-failing NumPy timedelta deprecation warning.
- Assignment 2: **21 unit tests passed**.
- Verification console: **41 checks: 39 passed, 2 informational, 0 warnings, 0 failures**, completed in 41.2 seconds. Full dated results are saved in [evidence/verification-2026-10-20.json](evidence/verification-2026-10-20.json).
- Batch warehouse: **5,843,620 fact rows**, six populated dimensions, two monthly partitions, **0 orphan fact rows**, no tested data-quality violations, audit counts reconcile and aggregate trip totals match facts. The hourly materialized view has **24,411 rows**.
- January audit: 3,066,766 extracted, 70,990 rejected, 2,995,776 loaded. February audit: 2,913,955 extracted, 66,111 rejected, 2,847,844 loaded. Latest runs succeeded for both files.
- Kafka: three healthy brokers, three quorum voters, six topic partitions, RF 3, min ISR 2, full ISR and zero under-replicated/leaderless partitions. Topic configuration was also checked independently using Kafka CLI.
- Stream snapshot: **46.6 acknowledged events/s**, **46.5 consumed events/s**, zero observed producer delivery failures, p95 end-to-end latency **1.79 seconds**. **72,610 raw events = 72,610 distinct IDs**, **1,811 aggregate rows**, correct 15-minute/5-minute boundaries, zero city/borough window-total mismatches. Counts vary continuously; these figures come from the saved snapshot.
- Monitoring: four healthy scrape targets, 13 loaded alert rules, zero firing alerts, healthy Grafana and both datasources. Required panel categories are provisioned. Streamlit, Prefect and Kafka UI are reachable.
- Reproduced finding 1 again with the actual windowing module: observing a slower, previously unseen partition changes the watermark from 3000 to 600; after tracker reconstruction, an event previously classified late is no longer classified late.
- Findings 2 and 3 remain present in the inspected source. The live snapshot contained zero invalid events and therefore did not exercise failed DLQ delivery. No controlled outage/failure injection was performed.
- Application source was not changed, the ETL was not rerun, and services were not restarted. Clean-machine installation, failure alerts under a controlled failure, restart/reassignment recovery, multiple-consumer finalization and failed-delivery recovery remain unverified.

Passing normal-operation checks does not prove the restart and failed-delivery cases above. Existing screenshots are historical visual evidence, not substitutes for those tests.

## Demonstration requirements

The PDF also evaluates the candidate's ability to explain randomly selected code and justify assumptions. Repository inspection cannot establish this; prepare to explain the star-schema grain, metric definitions, Prefect retries/hooks, Kafka acknowledgements/ISR/partitioning, windowing and lateness, transaction/offset ordering, backpressure and the cloud sizing choices.

The distance-weighted fare-per-mile interpretation, application-enforced referential integrity and use of `confluent-kafka` should be disclosed as implementation choices. The PDF permits assumptions and resource-limited or staged demonstrations; it does not require every service to run simultaneously.

Before claiming full readiness, resolve the three reliability issues and validate controlled failure/recovery, restart/reassignment, and out-of-order behavior. The PDF does not specify an upload portal, recipient, deadline or ZIP format. See the [root README](README.md) for setup and demonstration entry points. No submission was performed during this review.
