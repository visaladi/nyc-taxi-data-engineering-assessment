# Verification console

A small local web page that checks, live, that every part of both assignments is running and correct.

```bash
./run_all.sh                          # from the repo root: starts everything, then the console
# or, if the components are already running:
python verification-ui/server.py      # any Python >= 3.10, standard library only
```

Open <http://localhost:8765> and press **Run checks** (or tick *Auto-refresh 30 s*). A full run takes about 30 seconds,
most of it spent on the referential-integrity scan of the ~6 M-row fact table.

## How it works

* `server.py` serves the page from `static/` and exposes `GET /api/checks`, which runs all checks in parallel and
  returns JSON. It talks to PostgreSQL / TimescaleDB and Kafka through `docker exec` (`psql`, `kafka-topics.sh`,
  `kafka-consumer-groups.sh`, `kafka-metadata-quorum.sh`) and to Prometheus, Grafana, Streamlit and the producer over HTTP,
  so it needs no Python packages.
* `static/index.html`, `styles.css`, `app.js` render the results: summary tiles, links to every UI, and one card per area.
  Each check shows a status (pass / warn / fail / info, as an icon and a word, not colour alone), the measured value and the evidence.

## What is checked

| Area | Checks |
|---|---|
| Assignment 1: batch ETL | PostgreSQL healthy · star-schema objects exist · dimensions populated (1,440 minutes, 265 zones…) · rows per partition · latest run per file succeeded · run log reconciles with the table · **0 orphan fact rows** · data-quality rules hold in the loaded data · aggregate table sums equal the fact count · business metrics · structured log & alerts · rejected rows kept · Streamlit reachable |
| Assignment 2: Kafka | 3 broker containers healthy · topic-init job exited 0 · 3 brokers in metadata · KRaft quorum of 3 voters · topic has 6 partitions, RF 3, `min.insync.replicas=2`, full ISR · 0 under-replicated / leaderless partitions · DLQ topic exists · consumer group members, partition assignment and lag |
| Assignment 2: pipeline | containers up · producer rate within 10–50 ev/s · 0 delivery failures, ack latency, out-of-order injection · consumption rate · lag & DB backpressure · end-to-end latency · event outcomes (inserted / late / duplicate / invalid) · **raw events = distinct event ids** (idempotent sink) · window rows & finalisation · every window is 15 min and starts on a 5-min boundary · `ALL` row = sum of boroughs · DLQ count |
| Observability & UIs | Prometheus targets up · 13 alert rules healthy · active alerts · Grafana healthy · both datasources healthy · dashboard provisioned · Prefect UI flow-run history · Kafka UI cluster online |

Endpoints and container names can be overridden with environment variables (see the top of `server.py`).
