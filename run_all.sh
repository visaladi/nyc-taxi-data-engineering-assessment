#!/usr/bin/env bash
#
# run_all.sh - starts every component of both assignments plus the verification console.
#
#   ./run_all.sh            start PostgreSQL, Prefect UI, the streaming stack + Kafka UI, Streamlit and the console
#   ./run_all.sh --etl      additionally (re)run the batch ETL flow before starting the UIs
#   ./run_all.sh --stop     stop all containers (data volumes are kept)
#
# Prerequisite: assignment-1-batch-etl/setup.sh has been run once (venv + data).
#
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BATCH="$ROOT/assignment-1-batch-etl"
STREAM="$ROOT/assignment-2-streaming"
RUN_ETL=false

log() { printf '%s [%-5s] %s\n' "$(date -u +%H:%M:%S)" "$1" "$2"; }

venv_python() {
    if [[ -x "$BATCH/.venv/bin/python" ]]; then echo "$BATCH/.venv/bin/python"
    elif [[ -x "$BATCH/.venv/Scripts/python.exe" ]]; then echo "$BATCH/.venv/Scripts/python.exe"
    else log ERROR "no virtualenv found - run assignment-1-batch-etl/setup.sh first"; exit 1
    fi
}

wait_for() {  # wait_for <description> <command...>
    local what="$1"; shift
    for _ in $(seq 1 60); do
        if "$@" >/dev/null 2>&1; then log INFO "$what is ready"; return 0; fi
        sleep 3
    done
    log WARN "$what did not become ready in time (continuing)"
}

case "${1:-}" in
    --stop)
        (cd "$STREAM" && docker compose --profile ui stop)
        (cd "$BATCH" && docker compose stop)
        log INFO "containers stopped (volumes kept)"
        exit 0 ;;
    --etl) RUN_ETL=true ;;
    "") ;;
    *) sed -n '3,10p' "$0"; exit 1 ;;
esac

PY="$(venv_python)"
mkdir -p "$ROOT/.run"

log INFO "starting PostgreSQL (assignment 1)"
(cd "$BATCH" && docker compose up -d postgres)
wait_for "PostgreSQL" docker exec taxi-batch-postgres pg_isready -U taxi -d taxi_dw

log INFO "starting the Prefect server / UI"
nohup "$PY" -m prefect server start --host 127.0.0.1 --port 4200 > "$ROOT/.run/prefect.log" 2>&1 &
wait_for "Prefect UI" curl -sf http://127.0.0.1:4200/api/health
export PREFECT_API_URL=http://127.0.0.1:4200/api   # flow runs below show up in the Prefect UI

if $RUN_ETL; then
    log INFO "running the batch ETL flow (full months take ~5 min each)"
    (cd "$BATCH" && "$PY" -m flows.taxi_etl_flow run)
fi

log INFO "starting the streaming stack + Kafka UI (assignment 2)"
(cd "$STREAM" && docker compose --profile ui up -d --build)

log INFO "starting the Streamlit dashboard"
(cd "$BATCH" && nohup "$PY" -m streamlit run dashboard/app.py --server.headless true \
    --server.port 8501 --browser.gatherUsageStats false > "$ROOT/.run/streamlit.log" 2>&1 &)
wait_for "Streamlit" curl -sf http://localhost:8501/_stcore/health
wait_for "Grafana" curl -sf http://localhost:3300/api/health
wait_for "Kafka UI" curl -sf http://localhost:8088/actuator/health

cat <<EOF

  Verification console : http://localhost:8765
  Streamlit dashboard  : http://localhost:8501   (assignment 1)
  Prefect UI           : http://127.0.0.1:4200   (assignment 1 flow runs)
  Grafana              : http://localhost:3300   (assignment 2, admin / admin)
  Prometheus           : http://localhost:9090   (assignment 2)
  Kafka UI             : http://localhost:8088   (assignment 2)

  Give the stream ~1 minute to produce data before the first verification run.
  Ctrl+C stops the console; './run_all.sh --stop' stops the containers.

EOF

log INFO "starting the verification console"
cd "$ROOT"
PYTHONDONTWRITEBYTECODE=1 exec "$PY" verification-ui/server.py
