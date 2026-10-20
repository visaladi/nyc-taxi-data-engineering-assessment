#!/usr/bin/env bash
#
# setup.sh - bootstrap the local NYC taxi batch ETL environment.
#
#   1. checks prerequisites (python >= 3.10, docker, curl)
#   2. creates a Python virtual environment and installs dependencies
#   3. starts PostgreSQL in Docker and waits until it is healthy
#   4. downloads the requested months of NYC yellow taxi data + the zone lookup
#   5. applies the star schema (sql/schema.sql)
#
# Usage:
#   ./setup.sh                                   # full setup, months 2023-01 and 2023-02
#   ./setup.sh --months "2023-01 2023-02 2023-03"
#   ./setup.sh --download-only                   # only fetch data files
#   ./setup.sh --synthetic                       # generate fake data instead of downloading
#   ./setup.sh --skip-venv --skip-db             # mix and match
#
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"
RAW_DIR="${RAW_DIR:-$REPO_ROOT/data/raw}"
TRIP_DATA_BASE_URL="${TRIP_DATA_BASE_URL:-https://d37ci6vzurychx.cloudfront.net/trip-data}"
ZONE_LOOKUP_URL="${ZONE_LOOKUP_URL:-https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv}"
DB_WAIT_SECONDS="${DB_WAIT_SECONDS:-90}"

MONTHS="2023-01 2023-02"
DO_VENV=true
DO_DB=true
DO_DOWNLOAD=true
SYNTHETIC=false

log() { printf '%s [%-5s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2"; }
die() { log ERROR "$1"; exit 1; }
trap 'log ERROR "setup failed at line $LINENO: $BASH_COMMAND"' ERR

usage() {
    sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --months)        MONTHS="$2"; shift 2 ;;
            --skip-venv)     DO_VENV=false; shift ;;
            --skip-db)       DO_DB=false; shift ;;
            --skip-download) DO_DOWNLOAD=false; shift ;;
            --download-only) DO_VENV=false; DO_DB=false; shift ;;
            --synthetic)     SYNTHETIC=true; shift ;;
            -h|--help)       usage ;;
            *)               die "unknown argument: $1 (see --help)" ;;
        esac
    done
    for month in $MONTHS; do
        [[ "$month" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]] || die "invalid month '$month', expected YYYY-MM"
    done
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "'$1' is required but was not found on PATH"
}

# Finds a Python >= 3.10 interpreter. Honours $PYTHON, prefers well-supported
# versions, and falls back to the Windows "py" launcher when running in Git Bash.
find_python() {
    local candidates=()
    [[ -n "${PYTHON:-}" ]] && candidates+=("$PYTHON")
    candidates+=(python3.12 python3.11 python3.13 python3.10 python3 python "py -3.12" "py -3.11" "py -3")
    local candidate
    for candidate in "${candidates[@]}"; do
        # shellcheck disable=SC2086  # intentional word splitting for "py -3.x"
        if $candidate -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' >/dev/null 2>&1; then
            PYTHON_CMD="$candidate"
            return 0
        fi
    done
    die "Python >= 3.10 not found (set PYTHON=/path/to/python to override)"
}

check_prerequisites() {
    log INFO "checking prerequisites"
    require_cmd curl
    if $DO_DB; then
        require_cmd docker
        docker info >/dev/null 2>&1 || die "docker daemon is not running"
        docker compose version >/dev/null 2>&1 || die "docker compose v2 plugin is required"
    fi
    if $DO_VENV || $SYNTHETIC; then
        find_python
        log INFO "using python: $PYTHON_CMD ($($PYTHON_CMD --version 2>&1))"
    fi
}

venv_python() {
    if [[ -x "$VENV_DIR/bin/python" ]]; then
        echo "$VENV_DIR/bin/python"
    else
        echo "$VENV_DIR/Scripts/python.exe"   # Windows layout
    fi
}

create_venv() {
    if [[ ! -d "$VENV_DIR" ]]; then
        log INFO "creating virtual environment in $VENV_DIR"
        $PYTHON_CMD -m venv "$VENV_DIR"
    else
        log INFO "virtual environment already exists, reusing it"
    fi
    local py
    py="$(venv_python)"
    log INFO "installing python dependencies"
    "$py" -m pip install --quiet --upgrade pip
    "$py" -m pip install --quiet -r "$SCRIPT_DIR/requirements.txt"
}

ensure_env_file() {
    if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
        cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
        log INFO "created .env from .env.example"
    fi
    set -a
    # shellcheck disable=SC1091
    source "$SCRIPT_DIR/.env"
    set +a
}

start_database() {
    log INFO "starting postgres container"
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d postgres

    local container status waited=0
    container="$(docker compose -f "$SCRIPT_DIR/docker-compose.yml" ps -q postgres)"
    while (( waited < DB_WAIT_SECONDS )); do
        status="$(docker inspect --format '{{.State.Health.Status}}' "$container" 2>/dev/null || echo unknown)"
        if [[ "$status" == "healthy" ]]; then
            log INFO "postgres is healthy after ${waited}s"
            return 0
        fi
        sleep 3
        waited=$(( waited + 3 ))
    done
    die "postgres did not become healthy within ${DB_WAIT_SECONDS}s (last status: $status)"
}

apply_schema() {
    log INFO "applying sql/schema.sql"
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T \
        -e PGOPTIONS="-c client_min_messages=warning" postgres \
        psql -v ON_ERROR_STOP=1 -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" < "$SCRIPT_DIR/sql/schema.sql"
}

# A parquet file starts and ends with the 4-byte magic "PAR1".
is_valid_parquet() {
    local file="$1"
    [[ -s "$file" ]] || return 1
    [[ "$(head -c 4 "$file")" == "PAR1" && "$(tail -c 4 "$file")" == "PAR1" ]]
}

download() {
    local url="$1" dest="$2"
    local tmp="$dest.part"
    curl --fail --location --silent --show-error \
         --retry 3 --retry-delay 5 --connect-timeout 20 \
         --output "$tmp" "$url"
    mv "$tmp" "$dest"
}

download_data() {
    mkdir -p "$RAW_DIR"
    local month file dest
    for month in $MONTHS; do
        file="yellow_tripdata_${month}.parquet"
        dest="$RAW_DIR/$file"
        if is_valid_parquet "$dest"; then
            log INFO "$file already present, skipping"
            continue
        fi
        log INFO "downloading $file"
        download "$TRIP_DATA_BASE_URL/$file" "$dest"
        is_valid_parquet "$dest" || die "$file is not a valid parquet file"
        log INFO "downloaded $file ($(du -h "$dest" | cut -f1))"
    done

    if [[ ! -s "$RAW_DIR/taxi_zone_lookup.csv" ]]; then
        log INFO "downloading taxi_zone_lookup.csv"
        download "$ZONE_LOOKUP_URL" "$RAW_DIR/taxi_zone_lookup.csv"
    fi
}

generate_synthetic_data() {
    local py
    if [[ -d "$VENV_DIR" ]]; then py="$(venv_python)"; else py="$PYTHON_CMD"; fi
    log INFO "generating synthetic data for: $MONTHS"
    # shellcheck disable=SC2086
    $py "$SCRIPT_DIR/scripts/generate_sample_data.py" --output-dir "$RAW_DIR" --months $MONTHS
}

main() {
    parse_args "$@"
    log INFO "setup started (months: $MONTHS)"
    check_prerequisites

    $DO_VENV && create_venv
    ensure_env_file
    $DO_DB && start_database

    if $SYNTHETIC; then
        generate_synthetic_data
    elif $DO_DOWNLOAD; then
        download_data
    fi

    $DO_DB && apply_schema

    log INFO "setup finished"
    if $DO_VENV; then
        cat <<EOF

Next steps:
  source .venv/bin/activate            # Windows Git Bash: source .venv/Scripts/activate
  python -m flows.taxi_etl_flow run --months $MONTHS
  streamlit run dashboard/app.py
EOF
    fi
}

main "$@"
