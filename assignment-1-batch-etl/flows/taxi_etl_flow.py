"""Prefect orchestration of the NYC taxi batch ETL.

Flow layout:

    nyc-taxi-batch-etl                      (parent flow: alerts, run summary artifact)
      ├── prepare-database                  (schema + zone dimension)
      ├── process-month  x N                (one sub-flow per source file, isolated failures)
      │     ├── download-trip-file          (retries with exponential backoff)
      │     ├── extract-trips
      │     ├── clean-trips                 (data-quality rules, rejected rows kept for audit)
      │     ├── build-fact-rows             (dimension key lookups)
      │     └── load-fact-month             (transactional, idempotent COPY into a monthly partition)
      └── refresh-aggregates                (hourly aggregate used by the dashboard)

Usage:
    python -m flows.taxi_etl_flow run --months 2023-01 2023-02 [--max-rows 200000]
    python -m flows.taxi_etl_flow serve --cron "0 6 5 * *"
"""

from __future__ import annotations

import os

# Must be set before Prefect is imported so its log handlers attach to the pipeline logger.
os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "taxi_etl")

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.cache_policies import NO_CACHE
from prefect.client.schemas.objects import Flow, FlowRun
from prefect.runtime import flow_run as current_flow_run
from prefect.states import State
from prefect.tasks import exponential_backoff

from etl import extract, load, transform
from etl.config import get_settings, parse_months
from etl.logging_utils import configure_logging, log_step

settings = get_settings()
logger = configure_logging(settings.log_dir, settings.log_level, console=False)


class PipelineError(RuntimeError):
    """Raised when at least one month failed, so the parent flow ends in a Failed state."""


# --------------------------------------------------------------------------------------
# Alerting hooks
# --------------------------------------------------------------------------------------
def _send_alert(flow: Flow, flow_run: FlowRun, state: State) -> None:
    """Failure hook: structured error log + alerts.jsonl entry + optional webhook (Slack/Teams)."""
    alert = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "flow": flow.name,
        "flow_run": flow_run.name,
        "flow_run_id": str(flow_run.id),
        "state": state.name,
        "state_message": state.message,  # not "message": that name is reserved on LogRecord
    }
    logger.error("pipeline.alert", extra=alert)

    settings.log_dir.mkdir(parents=True, exist_ok=True)
    with (settings.log_dir / "alerts.jsonl").open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(alert) + "\n")

    if settings.alert_webhook_url:
        text = f":rotating_light: Prefect flow *{flow.name}* run `{flow_run.name}` is {state.name}: {state.message}"
        try:
            httpx.post(settings.alert_webhook_url, json={"text": text}, timeout=10).raise_for_status()
        except httpx.HTTPError as exc:  # alerting must never mask the original failure
            logger.warning("pipeline.alert_webhook_failed", extra={"error": str(exc)})


def _log_success(flow: Flow, flow_run: FlowRun, state: State) -> None:
    logger.info("pipeline.succeeded", extra={"flow": flow.name, "flow_run": flow_run.name})


# --------------------------------------------------------------------------------------
# Tasks
# --------------------------------------------------------------------------------------
@task(
    name="download-trip-file",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.5,
    tags=["network"],
)
def download_trip_file(month: str) -> Path:
    dest = settings.raw_dir / extract.trip_file_name(month)
    url = f"{settings.trip_data_base_url}/{dest.name}"
    with log_step(logger, "download", month=month, source_file=dest.name) as ctx:
        ctx["downloaded"] = extract.download_file(url, dest)
    return dest


# Escalating delays ride out a database that is still starting or recovering.
@task(name="prepare-database", retries=3, retry_delay_seconds=[10, 30, 60])
def prepare_database() -> dict[str, int]:
    zone_file = settings.raw_dir / "taxi_zone_lookup.csv"
    with log_step(logger, "prepare_database") as ctx:
        extract.download_file(settings.zone_lookup_url, zone_file)
        with load.connect(settings) as conn:
            load.apply_schema(conn, settings.schema_file)
            ctx["zones_upserted"] = load.upsert_locations(conn, extract.read_zone_lookup(zone_file))
    return {"zones_upserted": ctx["zones_upserted"]}


@task(name="extract-trips", cache_policy=NO_CACHE)
def extract_trips(path: Path, max_rows: int | None) -> pd.DataFrame:
    with log_step(logger, "extract", source_file=path.name, max_rows=max_rows) as ctx:
        df = extract.read_trip_file(path, max_rows)
        ctx["rows"] = len(df)
    return df


@task(name="clean-trips", cache_policy=NO_CACHE)
def clean_trips(df: pd.DataFrame, month: str, source_file: str) -> tuple[pd.DataFrame, transform.CleaningReport]:
    with log_step(logger, "clean", source_file=source_file) as ctx:
        clean, rejects, report = transform.clean_trips(df, month, source_file)
        ctx.update(rows_in=report.rows_in, rows_out=report.rows_out, rejected_by_rule=report.rejected_by_rule)

        if len(rejects):
            settings.rejected_dir.mkdir(parents=True, exist_ok=True)
            rejects_path = settings.rejected_dir / f"rejected_{source_file}"
            rejects.to_parquet(rejects_path, index=False)
            ctx["rejects_path"] = str(rejects_path)
    return clean, report


@task(name="build-fact-rows", cache_policy=NO_CACHE)
def build_fact_rows(clean: pd.DataFrame, source_file: str) -> pd.DataFrame:
    with log_step(logger, "transform", source_file=source_file) as ctx:
        with load.connect(settings) as conn:
            keys = load.fetch_dimension_keys(conn)
        fact = transform.build_fact_frame(clean, source_file, keys)
        ctx["rows"] = len(fact)
    return fact


@task(name="load-fact-month", cache_policy=NO_CACHE, retries=2, retry_delay_seconds=30)
def load_fact_month(fact: pd.DataFrame, source_file: str, month: str) -> int:
    with log_step(logger, "load", source_file=source_file, rows=len(fact)) as ctx:
        with load.connect(settings) as conn:
            ctx["rows_loaded"] = load.load_fact_month(conn, fact, source_file, month, settings.load_chunk_size)
    return ctx["rows_loaded"]


@task(name="refresh-aggregates", retries=1, retry_delay_seconds=30)
def refresh_aggregates() -> int:
    with log_step(logger, "refresh_aggregates") as ctx:
        with load.connect(settings) as conn:
            ctx["aggregate_rows"] = load.refresh_aggregates(conn)
    return ctx["aggregate_rows"]


# --------------------------------------------------------------------------------------
# Flows
# --------------------------------------------------------------------------------------
@flow(name="process-month", flow_run_name="process-{month}")
def process_month(month: str, max_rows: int | None = None) -> dict[str, Any]:
    source_file = extract.trip_file_name(month)
    with load.connect(settings) as conn:
        run_id = load.start_run(conn, source_file, current_flow_run.id)

    report: transform.CleaningReport | None = None
    try:
        path = download_trip_file(month)
        raw = extract_trips(path, max_rows)
        clean, report = clean_trips(raw, month, source_file)
        del raw  # release memory before building the fact frame
        fact = build_fact_rows(clean, source_file)
        del clean
        rows_loaded = load_fact_month(fact, source_file, month)
    except Exception as exc:
        with load.connect(settings) as conn:
            load.finish_run(conn, run_id, "failed", report=report, error=f"{type(exc).__name__}: {exc}")
        raise

    with load.connect(settings) as conn:
        load.finish_run(conn, run_id, "success", report=report, rows_loaded=rows_loaded)

    return {
        "month": month,
        "status": "success",
        "rows_extracted": report.rows_in,
        "rows_rejected": report.rows_rejected,
        "rows_loaded": rows_loaded,
        "reject_rate_pct": round(100 * report.rows_rejected / max(report.rows_in, 1), 2),
    }


@flow(
    name="nyc-taxi-batch-etl",
    description="Downloads NYC yellow taxi trips, cleans them and loads the PostgreSQL star schema.",
    on_completion=[_log_success],
    on_failure=[_send_alert],
    on_crashed=[_send_alert],
)
def nyc_taxi_etl(months: list[str] | None = None, max_rows_per_month: int | None = None) -> list[dict[str, Any]]:
    months = list(parse_months(months or settings.months))
    max_rows = max_rows_per_month or settings.max_rows_per_month
    started = time.perf_counter()
    logger.info("pipeline.started", extra={"months": months, "max_rows_per_month": max_rows})

    prepare_database()

    results: list[dict[str, Any]] = []
    for month in months:
        try:
            results.append(process_month(month, max_rows))
        except Exception as exc:  # keep processing the other months; fail the parent at the end
            results.append({"month": month, "status": "failed", "error": f"{type(exc).__name__}: {exc}"})

    failed = [r["month"] for r in results if r["status"] == "failed"]
    if len(failed) < len(months):  # at least one month changed the fact table
        refresh_aggregates()

    summary = {
        "months": months,
        "failed_months": failed,
        "rows_loaded": sum(r.get("rows_loaded", 0) for r in results),
        "duration_s": round(time.perf_counter() - started, 2),
    }
    create_table_artifact(
        key="taxi-etl-run-summary",
        table=results,
        description=f"Per-month results. Loaded {summary['rows_loaded']:,} rows in {summary['duration_s']}s.",
    )
    logger.info("pipeline.finished", extra=summary)

    if failed:
        create_markdown_artifact(
            key="taxi-etl-failures",
            markdown="\n".join(f"* **{r['month']}**: {r['error']}" for r in results if r["status"] == "failed"),
        )
        raise PipelineError(f"{len(failed)} of {len(months)} month(s) failed: {', '.join(failed)}")
    return results


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the pipeline once")
    run.add_argument("--months", nargs="+", help="YYYY-MM months (default: ETL_MONTHS)")
    run.add_argument("--max-rows", type=int, help="cap rows per month (demo mode)")

    serve = sub.add_parser("serve", help="serve the flow on a cron schedule (needs `prefect server start`)")
    serve.add_argument("--cron", default="0 6 5 * *", help="cron schedule (default: 06:00 on the 5th)")
    serve.add_argument("--months", nargs="+", help="YYYY-MM months (default: ETL_MONTHS)")

    args = parser.parse_args()
    if args.command == "run":
        nyc_taxi_etl(months=args.months, max_rows_per_month=args.max_rows)
    else:
        nyc_taxi_etl.serve(
            name="nyc-taxi-monthly",
            cron=args.cron,
            parameters={"months": args.months} if args.months else {},
            tags=["batch", "nyc-taxi"],
        )


if __name__ == "__main__":
    main()
