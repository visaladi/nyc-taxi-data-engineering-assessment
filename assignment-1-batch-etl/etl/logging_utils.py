"""Structured JSON-lines logging for the pipeline.

Every record is one JSON object, e.g.

    {"ts": "...", "level": "INFO", "logger": "taxi_etl", "event": "load.finished",
     "step": "load", "source_file": "yellow_tripdata_2023-01.parquet", "rows": 3016403, "duration_s": 41.2}

which makes the log file trivially greppable and ingestible by Loki/ELK/CloudWatch.
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

LOGGER_NAME = "taxi_etl"

# Attributes every LogRecord has; anything else on the record came from `extra=`.
_STANDARD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        payload.update(
            {k: v for k, v in vars(record).items() if k not in _STANDARD_ATTRS and not k.startswith("_")}
        )
        if record.exc_info:
            payload["error_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            payload["traceback"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(log_dir: Path, level: str = "INFO", console: bool = True) -> logging.Logger:
    """Configures the pipeline logger once: JSON to a rotating file and (optionally) to stderr.

    When running inside Prefect, pass console=False: Prefect already prints the records to the
    console and ships them to its API (via PREFECT_LOGGING_EXTRA_LOGGERS=taxi_etl).
    """
    logger = logging.getLogger(LOGGER_NAME)
    if getattr(logger, "_taxi_configured", False):
        return logger

    log_dir.mkdir(parents=True, exist_ok=True)
    formatter = JsonFormatter()

    file_handler = RotatingFileHandler(
        log_dir / "pipeline.jsonl", maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    logger.setLevel(level)
    logger._taxi_configured = True  # type: ignore[attr-defined]
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)


@contextmanager
def log_step(logger: logging.Logger, step: str, **context: Any) -> Iterator[dict[str, Any]]:
    """Logs `<step>.started` / `<step>.finished` (or `<step>.failed`) with the elapsed time.

    The yielded dict can be filled in by the caller (e.g. ctx["rows"] = 123) and its contents
    are attached to the closing log record.
    """
    started = time.perf_counter()
    logger.info(f"{step}.started", extra={"step": step, **context})
    try:
        yield context
    except Exception as exc:
        logger.error(
            f"{step}.failed",
            exc_info=True,
            extra={
                "step": step,
                "duration_s": round(time.perf_counter() - started, 3),
                "error": str(exc),
                **context,
            },
        )
        raise
    logger.info(
        f"{step}.finished",
        extra={"step": step, "duration_s": round(time.perf_counter() - started, 3), **context},
    )
