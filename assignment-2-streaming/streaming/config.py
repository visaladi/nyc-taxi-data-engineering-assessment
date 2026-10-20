"""Environment-driven configuration for the producer and the stream processor."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str = field(
        default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")
    )
    topic: str = field(default_factory=lambda: _env("KAFKA_TOPIC", "taxi-trips-stream"))
    dlq_topic: str = field(default_factory=lambda: _env("KAFKA_DLQ_TOPIC", "taxi-trips-dlq"))


@dataclass(frozen=True)
class ProducerSettings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    source_file: Path = field(
        default_factory=lambda: Path(_env("PRODUCER_SOURCE_FILE", "../data/raw/yellow_tripdata_2023-01.parquet"))
    )
    rate_profile: str = field(default_factory=lambda: _env("PRODUCER_RATE_PROFILE", "wave"))  # constant | wave
    rate: float = field(default_factory=lambda: _env_float("PRODUCER_RATE", 25))  # used by "constant"
    min_rate: float = field(default_factory=lambda: _env_float("PRODUCER_MIN_RATE", 10))
    max_rate: float = field(default_factory=lambda: _env_float("PRODUCER_MAX_RATE", 50))
    wave_period_s: float = field(default_factory=lambda: _env_float("PRODUCER_WAVE_PERIOD_SECONDS", 180))
    out_of_order_ratio: float = field(default_factory=lambda: _env_float("PRODUCER_OUT_OF_ORDER_RATIO", 0.05))
    max_delay_s: float = field(default_factory=lambda: _env_float("PRODUCER_MAX_DELAY_SECONDS", 30))
    max_events: int = field(default_factory=lambda: _env_int("PRODUCER_MAX_EVENTS", 0))  # 0 = whole file
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 8000))
    seed: int = field(default_factory=lambda: _env_int("PRODUCER_SEED", 7))


@dataclass(frozen=True)
class ProcessorSettings:
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    group_id: str = field(default_factory=lambda: _env("KAFKA_GROUP_ID", "taxi-stream-processor"))
    batch_size: int = field(default_factory=lambda: _env_int("PROCESSOR_BATCH_SIZE", 500))
    poll_timeout_s: float = field(default_factory=lambda: _env_float("PROCESSOR_POLL_TIMEOUT_SECONDS", 1.0))
    window_size_s: int = field(default_factory=lambda: _env_int("WINDOW_SIZE_SECONDS", 900))
    window_slide_s: int = field(default_factory=lambda: _env_int("WINDOW_SLIDE_SECONDS", 300))
    allowed_lateness_s: int = field(default_factory=lambda: _env_int("ALLOWED_LATENESS_SECONDS", 600))
    max_watermark_jump_s: int = field(default_factory=lambda: _env_int("MAX_WATERMARK_JUMP_SECONDS", 3600))
    max_db_retry_s: float = field(default_factory=lambda: _env_float("MAX_DB_RETRY_SECONDS", 120))
    zone_lookup_file: Path = field(
        default_factory=lambda: Path(_env("ZONE_LOOKUP_FILE", "../data/raw/taxi_zone_lookup.csv"))
    )
    pg_host: str = field(default_factory=lambda: _env("POSTGRES_HOST", "localhost"))
    pg_port: int = field(default_factory=lambda: _env_int("POSTGRES_PORT", 15433))
    pg_database: str = field(default_factory=lambda: _env("POSTGRES_DB", "taxi_stream"))
    pg_user: str = field(default_factory=lambda: _env("POSTGRES_USER", "taxi"))
    pg_password: str = field(default_factory=lambda: _env("POSTGRES_PASSWORD", "taxi"))
    metrics_port: int = field(default_factory=lambda: _env_int("METRICS_PORT", 8001))

    def pg_kwargs(self) -> dict:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
            "application_name": "taxi-stream-processor",
            "connect_timeout": 5,
        }
