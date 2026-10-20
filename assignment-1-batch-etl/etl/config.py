"""Runtime configuration, read from environment variables (and the project's .env file)."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PROJECT_ROOT.parent

load_dotenv(PROJECT_ROOT / ".env")

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def parse_months(raw: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
    """Parses 'YYYY-MM' months from a comma/space separated string or a list."""
    items = re.split(r"[,\s]+", raw.strip()) if isinstance(raw, str) else list(raw)
    months = tuple(dict.fromkeys(m for m in items if m))  # de-duplicate, keep order
    invalid = [m for m in months if not _MONTH_PATTERN.match(m)]
    if invalid:
        raise ValueError(f"invalid month(s) {invalid}, expected YYYY-MM")
    return months


def _optional_int(name: str) -> int | None:
    value = os.getenv(name, "").strip()
    return int(value) if value else None


@dataclass(frozen=True)
class Settings:
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    data_dir: Path
    log_dir: Path
    log_level: str
    months: tuple[str, ...]
    trip_data_base_url: str
    zone_lookup_url: str
    max_rows_per_month: int | None
    load_chunk_size: int
    alert_webhook_url: str | None

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def rejected_dir(self) -> Path:
        return self.data_dir / "rejected"

    @property
    def schema_file(self) -> Path:
        return PROJECT_ROOT / "sql" / "schema.sql"

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://{quote_plus(self.pg_user)}:{quote_plus(self.pg_password)}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    def psycopg2_kwargs(self) -> dict:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_database,
            "user": self.pg_user,
            "password": self.pg_password,
            "application_name": "taxi-batch-etl",
            "connect_timeout": 10,
        }

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            pg_host=os.getenv("POSTGRES_HOST", "localhost"),
            pg_port=int(os.getenv("POSTGRES_PORT", "15432")),
            pg_database=os.getenv("POSTGRES_DB", "taxi_dw"),
            pg_user=os.getenv("POSTGRES_USER", "taxi"),
            pg_password=os.getenv("POSTGRES_PASSWORD", "taxi"),
            data_dir=Path(os.getenv("DATA_DIR", REPO_ROOT / "data")),
            log_dir=Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            months=parse_months(os.getenv("ETL_MONTHS", "2023-01,2023-02")),
            trip_data_base_url=os.getenv(
                "TRIP_DATA_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
            ).rstrip("/"),
            zone_lookup_url=os.getenv(
                "ZONE_LOOKUP_URL", "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
            ),
            max_rows_per_month=_optional_int("ETL_MAX_ROWS_PER_MONTH"),
            load_chunk_size=int(os.getenv("ETL_LOAD_CHUNK_SIZE", "250000")),
            alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL") or None,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings.from_env()
