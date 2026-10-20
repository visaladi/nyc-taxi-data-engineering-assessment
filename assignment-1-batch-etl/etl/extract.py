"""Extract: download TLC files and read them into pandas with a normalised schema."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import httpx
import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger("taxi_etl.extract")

# Source column (lower-cased) -> pipeline column name.
SOURCE_COLUMNS: dict[str, str] = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_ts",
    "tpep_dropoff_datetime": "dropoff_ts",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "ratecodeid": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
}

# Columns that were introduced in later years; filled with 0 when absent from a file.
OPTIONAL_COLUMNS = {"congestion_surcharge", "airport_fee"}

_PARQUET_MAGIC = b"PAR1"
_FILE_MONTH = re.compile(r"(\d{4}-\d{2})")


class SchemaValidationError(ValueError):
    """The source file does not contain the columns the pipeline relies on."""


def trip_file_name(month: str) -> str:
    return f"yellow_tripdata_{month}.parquet"


def month_from_file_name(file_name: str) -> str:
    match = _FILE_MONTH.search(file_name)
    if not match:
        raise ValueError(f"cannot infer YYYY-MM from file name {file_name!r}")
    return match.group(1)


def is_valid_parquet(path: Path) -> bool:
    """Cheap integrity check: parquet files start and end with the 4-byte magic 'PAR1'."""
    if not path.is_file() or path.stat().st_size < 12:
        return False
    with path.open("rb") as fh:
        head = fh.read(4)
        fh.seek(-4, 2)
        tail = fh.read(4)
    return head == _PARQUET_MAGIC and tail == _PARQUET_MAGIC


def download_file(url: str, dest: Path, timeout_s: float = 60.0) -> bool:
    """Streams `url` to `dest` atomically. Returns False if a valid file was already present."""
    if dest.exists() and (dest.suffix != ".parquet" or is_valid_parquet(dest)):
        logger.info("download.skipped", extra={"url": url, "path": str(dest), "reason": "already present"})
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with httpx.stream("GET", url, timeout=timeout_s, follow_redirects=True) as response:
        response.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                fh.write(chunk)

    if dest.suffix == ".parquet" and not is_valid_parquet(tmp):
        tmp.unlink(missing_ok=True)
        raise ValueError(f"downloaded file from {url} is not a valid parquet file")
    tmp.replace(dest)
    logger.info("download.completed", extra={"url": url, "path": str(dest), "bytes": dest.stat().st_size})
    return True


def read_trip_file(path: Path, max_rows: int | None = None) -> pd.DataFrame:
    """Reads a TLC yellow-taxi parquet file and returns it with normalised column names.

    Column names differ in case between years (e.g. `airport_fee` vs `Airport_fee`), so the
    file schema is matched case-insensitively. Extra columns (e.g. `cbd_congestion_fee`) are ignored.
    """
    parquet = pq.ParquetFile(path)
    by_lower = {name.lower(): name for name in parquet.schema_arrow.names}

    missing = set(SOURCE_COLUMNS) - OPTIONAL_COLUMNS - set(by_lower)
    if missing:
        raise SchemaValidationError(f"{path.name} is missing required columns: {sorted(missing)}")

    present = [by_lower[c] for c in SOURCE_COLUMNS if c in by_lower]
    if max_rows:
        batches = []
        remaining = max_rows
        for batch in parquet.iter_batches(batch_size=min(remaining, 100_000), columns=present):
            batches.append(batch.slice(0, remaining))
            remaining -= len(batches[-1])
            if remaining <= 0:
                break
        df = pd.concat([b.to_pandas() for b in batches], ignore_index=True) if batches else pd.DataFrame()
    else:
        df = parquet.read(columns=present).to_pandas()

    df = df.rename(columns={name: SOURCE_COLUMNS[name.lower()] for name in df.columns})
    for column in OPTIONAL_COLUMNS:
        target = SOURCE_COLUMNS[column]
        if target not in df.columns:
            df[target] = 0.0
    return df[list(SOURCE_COLUMNS.values())]


def read_zone_lookup(path: Path) -> pd.DataFrame:
    """Reads taxi_zone_lookup.csv into dim_location's shape."""
    zones = pd.read_csv(path)
    zones.columns = [c.strip().lower() for c in zones.columns]
    zones = zones.rename(columns={"locationid": "location_key"})
    zones = zones[["location_key", "borough", "zone", "service_zone"]]
    zones[["borough", "zone", "service_zone"]] = (
        zones[["borough", "zone", "service_zone"]].fillna("Unknown").astype(str).apply(lambda s: s.str.strip())
    )
    return zones.drop_duplicates(subset="location_key")
