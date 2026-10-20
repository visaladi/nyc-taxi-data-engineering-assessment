"""Transform: data-quality rules, then mapping clean trips onto the star schema's fact grain."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field

import pandas as pd

MONEY_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
]
# Optional components of the fare: a missing value means "not charged".
SURCHARGE_COLUMNS = [c for c in MONEY_COLUMNS if c not in ("fare_amount", "total_amount")]

MAX_TRIP_DURATION = pd.Timedelta("6h")
MAX_TRIP_DISTANCE_MILES = 250
MAX_FARE_AMOUNT = 2_000

# "Unknown" members that exist in every dimension (see sql/schema.sql).
UNKNOWN_LOCATION = 264
UNKNOWN_VENDOR = 0
UNKNOWN_RATE_CODE = 99
UNKNOWN_PAYMENT_TYPE = 5

# Column order of the COPY into fact_taxi_trips.
FACT_COLUMNS = [
    "pickup_date_key",
    "pickup_time_key",
    "dropoff_date_key",
    "dropoff_time_key",
    "pickup_location_key",
    "dropoff_location_key",
    "vendor_key",
    "rate_code_key",
    "payment_type_key",
    "pickup_ts",
    "dropoff_ts",
    "store_and_fwd_flag",
    "passenger_count",
    "trip_distance_miles",
    "trip_duration_minutes",
    *MONEY_COLUMNS,
    "source_file",
]


@dataclass
class CleaningReport:
    source_file: str
    rows_in: int
    rows_out: int
    rejected_by_rule: dict[str, int] = field(default_factory=dict)

    @property
    def rows_rejected(self) -> int:
        return self.rows_in - self.rows_out

    def as_dict(self) -> dict:
        return {**asdict(self), "rows_rejected": self.rows_rejected}


@dataclass(frozen=True)
class DimensionKeys:
    """Keys currently present in the dimension tables; codes outside them map to 'Unknown'."""

    locations: frozenset[int]
    vendors: frozenset[int]
    rate_codes: frozenset[int]
    payment_types: frozenset[int]


def _rejection_rules(df: pd.DataFrame, month: str) -> list[tuple[str, pd.Series]]:
    """Ordered (rule name, row is invalid) pairs. A row is attributed to the first rule it fails."""
    month_start = pd.Timestamp(f"{month}-01")
    month_end = month_start + pd.DateOffset(months=1)
    duration = df["dropoff_ts"] - df["pickup_ts"]

    return [
        ("missing_timestamp", df["pickup_ts"].isna() | df["dropoff_ts"].isna()),
        # TLC files contain a handful of trips stamped years away from the file's month (clock errors).
        ("pickup_outside_file_month", (df["pickup_ts"] < month_start) | (df["pickup_ts"] >= month_end)),
        ("non_positive_duration", duration <= pd.Timedelta(0)),
        ("duration_too_long", duration > MAX_TRIP_DURATION),
        ("invalid_distance", ~df["trip_distance"].between(0, MAX_TRIP_DISTANCE_MILES, inclusive="right")),
        ("missing_amount", df[["fare_amount", "total_amount"]].isna().any(axis=1)),
        # Negative amounts are refunds / void adjustments, not revenue-generating trips.
        ("negative_amount", (df[MONEY_COLUMNS] < 0).any(axis=1)),
        ("implausible_fare", df["fare_amount"] > MAX_FARE_AMOUNT),
        ("missing_location", df["pu_location_id"].isna() | df["do_location_id"].isna()),
    ]


def clean_trips(
    df: pd.DataFrame, month: str, source_file: str
) -> tuple[pd.DataFrame, pd.DataFrame, CleaningReport]:
    """Applies the data-quality rules.

    Returns (clean rows, rejected rows with a `reject_reason` column, report with counts per rule).
    """
    df = df.copy()
    df[SURCHARGE_COLUMNS] = df[SURCHARGE_COLUMNS].fillna(0.0)

    rejected = pd.Series(False, index=df.index)
    reason = pd.Series(pd.NA, index=df.index, dtype="string")
    counts: dict[str, int] = {}

    for rule, invalid in _rejection_rules(df, month):
        newly_rejected = invalid.fillna(False).astype(bool) & ~rejected
        counts[rule] = int(newly_rejected.sum())
        reason[newly_rejected] = rule
        rejected |= newly_rejected

    duplicated = df[~rejected].duplicated(keep="first")
    duplicate_index = duplicated[duplicated].index
    counts["duplicate_row"] = len(duplicate_index)
    reason[duplicate_index] = "duplicate_row"
    rejected[duplicate_index] = True

    clean = df[~rejected].reset_index(drop=True)
    rejects = df[rejected].assign(reject_reason=reason[rejected]).reset_index(drop=True)
    report = CleaningReport(
        source_file=source_file,
        rows_in=len(df),
        rows_out=len(clean),
        rejected_by_rule={rule: n for rule, n in counts.items() if n},
    )
    return clean, rejects, report


def _date_key(ts: pd.Series) -> pd.Series:
    return (ts.dt.year * 10_000 + ts.dt.month * 100 + ts.dt.day).astype("int32")


def _time_key(ts: pd.Series) -> pd.Series:
    return (ts.dt.hour * 100 + ts.dt.minute).astype("int16")


def _conform(codes: pd.Series, valid: frozenset[int], unknown: int) -> pd.Series:
    """Maps nulls and codes missing from the dimension to the dimension's 'Unknown' member."""
    codes = pd.to_numeric(codes, errors="coerce").fillna(unknown).astype("int64")
    return codes.where(codes.isin(valid), unknown).astype("int16")


def build_fact_frame(clean: pd.DataFrame, source_file: str, keys: DimensionKeys) -> pd.DataFrame:
    """Maps clean trips to fact_taxi_trips rows (surrogate/dimension keys + measures)."""
    pickup, dropoff = clean["pickup_ts"], clean["dropoff_ts"]

    fact = pd.DataFrame(
        {
            "pickup_date_key": _date_key(pickup),
            "pickup_time_key": _time_key(pickup),
            "dropoff_date_key": _date_key(dropoff),
            "dropoff_time_key": _time_key(dropoff),
            "pickup_location_key": _conform(clean["pu_location_id"], keys.locations, UNKNOWN_LOCATION),
            "dropoff_location_key": _conform(clean["do_location_id"], keys.locations, UNKNOWN_LOCATION),
            "vendor_key": _conform(clean["vendor_id"], keys.vendors, UNKNOWN_VENDOR),
            "rate_code_key": _conform(clean["rate_code_id"], keys.rate_codes, UNKNOWN_RATE_CODE),
            "payment_type_key": _conform(clean["payment_type"], keys.payment_types, UNKNOWN_PAYMENT_TYPE),
            "pickup_ts": pickup,
            "dropoff_ts": dropoff,
            "store_and_fwd_flag": clean["store_and_fwd_flag"].map({"Y": True, "N": False}).astype("boolean"),
            "passenger_count": clean["passenger_count"].round().astype("Int16"),
            "trip_distance_miles": clean["trip_distance"].round(2),
            "trip_duration_minutes": ((dropoff - pickup).dt.total_seconds() / 60).round(2),
            **{column: clean[column].round(2) for column in MONEY_COLUMNS},
            "source_file": source_file,
        }
    )
    return fact[FACT_COLUMNS]
