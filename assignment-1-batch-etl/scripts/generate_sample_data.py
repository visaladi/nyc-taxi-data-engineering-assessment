"""Generates synthetic NYC yellow-taxi parquet files with the real TLC schema.

Used when the public dataset cannot be downloaded (offline demo, firewall, ...). The data has a
realistic shape (rush-hour peaks, card vs cash mix, fare ~ distance) and deliberately contains a
small share of dirty rows so every data-quality rule in the pipeline gets exercised.

    python scripts/generate_sample_data.py --months 2023-01 2023-02 --rows 200000
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

# Relative pickup volume per hour of day (roughly the shape of real yellow-taxi demand).
HOURLY_WEIGHTS = np.array(
    [3.0, 2.0, 1.4, 1.0, 0.8, 0.9, 1.8, 3.2, 4.2, 4.4, 4.5, 4.8,
     5.1, 5.3, 5.6, 5.8, 5.9, 6.4, 6.9, 6.3, 5.6, 5.4, 5.1, 4.1]
)
BOROUGH_ZONES = {
    "Manhattan": range(1, 120),
    "Brooklyn": range(120, 170),
    "Queens": range(170, 230),
    "Bronx": range(230, 255),
    "Staten Island": range(255, 262),
    "EWR": range(262, 264),
}
AIRPORT_ZONES = np.array([132, 138])


def write_zone_lookup(path: Path) -> None:
    rows = [
        {"LocationID": zone, "Borough": borough, "Zone": f"{borough} zone {zone}",
         "service_zone": "Yellow Zone" if borough == "Manhattan" else "Boro Zone"}
        for borough, zones in BOROUGH_ZONES.items()
        for zone in zones
    ]
    rows += [
        {"LocationID": 264, "Borough": "Unknown", "Zone": "Unknown", "service_zone": "N/A"},
        {"LocationID": 265, "Borough": "N/A", "Zone": "Outside of NYC", "service_zone": "N/A"},
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def generate_month(month: str, rows: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    start = pd.Timestamp(f"{month}-01")
    days_in_month = (start + pd.DateOffset(months=1) - start).days

    hours = rng.choice(24, size=rows, p=HOURLY_WEIGHTS / HOURLY_WEIGHTS.sum())
    pickup = (
        start
        + pd.to_timedelta(rng.integers(0, days_in_month, rows), unit="D")
        + pd.to_timedelta(hours, unit="h")
        + pd.to_timedelta(rng.integers(0, 3600, rows), unit="s")
    )
    distance = np.round(rng.gamma(shape=1.8, scale=1.9, size=rows) + 0.1, 2)
    duration_min = distance * rng.uniform(2.5, 5.5, rows) + rng.uniform(1, 8, rows)
    dropoff = pickup + pd.to_timedelta(np.round(duration_min * 60), unit="s")

    # Manhattan-heavy pickups like the real data.
    pu = np.where(rng.random(rows) < 0.85, rng.integers(1, 120, rows), rng.integers(120, 264, rows))
    airport_trip = rng.random(rows) < 0.04
    pu[airport_trip] = rng.choice(AIRPORT_ZONES, size=int(airport_trip.sum()))
    do = np.where(rng.random(rows) < 0.8, rng.integers(1, 120, rows), rng.integers(120, 266, rows))

    payment = rng.choice([1, 2, 3, 4, 0], size=rows, p=[0.76, 0.18, 0.01, 0.01, 0.04])
    fare = np.round(3.0 + 2.8 * distance + 0.45 * duration_min, 2)
    tip = np.where(payment == 1, np.round(fare * rng.uniform(0.1, 0.3, rows), 2), 0.0)
    extra = rng.choice([0.0, 1.0, 2.5], size=rows, p=[0.45, 0.35, 0.20])
    tolls = np.where(rng.random(rows) < 0.05, 6.55, 0.0)
    congestion = np.where(rng.random(rows) < 0.9, 2.5, 0.0)
    airport_fee = np.where(np.isin(pu, AIRPORT_ZONES), 1.25, 0.0)
    total = np.round(fare + tip + extra + tolls + congestion + airport_fee + 0.5 + 1.0, 2)

    df = pd.DataFrame(
        {
            "VendorID": rng.choice([1, 2], size=rows, p=[0.3, 0.7]).astype("int32"),
            "tpep_pickup_datetime": pickup.astype("datetime64[us]"),
            "tpep_dropoff_datetime": dropoff.astype("datetime64[us]"),
            "passenger_count": rng.choice([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], size=rows,
                                          p=[0.72, 0.15, 0.05, 0.03, 0.03, 0.02]),
            "trip_distance": distance,
            "RatecodeID": rng.choice([1.0, 2.0, 3.0, 5.0], size=rows, p=[0.94, 0.04, 0.005, 0.015]),
            "store_and_fwd_flag": rng.choice(["N", "Y"], size=rows, p=[0.995, 0.005]),
            "PULocationID": pu.astype("int32"),
            "DOLocationID": do.astype("int32"),
            "payment_type": payment.astype("int64"),
            "fare_amount": fare,
            "extra": extra,
            "mta_tax": 0.5,
            "tip_amount": tip,
            "tolls_amount": tolls,
            "improvement_surcharge": 1.0,
            "total_amount": total,
            "congestion_surcharge": congestion,
            "airport_fee": airport_fee,
        }
    )
    return _inject_dirty_rows(df, rng, start)


def _inject_dirty_rows(df: pd.DataFrame, rng: np.random.Generator, month_start: pd.Timestamp) -> pd.DataFrame:
    """Corrupts ~3% of rows the way real TLC files are corrupted."""
    n = len(df)

    def pick(share: float) -> np.ndarray:
        return rng.choice(n, size=max(1, int(n * share)), replace=False)

    idx = pick(0.01)
    df.loc[idx, ["passenger_count", "RatecodeID", "store_and_fwd_flag",
                 "congestion_surcharge", "airport_fee"]] = np.nan          # missing optional fields
    df.loc[pick(0.008), "trip_distance"] = 0.0                              # zero-distance trips
    neg = pick(0.006)
    df.loc[neg, ["fare_amount", "total_amount"]] = -df.loc[neg, ["fare_amount", "total_amount"]]  # refunds
    swap = pick(0.002)
    df.loc[swap, ["tpep_pickup_datetime", "tpep_dropoff_datetime"]] = (
        df.loc[swap, ["tpep_dropoff_datetime", "tpep_pickup_datetime"]].to_numpy()  # dropoff before pickup
    )
    df.loc[pick(0.0005), "tpep_pickup_datetime"] = month_start - pd.DateOffset(years=14)  # clock errors
    duplicates = df.iloc[pick(0.002)]
    return pd.concat([df, duplicates], ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--months", nargs="+", default=["2023-01", "2023-02"])
    parser.add_argument("--rows", type=int, default=200_000, help="rows per month")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data" / "raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    zone_file = args.output_dir / "taxi_zone_lookup.csv"
    if not zone_file.exists():
        write_zone_lookup(zone_file)
        print(f"wrote {zone_file}")

    for i, month in enumerate(args.months):
        path = args.output_dir / f"yellow_tripdata_{month}.parquet"
        generate_month(month, args.rows, args.seed + i).to_parquet(path, index=False)
        print(f"wrote {path} ({args.rows:,} rows + dirty duplicates)")


if __name__ == "__main__":
    main()
