import pandas as pd
import pytest

from etl import transform
from etl.transform import DimensionKeys

MONTH = "2023-01"


def make_trip(**overrides) -> dict:
    trip = {
        "vendor_id": 2,
        "pickup_ts": pd.Timestamp("2023-01-15 08:30:00"),
        "dropoff_ts": pd.Timestamp("2023-01-15 08:50:00"),
        "passenger_count": 1.0,
        "trip_distance": 3.2,
        "rate_code_id": 1.0,
        "store_and_fwd_flag": "N",
        "pu_location_id": 161,
        "do_location_id": 236,
        "payment_type": 1,
        "fare_amount": 17.0,
        "extra": 2.5,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 27.5,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }
    trip.update(overrides)
    return trip


def frame(*trips: dict) -> pd.DataFrame:
    return pd.DataFrame(list(trips))


KEYS = DimensionKeys(
    locations=frozenset({161, 236, 264, 265}),
    vendors=frozenset({0, 1, 2}),
    rate_codes=frozenset({1, 2, 99}),
    payment_types=frozenset({0, 1, 2, 5}),
)


@pytest.mark.parametrize(
    ("overrides", "rule"),
    [
        ({"pickup_ts": pd.NaT}, "missing_timestamp"),
        ({"pickup_ts": pd.Timestamp("2009-01-01 10:00")}, "pickup_outside_file_month"),
        ({"dropoff_ts": pd.Timestamp("2023-01-15 08:00:00")}, "non_positive_duration"),
        ({"dropoff_ts": pd.Timestamp("2023-01-15 20:00:00")}, "duration_too_long"),
        ({"trip_distance": 0.0}, "invalid_distance"),
        ({"trip_distance": 900.0}, "invalid_distance"),
        ({"total_amount": float("nan")}, "missing_amount"),
        ({"fare_amount": -17.0}, "negative_amount"),
        ({"fare_amount": 50_000.0}, "implausible_fare"),
        ({"pu_location_id": float("nan")}, "missing_location"),
    ],
)
def test_each_rule_rejects_bad_rows(overrides, rule):
    clean, rejects, report = transform.clean_trips(frame(make_trip(), make_trip(**overrides)), MONTH, "f")

    assert len(clean) == 1
    assert rejects["reject_reason"].tolist() == [rule]
    assert report.rejected_by_rule == {rule: 1}


def test_row_is_attributed_to_first_failing_rule_only():
    bad = make_trip(pickup_ts=pd.Timestamp("2009-01-01"), fare_amount=-5.0)
    _, _, report = transform.clean_trips(frame(bad), MONTH, "f")

    assert report.rejected_by_rule == {"pickup_outside_file_month": 1}
    assert report.rows_rejected == 1


def test_exact_duplicates_are_removed():
    clean, _, report = transform.clean_trips(frame(make_trip(), make_trip(), make_trip()), MONTH, "f")

    assert len(clean) == 1
    assert report.rejected_by_rule == {"duplicate_row": 2}


def test_missing_surcharges_default_to_zero():
    trip = make_trip(airport_fee=float("nan"), congestion_surcharge=float("nan"))
    clean, _, _ = transform.clean_trips(frame(trip), MONTH, "f")

    assert clean.loc[0, "airport_fee"] == 0.0
    assert clean.loc[0, "congestion_surcharge"] == 0.0


def test_fact_frame_keys_and_measures():
    clean, _, _ = transform.clean_trips(frame(make_trip()), MONTH, "f")
    fact = transform.build_fact_frame(clean, "yellow_tripdata_2023-01.parquet", KEYS)
    row = fact.iloc[0]

    assert list(fact.columns) == transform.FACT_COLUMNS
    assert row["pickup_date_key"] == 20230115
    assert row["pickup_time_key"] == 830
    assert row["dropoff_time_key"] == 850
    assert row["trip_duration_minutes"] == 20.0
    assert bool(row["store_and_fwd_flag"]) is False
    assert row["source_file"] == "yellow_tripdata_2023-01.parquet"


def test_unknown_codes_map_to_unknown_members():
    trip = make_trip(pu_location_id=999, vendor_id=float("nan"), rate_code_id=float("nan"), payment_type=42)
    clean, _, _ = transform.clean_trips(frame(trip), MONTH, "f")
    row = transform.build_fact_frame(clean, "f", KEYS).iloc[0]

    assert row["pickup_location_key"] == transform.UNKNOWN_LOCATION
    assert row["vendor_key"] == transform.UNKNOWN_VENDOR
    assert row["rate_code_key"] == transform.UNKNOWN_RATE_CODE
    assert row["payment_type_key"] == transform.UNKNOWN_PAYMENT_TYPE
