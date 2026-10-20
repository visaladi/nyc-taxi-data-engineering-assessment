import json
from datetime import datetime, timezone

import pytest

from streaming.events import InvalidEventError, TripEvent, make_event_id


def make_event(**overrides) -> TripEvent:
    fields = dict(
        event_id=make_event_id("yellow_tripdata_2023-01.parquet", 42),
        event_time=datetime(2023, 1, 1, 10, 20, tzinfo=timezone.utc),
        pickup_time=datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
        vendor_id=2,
        pu_location_id=161,
        do_location_id=236,
        passenger_count=1,
        trip_distance=3.1,
        payment_type=1,
        fare_amount=17.0,
        tip_amount=3.0,
        total_amount=24.5,
    )
    fields.update(overrides)
    return TripEvent(**fields)


def test_round_trip_serialisation():
    event = make_event().stamped(datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert TripEvent.from_bytes(event.to_bytes()) == event


def test_event_id_is_deterministic():
    assert make_event_id("f", 1) == make_event_id("f", 1)
    assert make_event_id("f", 1) != make_event_id("f", 2)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not json",
        b"[]",
        json.dumps({"schema_version": 99}).encode(),
    ],
)
def test_malformed_payloads_are_rejected(payload):
    with pytest.raises(InvalidEventError):
        TripEvent.from_bytes(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "nope"),
        ("event_time", "yesterday"),
        ("fare_amount", "abc"),
        ("total_amount", None),
        ("vendor_id", "x"),
    ],
)
def test_invalid_fields_are_rejected(field, value):
    body = json.loads(make_event().to_bytes())
    body[field] = value

    with pytest.raises(InvalidEventError):
        TripEvent.from_bytes(json.dumps(body).encode())


def test_dropoff_before_pickup_is_rejected():
    event = make_event(event_time=datetime(2023, 1, 1, 9, 0, tzinfo=timezone.utc))

    with pytest.raises(InvalidEventError):
        TripEvent.from_bytes(event.to_bytes())
