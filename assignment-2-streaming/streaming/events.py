"""The trip event contract shared by producer and processor (JSON, schema version 1)."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1
# Namespace for deterministic event ids: the same source row always gets the same id,
# which lets the sink de-duplicate producer retries and replays.
EVENT_ID_NAMESPACE = uuid.UUID("3b8f6f0e-2a7d-4d0c-9a49-0c3f2f6d9e15")


class InvalidEventError(ValueError):
    """The message cannot be parsed into a valid TripEvent (routed to the DLQ)."""


def make_event_id(source: str, row_number: int) -> str:
    return str(uuid.uuid5(EVENT_ID_NAMESPACE, f"{source}:{row_number}"))


def _parse_ts(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise InvalidEventError(f"{name} must be an ISO-8601 string")
    try:
        ts = datetime.fromisoformat(value)
    except ValueError as exc:
        raise InvalidEventError(f"{name} is not ISO-8601: {value!r}") from exc
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def _opt_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _num(value: Any, name: str) -> float:
    if value is None:
        raise InvalidEventError(f"{name} is required")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidEventError(f"{name} is not numeric: {value!r}") from exc


@dataclass(frozen=True)
class TripEvent:
    event_id: str
    event_time: datetime          # drop-off time: the moment the trip "happened"
    pickup_time: datetime
    vendor_id: int | None
    pu_location_id: int | None
    do_location_id: int | None
    passenger_count: int | None
    trip_distance: float
    payment_type: int | None
    fare_amount: float
    tip_amount: float
    total_amount: float
    produced_at: datetime | None = None

    @property
    def event_ts(self) -> int:
        """Event time as epoch seconds (what the windowing logic works with)."""
        return int(self.event_time.timestamp())

    def stamped(self, produced_at: datetime) -> TripEvent:
        return replace(self, produced_at=produced_at)

    def to_bytes(self) -> bytes:
        body = asdict(self)
        for key in ("event_time", "pickup_time", "produced_at"):
            if body[key] is not None:
                body[key] = body[key].isoformat()
        body["schema_version"] = SCHEMA_VERSION
        return json.dumps(body, separators=(",", ":")).encode()

    @classmethod
    def from_bytes(cls, raw: bytes | None) -> TripEvent:
        if not raw:
            raise InvalidEventError("empty message")
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidEventError(f"not valid JSON: {exc}") from exc
        if not isinstance(body, dict):
            raise InvalidEventError("payload must be a JSON object")
        if body.get("schema_version") != SCHEMA_VERSION:
            raise InvalidEventError(f"unsupported schema_version {body.get('schema_version')!r}")

        try:
            event_id = str(uuid.UUID(str(body.get("event_id"))))
        except ValueError as exc:
            raise InvalidEventError("event_id must be a UUID") from exc

        try:
            event = cls(
                event_id=event_id,
                event_time=_parse_ts(body.get("event_time"), "event_time"),
                pickup_time=_parse_ts(body.get("pickup_time"), "pickup_time"),
                vendor_id=_opt_int(body.get("vendor_id")),
                pu_location_id=_opt_int(body.get("pu_location_id")),
                do_location_id=_opt_int(body.get("do_location_id")),
                passenger_count=_opt_int(body.get("passenger_count")),
                trip_distance=_num(body.get("trip_distance"), "trip_distance"),
                payment_type=_opt_int(body.get("payment_type")),
                fare_amount=_num(body.get("fare_amount"), "fare_amount"),
                tip_amount=_num(body.get("tip_amount", 0), "tip_amount"),
                total_amount=_num(body.get("total_amount"), "total_amount"),
                produced_at=_parse_ts(body["produced_at"], "produced_at") if body.get("produced_at") else None,
            )
        except InvalidEventError:
            raise
        except (TypeError, ValueError) as exc:  # e.g. int("abc") in an optional field
            raise InvalidEventError(f"invalid field value: {exc}") from exc

        if event.event_time < event.pickup_time:
            raise InvalidEventError("event_time (drop-off) is before pickup_time")
        if event.trip_distance < 0:
            raise InvalidEventError("trip_distance is negative")
        return event
