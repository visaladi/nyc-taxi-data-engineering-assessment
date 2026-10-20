"""Event-time sliding windows and watermarks (pure logic, no I/O - unit tested).

Terminology
-----------
* Sliding window: size S, slide L (S is a multiple of L). Windows start every L seconds
  (epoch aligned) and each event belongs to S / L overlapping windows.
  e.g. S=15 min, L=5 min -> an event at 10:07 is in [09:55,10:10), [10:00,10:15), [10:05,10:20).
* Watermark: "no more events older than W are expected". Computed per Kafka partition as
  max(event time seen) - allowed lateness; the consumer's watermark is the minimum across its
  partitions, so one slow partition cannot cause another partition's events to be dropped.
* A window [start, end) is closed (final) once watermark >= end. An event whose windows are
  all closed is "late": it is still stored raw (flagged) but no longer changes aggregates.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, order=True)
class Window:
    start: int  # epoch seconds, inclusive
    end: int    # epoch seconds, exclusive


@dataclass(frozen=True)
class SlidingWindowSpec:
    size_s: int
    slide_s: int

    def __post_init__(self) -> None:
        if self.size_s <= 0 or self.slide_s <= 0:
            raise ValueError("window size and slide must be positive")
        if self.size_s % self.slide_s:
            raise ValueError("window size must be a multiple of the slide")

    def windows_for(self, ts: int) -> list[Window]:
        """All windows containing `ts`, newest first."""
        last_start = ts - ts % self.slide_s
        return [Window(s, s + self.size_s) for s in range(last_start, ts - self.size_s, -self.slide_s)]

    def open_windows_for(self, ts: int, watermark: int | None) -> list[Window]:
        """Windows containing `ts` that the watermark has not closed yet."""
        windows = self.windows_for(ts)
        if watermark is None:
            return windows
        return [w for w in windows if w.end > watermark]

    def is_late(self, ts: int, watermark: int | None) -> bool:
        return not self.open_windows_for(ts, watermark)


@dataclass
class WatermarkTracker:
    allowed_lateness_s: int
    max_forward_jump_s: int = 3600
    _max_ts: dict[int, int] = field(default_factory=dict)

    def observe(self, partition: int, ts: int) -> bool:
        """Records an event time. Returns False when it is ignored as a clock outlier.

        A timestamp more than `max_forward_jump_s` ahead of what the partition has seen so far
        is almost certainly a device clock error; letting it advance the watermark would make
        every subsequent on-time event "late".
        """
        current = self._max_ts.get(partition)
        if current is None:
            self._max_ts[partition] = ts
            return True
        if ts > current + self.max_forward_jump_s:
            return False
        if ts > current:
            self._max_ts[partition] = ts
        return True

    def forget(self, partitions: Iterable[int]) -> None:
        """Drops state for partitions revoked in a rebalance (the new owner rebuilds it)."""
        for partition in partitions:
            self._max_ts.pop(partition, None)

    @property
    def watermark(self) -> int | None:
        if not self._max_ts:
            return None
        return min(self._max_ts.values()) - self.allowed_lateness_s


class AggregatableEvent(Protocol):
    @property
    def event_ts(self) -> int: ...
    @property
    def borough(self) -> str: ...
    @property
    def passenger_count(self) -> int | None: ...
    @property
    def trip_distance(self) -> float: ...
    @property
    def fare_amount(self) -> float: ...
    @property
    def tip_amount(self) -> float: ...
    @property
    def total_amount(self) -> float: ...


@dataclass
class WindowDelta:
    """Additive measures for one (window, borough) produced by one micro-batch."""

    trip_count: int = 0
    passenger_count: int = 0
    total_distance: float = 0.0
    total_fare: float = 0.0
    total_tip: float = 0.0
    total_revenue: float = 0.0

    def add(self, event: AggregatableEvent) -> None:
        self.trip_count += 1
        self.passenger_count += event.passenger_count or 0
        self.total_distance += event.trip_distance
        self.total_fare += event.fare_amount
        self.total_tip += event.tip_amount
        self.total_revenue += event.total_amount


ALL_BOROUGHS = "ALL"


def aggregate(
    events: Iterable[AggregatableEvent], spec: SlidingWindowSpec, watermark: int | None
) -> dict[tuple[Window, str], WindowDelta]:
    """Folds events into per-(window, borough) deltas, plus a city-wide 'ALL' row per window.

    Only windows still open under `watermark` receive the event.
    """
    deltas: dict[tuple[Window, str], WindowDelta] = {}
    for event in events:
        for window in spec.open_windows_for(event.event_ts, watermark):
            for borough in (event.borough, ALL_BOROUGHS):
                deltas.setdefault((window, borough), WindowDelta()).add(event)
    return deltas
