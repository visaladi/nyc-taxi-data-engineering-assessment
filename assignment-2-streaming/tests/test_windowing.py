from dataclasses import dataclass

import pytest

from streaming.windowing import (
    ALL_BOROUGHS,
    SlidingWindowSpec,
    WatermarkTracker,
    Window,
    aggregate,
)

MIN = 60
SPEC = SlidingWindowSpec(size_s=15 * MIN, slide_s=5 * MIN)


@dataclass
class FakeEvent:
    event_ts: int
    borough: str = "Manhattan"
    passenger_count: int | None = 1
    trip_distance: float = 2.0
    fare_amount: float = 10.0
    tip_amount: float = 2.0
    total_amount: float = 15.0


def test_event_belongs_to_size_over_slide_windows():
    ts = 10 * MIN + 7 * 60  # 00:17 relative to epoch -> windows starting 00:15, 00:10, 00:05
    windows = SPEC.windows_for(ts)

    assert windows == [Window(15 * MIN, 30 * MIN), Window(10 * MIN, 25 * MIN), Window(5 * MIN, 20 * MIN)]
    assert all(w.start <= ts < w.end for w in windows)


def test_window_boundaries_are_start_inclusive_end_exclusive():
    windows = SPEC.windows_for(15 * MIN)

    assert Window(15 * MIN, 30 * MIN) in windows
    assert Window(0, 15 * MIN) not in windows


def test_spec_rejects_size_not_multiple_of_slide():
    with pytest.raises(ValueError):
        SlidingWindowSpec(size_s=10 * MIN, slide_s=3 * MIN)


def test_watermark_is_min_over_partitions_minus_lateness():
    tracker = WatermarkTracker(allowed_lateness_s=100)
    tracker.observe(0, 1_000)
    tracker.observe(1, 5_000)

    assert tracker.watermark == 900  # held back by the slowest partition


def test_out_of_order_event_does_not_move_watermark_backwards():
    tracker = WatermarkTracker(allowed_lateness_s=0)
    tracker.observe(0, 2_000)
    tracker.observe(0, 1_500)

    assert tracker.watermark == 2_000


def test_clock_outlier_is_ignored_for_watermark():
    tracker = WatermarkTracker(allowed_lateness_s=0, max_forward_jump_s=3_600)
    tracker.observe(0, 10_000)

    assert tracker.observe(0, 10_000 + 86_400) is False
    assert tracker.watermark == 10_000


def test_forget_removes_revoked_partitions():
    tracker = WatermarkTracker(allowed_lateness_s=0)
    tracker.observe(0, 100)
    tracker.observe(1, 900)
    tracker.forget([0])

    assert tracker.watermark == 900


def test_late_event_only_updates_still_open_windows():
    ts = 17 * MIN
    watermark = 25 * MIN  # closes windows ending at 20 and 25 minutes

    assert SPEC.open_windows_for(ts, watermark) == [Window(15 * MIN, 30 * MIN)]
    assert not SPEC.is_late(ts, watermark)
    assert SPEC.is_late(ts, 30 * MIN)


def test_aggregate_adds_borough_and_city_wide_rows():
    events = [FakeEvent(17 * MIN), FakeEvent(18 * MIN, borough="Queens", passenger_count=None)]
    deltas = aggregate(events, SPEC, watermark=None)

    window = Window(15 * MIN, 30 * MIN)
    assert deltas[(window, "Manhattan")].trip_count == 1
    assert deltas[(window, "Queens")].passenger_count == 0
    city = deltas[(window, ALL_BOROUGHS)]
    assert city.trip_count == 2
    assert city.total_revenue == 30.0
    assert len(deltas) == 3 * 3  # 3 windows x (Manhattan, Queens, ALL)
