from stl.metrics import PerShowMetrics


def test_metrics_counts_outcomes_and_lag():
    m = PerShowMetrics()
    m.record(99, "on_time", event_time=6.0, watermark=1.0)
    m.record(99, "dropped", event_time=6.0, watermark=17.0)
    snap = m.snapshot()[99]
    assert snap["events"] == 2
    assert snap["dropped"] == 1
    assert snap["max_event_time"] == 6.0
    assert snap["watermark"] == 17.0
    assert snap["watermark_lag"] == 6.0 - 17.0


def test_late_allowed_outcome_is_counted():
    m = PerShowMetrics()
    m.record(7, "on_time", event_time=1.0, watermark=-4.0)
    m.record(7, "late_allowed", event_time=5.0, watermark=2.0)
    snap = m.snapshot()[7]
    assert snap["events"] == 2
    assert snap["late_allowed"] == 1
    assert snap["dropped"] == 0


def test_metrics_isolated_per_show():
    m = PerShowMetrics()
    m.record(7, "on_time", event_time=1.0, watermark=0.0)
    m.record(42, "dropped", event_time=4.0, watermark=9.0)
    snap = m.snapshot()
    assert snap[7]["events"] == 1 and snap[7]["dropped"] == 0
    assert snap[42]["events"] == 1 and snap[42]["dropped"] == 1
