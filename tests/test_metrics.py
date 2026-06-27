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
