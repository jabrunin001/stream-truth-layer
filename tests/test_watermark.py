from stl.watermark import BoundedOutOfOrdernessWatermark


def test_watermark_trails_max_event_time_by_lateness():
    wm = BoundedOutOfOrdernessWatermark(max_lateness=5.0)
    assert wm.current == float("-inf")
    wm.observe(2.0)
    assert wm.current == -3.0
    wm.observe(22.0)
    assert wm.current == 17.0
    wm.observe(4.0)            # out-of-order: does not move watermark back
    assert wm.current == 17.0


def test_watermark_snapshot_restore_roundtrip():
    wm = BoundedOutOfOrdernessWatermark(5.0)
    wm.observe(22.0)
    snap = wm.snapshot()
    wm2 = BoundedOutOfOrdernessWatermark(5.0)
    wm2.restore(snap)
    assert wm2.current == 17.0
