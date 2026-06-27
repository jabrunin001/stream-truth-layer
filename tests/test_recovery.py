from stl.seeds import load_events
from stl.pipelines.recovery import run_with_crash


def test_exactly_once_recovery_matches_clean_run():
    r = run_with_crash(load_events(inject_late=True), crash_at=3)
    assert r["exactly_once"] is True
    assert r["recovered_gmv"] == r["clean_gmv"]
    assert r["recovered_gmv"] != r["double_count_gmv"]


def test_recovery_resumes_from_checkpoint_offset_not_from_zero():
    events = load_events(inject_late=True)
    r = run_with_crash(events, crash_at=3)
    # If seek() were broken (resume from 0), replayed_count would equal len(events) and fail.
    assert r["replayed_count"] == r["total_events"] - 3
    assert r["replayed_count"] < r["total_events"]
