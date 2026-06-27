from stl.seeds import load_events
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time
from stl.pipelines.common import window_key

K42 = window_key(42, 0.0)


def test_batch_oracle_show42_winner_is_cy():
    t = run_batch(load_events(inject_late=True))
    assert t[K42]["winner"] == "Cy"
    assert t[K42]["winning_bid_cents"] == 15000


def test_naive_crowns_bo_under_late_arrival():
    t = run_naive(load_events(inject_late=True))
    assert t[K42]["winner"] == "Bo"
    assert t[K42]["winning_bid_cents"] == 14000


def test_event_time_matches_oracle():
    t, metrics, sink = run_event_time(load_events(inject_late=True))
    assert t[K42]["winner"] == "Cy"
    assert t[K42]["winning_bid_cents"] == 15000
    # show 99 straggler (event_time 6, arrives at 24) is dropped beyond allowed lateness
    assert metrics.snapshot()[99]["dropped"] == 1
