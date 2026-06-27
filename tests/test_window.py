from stl.event import Event
from stl.state import KeyedStateBackend
from stl.window import EventTimeTumblingWindowOperator


def _bid(show, t, amt, bidder):
    return Event(show_id=show, type="bid", bidder_id=bidder,
                 amount_cents=amt, event_time=t, ingest_time=t)


def test_assign_buckets_by_event_time():
    op = EventTimeTumblingWindowOperator(size=10, allowed_lateness=5,
                                         state=KeyedStateBackend())
    assert op.assign(4.0) == (0.0, 10.0)
    assert op.assign(11.0) == (10.0, 20.0)


def test_highest_bid_wins_window():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    for ev, wm in [(_bid(42, 0, 10000, "Ana"), -5),
                   (_bid(42, 2, 12000, "Bo"), -3),
                   (_bid(42, 4, 15000, "Cy"), 0),
                   (_bid(42, 5, 14000, "Bo"), 0)]:
        assert op.process(ev, wm) in ("on_time", "late_allowed")
    [res] = op.results()
    assert res.winner == "Cy"
    assert res.winning_bid_cents == 15000


def test_event_beyond_allowed_lateness_is_dropped():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    # window [0,10): dropped once watermark >= 10 + 5 = 15
    assert op.process(_bid(99, 6, 9000, "Eve"), watermark=17.0) == "dropped"
    assert op.results() == []  # nothing accumulated
