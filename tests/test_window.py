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


def test_late_event_within_allowed_lateness_is_late_allowed():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    # window [0,10); watermark=11 >= end=10 but < end+lateness=15 -> late_allowed
    assert op.process(_bid(7, 5, 8000, "Dee"), watermark=11.0) == "late_allowed"
    [res] = op.results()
    assert res.winning_bid_cents == 8000


def test_drop_boundary_is_inclusive_at_end_plus_lateness():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    # window [0,10); end+lateness=15; watermark exactly 15 -> dropped (>= boundary)
    assert op.process(_bid(7, 5, 8000, "Dee"), watermark=15.0) == "dropped"
    assert op.results() == []


def test_non_bid_events_do_not_change_winner():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    op.process(_bid(7, 1, 5000, "Ana"), watermark=-5)
    view = Event(show_id=7, type="view", bidder_id="Bo", amount_cents=99999,
                 event_time=2, ingest_time=2)
    op.process(view, watermark=-5)
    [res] = op.results()
    assert res.winner == "Ana"
    assert res.winning_bid_cents == 5000
