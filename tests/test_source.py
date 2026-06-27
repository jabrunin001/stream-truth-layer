from stl.event import Event
from stl.source import EventSource


def _ev(show, t_event, t_ingest, amt=0, bidder=None, type="bid"):
    return Event(show_id=show, type=type, bidder_id=bidder,
                 amount_cents=amt, event_time=t_event, ingest_time=t_ingest)


def test_source_replays_in_arrival_order():
    src = EventSource([_ev(1, 4, 11), _ev(1, 0, 0.5), _ev(1, 2, 2.5)])
    seen = [e.event_time for e in src]
    assert seen == [0, 2, 4]  # sorted by ingest_time, not event_time


def test_source_offset_and_seek():
    src = EventSource([_ev(1, 0, 0.5), _ev(1, 2, 2.5), _ev(1, 4, 11)])
    src.poll(); src.poll()
    assert src.offset == 2
    src.seek(1)
    assert src.poll().event_time == 2
