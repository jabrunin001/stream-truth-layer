from stl.event import Event, EventType


def test_event_parses_and_defaults():
    e = Event(show_id=42, type="bid", bidder_id="Cy", amount_cents=15000,
              event_time=4.0, ingest_time=11.0)
    assert e.type is EventType.BID
    assert e.amount_cents == 15000


def test_view_event_has_zero_amount_default():
    e = Event(show_id=7, type="view", event_time=1.0, ingest_time=1.0)
    assert e.amount_cents == 0
    assert e.bidder_id is None
