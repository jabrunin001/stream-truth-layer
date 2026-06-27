from stl.state import KeyedStateBackend


def test_keyed_state_isolated_per_key():
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000, "winner": "Cy"})
    s.put("7|0", {"winning_bid_cents": 500, "winner": "Di"})
    assert s.get("42|0")["winner"] == "Cy"
    assert s.get("7|0")["winner"] == "Di"
    assert s.get("99|0") is None


def test_snapshot_is_deep_copy():
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000})
    snap = s.snapshot()
    s.put("42|0", {"winning_bid_cents": 99999})  # mutate after snapshot
    s2 = KeyedStateBackend()
    s2.restore(snap)
    assert s2.get("42|0")["winning_bid_cents"] == 15000
