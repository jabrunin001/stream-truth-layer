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


def test_snapshot_deep_copy_value_mutation():
    """Shallow copy of state dict would fail this: mutate the inner dict in-place."""
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000})
    snap = s.snapshot()
    s.get("42|0")["winning_bid_cents"] = 99999  # in-place mutation
    s2 = KeyedStateBackend()
    s2.restore(snap)
    assert s2.get("42|0")["winning_bid_cents"] == 15000


def test_restore_deep_copy_isolation():
    """Mutating restored state must not corrupt the snapshot dict."""
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000})
    snap = s.snapshot()
    s2 = KeyedStateBackend()
    s2.restore(snap)
    s2.get("42|0")["winning_bid_cents"] = 0  # in-place mutation of restored state
    assert snap["42|0"]["winning_bid_cents"] == 15000
