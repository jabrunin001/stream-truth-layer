from stl.pipelines.reconcile import reconcile
from stl.pipelines.common import window_key

K42 = window_key(42, 0.0)


def test_hero_event_time_matches_oracle_naive_does_not():
    r = reconcile(inject_late=True)
    assert r["matches_oracle"] is True
    assert r["event_time"][K42]["winner"] == "Cy"
    assert r["event_time"][K42]["winning_bid_cents"] == 15000
    assert r["naive"][K42]["winner"] == "Bo"
    assert r["naive"][K42]["winning_bid_cents"] == 14000
    div = {d["key"]: d for d in r["naive_divergences"]}
    assert K42 in div
    assert div[K42]["naive_winner"] == "Bo"
    assert div[K42]["oracle_winner"] == "Cy"
    # show 99 dropped a beyond-lateness bid -> reported as an SLO drop, not a parity failure
    assert any(d["show_id"] == 99 for d in r["late_drops"])
    # guard: Show #42 must NOT be excluded from parity (else matches_oracle is vacuously True)
    assert not any(d["show_id"] == 42 for d in r["late_drops"])
