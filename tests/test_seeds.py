from stl.seeds import load_events


def test_clean_vs_late_differ_only_in_cy_ingest():
    clean = {(e.show_id, e.bidder_id, e.event_time): e.ingest_time
             for e in load_events(inject_late=False)}
    late = {(e.show_id, e.bidder_id, e.event_time): e.ingest_time
            for e in load_events(inject_late=True)}
    diffs = {k for k in clean if clean[k] != late[k]}
    assert diffs == {(42, "Cy", 4.0)}
    assert late[(42, "Cy", 4.0)] == 11.0


def test_load_returns_events():
    evs = load_events()
    assert len(evs) == 9
    assert {e.show_id for e in evs} == {42, 7, 99}
