from stl.seeds import load_events
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time


def reconcile(inject_late: bool = True) -> dict:
    events = load_events(inject_late=inject_late)
    batch = run_batch(events)
    naive = run_naive(events)
    event_time, metrics, _sink = run_event_time(events)
    msnap = metrics.snapshot()
    dropped_shows = {sid for sid, m in msnap.items() if m["dropped"] > 0}

    # Parity excludes shows that intentionally dropped beyond-lateness data:
    # those legitimately diverge from the all-inclusive batch oracle.
    shared = {k for k in (set(event_time) & set(batch))
              if event_time[k]["show_id"] not in dropped_shows}
    matches = all(event_time[k]["winner"] == batch[k]["winner"]
                  and event_time[k]["winning_bid_cents"] == batch[k]["winning_bid_cents"]
                  for k in shared)

    divergences = []
    for k in set(naive) & set(batch):
        if naive[k]["winner"] != batch[k]["winner"]:
            divergences.append({
                "key": k, "naive_winner": naive[k]["winner"],
                "naive_bid_cents": naive[k]["winning_bid_cents"],
                "oracle_winner": batch[k]["winner"],
                "oracle_bid_cents": batch[k]["winning_bid_cents"],
            })

    late_drops = [{"show_id": sid, "dropped": msnap[sid]["dropped"]}
                  for sid in sorted(dropped_shows)]
    return {"event_time": event_time, "naive": naive, "batch": batch,
            "matches_oracle": matches, "naive_divergences": divergences,
            "late_drops": late_drops, "metrics": msnap}
