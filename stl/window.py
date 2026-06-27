from stl.event import Event, EventType
from stl.state import KeyedStateBackend
from stl.pipelines.common import WindowResult, window_key


class EventTimeTumblingWindowOperator:
    def __init__(self, size: float, allowed_lateness: float, state: KeyedStateBackend):
        self.size = size
        self.allowed_lateness = allowed_lateness
        self.state = state

    def assign(self, event_time: float) -> tuple[float, float]:
        start = (event_time // self.size) * self.size
        return (start, start + self.size)

    def process(self, event: Event, watermark: float) -> str:
        start, end = self.assign(event.event_time)
        if watermark >= end + self.allowed_lateness:
            return "dropped"
        key = window_key(event.show_id, start)
        acc = self.state.get(key) or {
            "show_id": event.show_id, "window_start": start, "window_end": end,
            "winning_bid_cents": -1, "winner": None,
        }
        outcome = "late_allowed" if watermark >= end else "on_time"
        if event.type is EventType.BID and event.amount_cents > acc["winning_bid_cents"]:
            acc["winning_bid_cents"] = event.amount_cents
            acc["winner"] = event.bidder_id
        self.state.put(key, acc)
        return outcome

    def results(self) -> list[WindowResult]:
        out = []
        for key in self.state.keys():
            a = self.state.get(key)
            out.append(WindowResult(
                show_id=a["show_id"], window_start=a["window_start"],
                window_end=a["window_end"], winner=a["winner"],
                winning_bid_cents=max(a["winning_bid_cents"], 0),
            ))
        return sorted(out, key=lambda r: (r.show_id, r.window_start))
