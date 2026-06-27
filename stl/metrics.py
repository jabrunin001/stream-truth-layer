class PerShowMetrics:
    def __init__(self):
        self._m: dict[int, dict] = {}

    def record(self, show_id: int, outcome: str, event_time: float, watermark: float) -> None:
        s = self._m.setdefault(show_id, {
            "events": 0, "late_allowed": 0, "dropped": 0,
            "max_event_time": float("-inf"), "watermark": float("-inf"),
        })
        s["events"] += 1
        if outcome == "late_allowed":
            s["late_allowed"] += 1
        elif outcome == "dropped":
            s["dropped"] += 1
        s["max_event_time"] = max(s["max_event_time"], event_time)
        s["watermark"] = watermark

    def snapshot(self) -> dict[int, dict]:
        out = {}
        for show_id, s in self._m.items():
            out[show_id] = {**s, "watermark_lag": s["max_event_time"] - s["watermark"]}
        return out
