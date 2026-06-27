class BoundedOutOfOrdernessWatermark:
    def __init__(self, max_lateness: float):
        self.max_lateness = max_lateness
        self._max_event_time = float("-inf")

    def observe(self, event_time: float) -> None:
        if event_time > self._max_event_time:
            self._max_event_time = event_time

    @property
    def current(self) -> float:
        if self._max_event_time == float("-inf"):
            return float("-inf")
        return self._max_event_time - self.max_lateness

    def snapshot(self) -> dict:
        return {"max_event_time": self._max_event_time}

    def restore(self, state: dict) -> None:
        self._max_event_time = state["max_event_time"]
