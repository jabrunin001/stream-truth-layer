from stl.event import Event


class EventSource:
    def __init__(self, events: list[Event]):
        self._events = sorted(events, key=lambda e: e.ingest_time)
        self._offset = 0

    def poll(self) -> Event | None:
        if self._offset >= len(self._events):
            return None
        e = self._events[self._offset]
        self._offset += 1
        return e

    @property
    def offset(self) -> int:
        return self._offset

    def seek(self, offset: int) -> None:
        self._offset = offset

    def __iter__(self):
        while (e := self.poll()) is not None:
            yield e
