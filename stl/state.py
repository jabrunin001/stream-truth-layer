import copy


class KeyedStateBackend:
    def __init__(self):
        self._state: dict[str, dict] = {}

    def get(self, key: str, default=None):
        return self._state.get(key, default)

    def put(self, key: str, value: dict) -> None:
        self._state[key] = value

    def delete(self, key: str) -> None:
        self._state.pop(key, None)

    def keys(self) -> list[str]:
        return list(self._state.keys())

    def snapshot(self) -> dict:
        return copy.deepcopy(self._state)

    def restore(self, state: dict) -> None:
        self._state = copy.deepcopy(state)
