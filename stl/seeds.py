import json
from importlib import resources
from stl.event import Event


def load_events(inject_late: bool = False) -> list[Event]:
    name = "shows_late.jsonl" if inject_late else "shows.jsonl"
    text = resources.files("stl.data").joinpath(name).read_text()
    return [Event(**json.loads(line)) for line in text.splitlines() if line.strip()]
