from stl.state import KeyedStateBackend
from stl.window import EventTimeTumblingWindowOperator
from stl.pipelines.common import SIZE, to_table


def run_batch(events) -> dict:
    op = EventTimeTumblingWindowOperator(SIZE, allowed_lateness=float("inf"),
                                         state=KeyedStateBackend())
    for e in sorted(events, key=lambda e: e.event_time):
        op.process(e, watermark=float("-inf"))  # never drops; pure event-time buckets
    return to_table(op.results())
