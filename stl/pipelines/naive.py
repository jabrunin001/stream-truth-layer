from stl.state import KeyedStateBackend
from stl.window import EventTimeTumblingWindowOperator
from stl.source import EventSource
from stl.pipelines.common import SIZE, to_table


def run_naive(events) -> dict:
    op = EventTimeTumblingWindowOperator(SIZE, allowed_lateness=float("inf"),
                                         state=KeyedStateBackend())
    for e in EventSource(events):  # arrival order
        # naive: bucket by processing/ingest time, no watermark, no lateness
        proc = e.model_copy(update={"event_time": e.ingest_time})
        op.process(proc, watermark=float("-inf"))
    return to_table(op.results())
