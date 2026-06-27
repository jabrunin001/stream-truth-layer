from collections import defaultdict
from stl.state import KeyedStateBackend
from stl.watermark import BoundedOutOfOrdernessWatermark
from stl.window import EventTimeTumblingWindowOperator
from stl.metrics import PerShowMetrics
from stl.sink import IdempotentSink
from stl.source import EventSource
from stl.pipelines.common import SIZE, ALLOWED_LATENESS, MAX_LATENESS, to_table


def run_event_time(events):
    state = KeyedStateBackend()
    op = EventTimeTumblingWindowOperator(SIZE, ALLOWED_LATENESS, state)
    metrics = PerShowMetrics()
    watermarks = defaultdict(lambda: BoundedOutOfOrdernessWatermark(MAX_LATENESS))
    for e in EventSource(events):  # arrival order
        wm = watermarks[e.show_id]
        wm.observe(e.event_time)
        outcome = op.process(e, watermark=wm.current)
        metrics.record(e.show_id, outcome, e.event_time, wm.current)
    sink = IdempotentSink()
    for r in op.results():
        sink.write(r)
    table = to_table(op.results())
    return table, metrics, sink
