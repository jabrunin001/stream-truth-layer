from collections import defaultdict
from stl.state import KeyedStateBackend
from stl.watermark import BoundedOutOfOrdernessWatermark
from stl.window import EventTimeTumblingWindowOperator
from stl.sink import IdempotentSink, NaiveAppendSink
from stl.source import EventSource
from stl import checkpoint as ckpt_mod
from stl.pipelines.common import SIZE, ALLOWED_LATENESS, MAX_LATENESS
from stl.pipelines.event_time import run_event_time


def _wm():
    return BoundedOutOfOrdernessWatermark(MAX_LATENESS)


def _drain(source, op, watermarks):
    for e in source:
        wm = watermarks[e.show_id]
        wm.observe(e.event_time)
        op.process(e, watermark=wm.current)


def run_with_crash(events, crash_at: int) -> dict:
    # clean reference
    _t, _m, clean_sink = run_event_time(events)
    clean_gmv = clean_sink.gmv_by_show()

    # crash + restore
    state = KeyedStateBackend()
    op = EventTimeTumblingWindowOperator(SIZE, ALLOWED_LATENESS, state)
    watermarks = defaultdict(_wm)
    source = EventSource(events)
    for _ in range(crash_at):
        e = source.poll()
        if e is None:
            break
        wm = watermarks[e.show_id]
        wm.observe(e.event_time)
        op.process(e, watermark=wm.current)
    ckpt = ckpt_mod.take(state, source, dict(watermarks))  # checkpoint at crash_at
    # ---- simulate crash: drop op/state/watermarks, rebuild and restore ----
    state2 = KeyedStateBackend()
    op2 = EventTimeTumblingWindowOperator(SIZE, ALLOWED_LATENESS, state2)
    source2 = EventSource(events)
    restored_wms = defaultdict(_wm)
    restored_wms.update(ckpt_mod.restore_into(ckpt, state2, source2, _wm))
    replayed_count = 0
    for e in source2:  # replays only offset..end
        wm = restored_wms[e.show_id]
        wm.observe(e.event_time)
        op2.process(e, watermark=wm.current)
        replayed_count += 1
    recovered_sink = IdempotentSink()
    for r in op2.results():
        recovered_sink.write(r)
    recovered_gmv = recovered_sink.gmv_by_show()

    # demonstrate what at-least-once + non-idempotent sink would do: replay from 0 twice
    naive_sink = NaiveAppendSink()
    for _pass in range(2):
        s = KeyedStateBackend()
        o = EventTimeTumblingWindowOperator(SIZE, ALLOWED_LATENESS, s)
        wms = defaultdict(_wm)
        _drain(EventSource(events), o, wms)
        for r in o.results():
            naive_sink.write(r)
    double_count_gmv = naive_sink.gmv_by_show()

    return {
        "recovered_gmv": recovered_gmv, "clean_gmv": clean_gmv,
        "double_count_gmv": double_count_gmv,
        "exactly_once": recovered_gmv == clean_gmv != double_count_gmv,
        "replayed_count": replayed_count,
        "total_events": len(events),
    }
