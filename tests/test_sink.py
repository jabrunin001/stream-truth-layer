import duckdb
from stl.pipelines.common import WindowResult
from stl.sink import IdempotentSink, NaiveAppendSink


def _r(show, start, amt, winner="Cy"):
    return WindowResult(show, start, start + 10, winner, amt, False)


def test_idempotent_sink_dedups_on_replay():
    sink = IdempotentSink()
    sink.write(_r(42, 0, 15000))
    sink.write(_r(42, 0, 15000))   # duplicate (replay)
    assert sink.gmv_by_show() == {42: 15000}


def test_naive_append_sink_double_counts_on_replay():
    sink = NaiveAppendSink()
    sink.write(_r(42, 0, 15000))
    sink.write(_r(42, 0, 15000))
    assert sink.gmv_by_show() == {42: 30000}


def test_materialize_writes_duckdb_table(tmp_path):
    sink = IdempotentSink()
    sink.write(_r(42, 0, 15000))
    sink.write(_r(7, 0, 500, winner="Di"))
    db = str(tmp_path / "serving.duckdb")
    sink.materialize(db)
    rows = dict(duckdb.connect(db).execute(
        "select show_id, gmv_cents from show_gmv order by show_id").fetchall())
    assert rows == {7: 500, 42: 15000}
