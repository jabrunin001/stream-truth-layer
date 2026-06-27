import duckdb
from stl.pipelines.common import WindowResult, window_key


class IdempotentSink:
    def __init__(self):
        self._rows: dict[str, WindowResult] = {}

    def write(self, r: WindowResult) -> None:
        self._rows[window_key(r.show_id, r.window_start)] = r

    def gmv_by_show(self) -> dict[int, int]:
        out: dict[int, int] = {}
        for r in self._rows.values():
            out[r.show_id] = out.get(r.show_id, 0) + r.winning_bid_cents
        return out

    def materialize(self, path: str) -> None:
        con = duckdb.connect(path)
        con.execute("create or replace table show_gmv (show_id INTEGER, gmv_cents BIGINT)")
        for show_id, gmv in sorted(self.gmv_by_show().items()):
            con.execute("insert into show_gmv values (?, ?)", [show_id, gmv])
        con.close()


class NaiveAppendSink(IdempotentSink):
    def __init__(self):
        self._appended: list[WindowResult] = []

    def write(self, r: WindowResult) -> None:
        self._appended.append(r)

    def gmv_by_show(self) -> dict[int, int]:
        out: dict[int, int] = {}
        for r in self._appended:
            out[r.show_id] = out.get(r.show_id, 0) + r.winning_bid_cents
        return out
