# stream-truth-layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a pure-Python "mini-Flink" streaming engine for Whatnot live auctions whose hero control proves that event-time windowing with watermarks + allowed lateness reconciles to a batch oracle while a naive processing-time pipeline silently crowns the wrong auction winner.

**Architecture:** Small, single-purpose engine units (`event`, `source`, `watermark`, `state`, `window`, `sink`, `metrics`, `checkpoint`) composed into three pipelines (naive / event-time / batch-oracle). A `reconcile` driver asserts `event_time == batch` and `naive != batch`; a `recovery` driver proves exactly-once after a simulated crash. A Typer CLI exposes everything; CI asserts the exact numbers; a hand-built demo page ships on GitHub Pages.

**Tech Stack:** Python 3.11+, Pydantic v2, Typer, DuckDB (local serving sink), pytest, GitHub Actions. Optional **local** Ollama for `explain` (never cloud/keys).

## Global Constraints

- **Python 3.11+ only** (uses `X | None` unions, `match` not required). Pin nothing above 3.12 in CI.
- **Money is integer cents.** `amount_cents: int`. Never use floats for money.
- **Per-show watermarks.** Each `show_id` (tenant) has its own watermark generator — isolated tenant state. (Document this simplification vs Flink's per-subtask watermark in the ADR.)
- **Clone-and-run-free.** No network, no cloud, no API keys, no Java. `pip install -e .` then run.
- **Seed variants by flag.** Default seed `stl/data/shows.jsonl` is never edited; the late-injected scenario lives in `stl/data/shows_late.jsonl`, selected by `--inject-late`.
- **Hero numbers (CI contract):** Show #42 window `[0,10)` — naive winner = `Bo` @ `14000`¢; event-time & batch winner = `Cy` @ `15000`¢.
- **Stage explicit paths in commits** (never `git add -A`; the Desktop is iCloud-synced).
- **Co-author trailer on every commit:** `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

```
stl/
  event.py        # Event Pydantic model + EventType enum
  source.py       # EventSource: arrival-ordered replay with resumable offset
  watermark.py    # BoundedOutOfOrdernessWatermark
  state.py        # KeyedStateBackend (string keys, snapshot/restore)
  window.py       # EventTimeTumblingWindowOperator + WindowResult
  sink.py         # IdempotentSink + DuckDB serving table
  metrics.py      # PerShowMetrics (SLO counters)
  checkpoint.py   # Checkpoint snapshot/restore of (state, offset, watermarks)
  seeds.py        # load_events(inject_late) -> list[Event]
  explain.py      # deterministic narration + optional local Ollama
  cli.py          # Typer app `stl`
  pipelines/
    common.py     # result_table(), window_key(), ResultTable type alias
    naive.py      # processing-time windowing (the bug)
    event_time.py # watermark + allowed lateness + idempotent sink (the platform)
    batch.py      # sort-by-event_time oracle
    reconcile.py  # run all three, diff, assert
    recovery.py   # crash-at-N + restore exactly-once driver
  data/
    shows.jsonl       # clean multi-show seed
    shows_late.jsonl  # late-injected seed (hero scenario)
tests/              # one test module per unit + reconcile/recovery/cli
demo/index.html     # hand-built dark-OLED landing page (do not regenerate)
docs/adr/0001-when-flink.md
docs/production-architecture.md
.github/workflows/{ci.yml,pages.yml}
pyproject.toml
README.md
```

---

### Task 1: Project scaffolding + `Event` model

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `stl/__init__.py`, `stl/event.py`, `tests/__init__.py`, `tests/test_event.py`

**Interfaces:**
- Produces: `EventType(str, Enum)` with `BID="bid"`, `SOLD="sold"`, `VIEW="view"`. `Event(BaseModel)` with fields `show_id:int`, `type:EventType`, `bidder_id:str|None=None`, `amount_cents:int=0`, `event_time:float`, `ingest_time:float`.

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "stream-truth-layer"
version = "0.1.0"
description = "A pure-Python mini-Flink streaming engine for Whatnot live auctions"
requires-python = ">=3.11"
dependencies = ["pydantic>=2.5", "typer>=0.12", "duckdb>=0.10"]

[project.optional-dependencies]
dev = ["pytest>=8.0"]

[project.scripts]
stl = "stl.cli:app"

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["stl*"]

[tool.setuptools.package-data]
stl = ["data/*.jsonl"]
```

- [ ] **Step 2: Write `.gitignore`**

```
__pycache__/
*.pyc
.venv/
*.duckdb
demo/observe.html
dist/
*.egg-info/
.pytest_cache/
```

- [ ] **Step 3: Write the failing test** in `tests/test_event.py`

```python
from stl.event import Event, EventType


def test_event_parses_and_defaults():
    e = Event(show_id=42, type="bid", bidder_id="Cy", amount_cents=15000,
              event_time=4.0, ingest_time=11.0)
    assert e.type is EventType.BID
    assert e.amount_cents == 15000


def test_view_event_has_zero_amount_default():
    e = Event(show_id=7, type="view", event_time=1.0, ingest_time=1.0)
    assert e.amount_cents == 0
    assert e.bidder_id is None
```

- [ ] **Step 4: Run test, verify it fails**

Run: `python -m pytest tests/test_event.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'stl.event'`

- [ ] **Step 5: Implement `stl/event.py`** (and empty `stl/__init__.py`, `tests/__init__.py`)

```python
from enum import Enum
from pydantic import BaseModel


class EventType(str, Enum):
    BID = "bid"
    SOLD = "sold"
    VIEW = "view"


class Event(BaseModel):
    show_id: int
    type: EventType
    bidder_id: str | None = None
    amount_cents: int = 0
    event_time: float
    ingest_time: float
```

- [ ] **Step 6: Install editable + run tests**

Run: `python -m venv .venv && .venv/bin/pip install -q -e ".[dev]" && .venv/bin/python -m pytest tests/test_event.py -v`
Expected: 2 passed. (Use `.venv/bin/python -m pytest` for all later runs.)

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .gitignore stl/__init__.py stl/event.py tests/__init__.py tests/test_event.py
git commit -m "feat: scaffold project + Event model

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `EventSource` — arrival-ordered replay with resumable offset

**Files:**
- Create: `stl/source.py`, `tests/test_source.py`

**Interfaces:**
- Consumes: `Event` from Task 1.
- Produces: `EventSource(events: list[Event])`. Sorts events by `ingest_time`. `poll() -> Event | None` (returns next, advances offset, `None` when exhausted). Property `offset:int`. `seek(offset:int) -> None`. `__iter__` yields remaining via `poll`.

- [ ] **Step 1: Write failing test** `tests/test_source.py`

```python
from stl.event import Event
from stl.source import EventSource


def _ev(show, t_event, t_ingest, amt=0, bidder=None, type="bid"):
    return Event(show_id=show, type=type, bidder_id=bidder,
                 amount_cents=amt, event_time=t_event, ingest_time=t_ingest)


def test_source_replays_in_arrival_order():
    src = EventSource([_ev(1, 4, 11), _ev(1, 0, 0.5), _ev(1, 2, 2.5)])
    seen = [e.event_time for e in src]
    assert seen == [0, 2, 4]  # sorted by ingest_time, not event_time


def test_source_offset_and_seek():
    src = EventSource([_ev(1, 0, 0.5), _ev(1, 2, 2.5), _ev(1, 4, 11)])
    src.poll(); src.poll()
    assert src.offset == 2
    src.seek(1)
    assert src.poll().event_time == 2
```

- [ ] **Step 2: Run, verify fail** — `.venv/bin/python -m pytest tests/test_source.py -v` → FAIL (no module).

- [ ] **Step 3: Implement `stl/source.py`**

```python
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
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit** (`git add stl/source.py tests/test_source.py`).

---

### Task 3: `BoundedOutOfOrdernessWatermark`

**Files:**
- Create: `stl/watermark.py`, `tests/test_watermark.py`

**Interfaces:**
- Produces: `BoundedOutOfOrdernessWatermark(max_lateness: float)`. `observe(event_time: float) -> None`. Property `current -> float` = `max_event_time_seen - max_lateness`, or `float("-inf")` before any event. `snapshot() -> dict`, `restore(state: dict) -> None`.

- [ ] **Step 1: Write failing test** `tests/test_watermark.py`

```python
from stl.watermark import BoundedOutOfOrdernessWatermark


def test_watermark_trails_max_event_time_by_lateness():
    wm = BoundedOutOfOrdernessWatermark(max_lateness=5.0)
    assert wm.current == float("-inf")
    wm.observe(2.0)
    assert wm.current == -3.0
    wm.observe(22.0)
    assert wm.current == 17.0
    wm.observe(4.0)            # out-of-order: does not move watermark back
    assert wm.current == 17.0


def test_watermark_snapshot_restore_roundtrip():
    wm = BoundedOutOfOrdernessWatermark(5.0)
    wm.observe(22.0)
    snap = wm.snapshot()
    wm2 = BoundedOutOfOrdernessWatermark(5.0)
    wm2.restore(snap)
    assert wm2.current == 17.0
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/watermark.py`**

```python
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
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 4: `KeyedStateBackend`

**Files:**
- Create: `stl/state.py`, `tests/test_state.py`

**Interfaces:**
- Produces: `KeyedStateBackend()`. String keys only. `get(key:str, default=None)`, `put(key:str, value:dict)`, `keys() -> list[str]`, `delete(key:str)`, `snapshot() -> dict` (deep-copied), `restore(state: dict)`.

- [ ] **Step 1: Write failing test** `tests/test_state.py`

```python
from stl.state import KeyedStateBackend


def test_keyed_state_isolated_per_key():
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000, "winner": "Cy"})
    s.put("7|0", {"winning_bid_cents": 500, "winner": "Di"})
    assert s.get("42|0")["winner"] == "Cy"
    assert s.get("7|0")["winner"] == "Di"
    assert s.get("99|0") is None


def test_snapshot_is_deep_copy():
    s = KeyedStateBackend()
    s.put("42|0", {"winning_bid_cents": 15000})
    snap = s.snapshot()
    s.put("42|0", {"winning_bid_cents": 99999})  # mutate after snapshot
    s2 = KeyedStateBackend()
    s2.restore(snap)
    assert s2.get("42|0")["winning_bid_cents"] == 15000
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/state.py`**

```python
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
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 5: `EventTimeTumblingWindowOperator` + `WindowResult`

**Files:**
- Create: `stl/window.py`, `stl/pipelines/__init__.py`, `stl/pipelines/common.py`, `tests/test_window.py`

**Interfaces:**
- Consumes: `Event`, `KeyedStateBackend`, `BoundedOutOfOrdernessWatermark`.
- Produces in `stl/pipelines/common.py`:
  - `WindowResult` dataclass: `show_id:int, window_start:float, window_end:float, winner:str|None, winning_bid_cents:int, late_firing:bool`.
  - `window_key(show_id:int, window_start:float) -> str` returns `f"{show_id}|{window_start:g}"`.
- Produces in `stl/window.py`: `EventTimeTumblingWindowOperator(size:float, allowed_lateness:float, state:KeyedStateBackend)`.
  - `assign(event_time:float) -> tuple[float,float]` → `(start, end)` where `start = (event_time // size) * size`, `end = start + size`.
  - `process(event:Event, watermark:float) -> str` returns one of `"on_time"`, `"late_allowed"`, `"dropped"`. Drops (returns `"dropped"`, no state change) when `watermark >= end + allowed_lateness`. Otherwise updates the keyed accumulator (only `bid` events change winner/bid; non-bid events count as accepted but don't change winner). Marks `late_allowed` when `watermark >= end` (window already eligible to fire) at process time.
  - `results() -> list[WindowResult]` materializes current accumulators (sorted by `show_id`, then `window_start`).

Accumulator shape stored in state under `window_key`: `{"show_id", "window_start", "window_end", "winning_bid_cents", "winner", "fired"}`. Winner = highest `amount_cents`; tie broken by earliest `event_time` (keep first seen at that max).

- [ ] **Step 1: Write failing test** `tests/test_window.py`

```python
from stl.event import Event
from stl.state import KeyedStateBackend
from stl.window import EventTimeTumblingWindowOperator


def _bid(show, t, amt, bidder):
    return Event(show_id=show, type="bid", bidder_id=bidder,
                 amount_cents=amt, event_time=t, ingest_time=t)


def test_assign_buckets_by_event_time():
    op = EventTimeTumblingWindowOperator(size=10, allowed_lateness=5,
                                         state=KeyedStateBackend())
    assert op.assign(4.0) == (0.0, 10.0)
    assert op.assign(11.0) == (10.0, 20.0)


def test_highest_bid_wins_window():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    for ev, wm in [(_bid(42, 0, 10000, "Ana"), -5),
                   (_bid(42, 2, 12000, "Bo"), -3),
                   (_bid(42, 4, 15000, "Cy"), 0),
                   (_bid(42, 5, 14000, "Bo"), 0)]:
        assert op.process(ev, wm) in ("on_time", "late_allowed")
    [res] = op.results()
    assert res.winner == "Cy"
    assert res.winning_bid_cents == 15000


def test_event_beyond_allowed_lateness_is_dropped():
    op = EventTimeTumblingWindowOperator(10, 5, KeyedStateBackend())
    # window [0,10): dropped once watermark >= 10 + 5 = 15
    assert op.process(_bid(99, 6, 9000, "Eve"), watermark=17.0) == "dropped"
    assert op.results() == []  # nothing accumulated
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/pipelines/common.py`**

```python
from dataclasses import dataclass


@dataclass
class WindowResult:
    show_id: int
    window_start: float
    window_end: float
    winner: str | None
    winning_bid_cents: int
    late_firing: bool


def window_key(show_id: int, window_start: float) -> str:
    return f"{show_id}|{window_start:g}"
```

- [ ] **Step 4: Implement `stl/window.py`**

```python
from stl.event import Event, EventType
from stl.state import KeyedStateBackend
from stl.pipelines.common import WindowResult, window_key


class EventTimeTumblingWindowOperator:
    def __init__(self, size: float, allowed_lateness: float, state: KeyedStateBackend):
        self.size = size
        self.allowed_lateness = allowed_lateness
        self.state = state

    def assign(self, event_time: float) -> tuple[float, float]:
        start = (event_time // self.size) * self.size
        return (start, start + self.size)

    def process(self, event: Event, watermark: float) -> str:
        start, end = self.assign(event.event_time)
        if watermark >= end + self.allowed_lateness:
            return "dropped"
        key = window_key(event.show_id, start)
        acc = self.state.get(key) or {
            "show_id": event.show_id, "window_start": start, "window_end": end,
            "winning_bid_cents": -1, "winner": None, "fired": False,
        }
        outcome = "late_allowed" if watermark >= end else "on_time"
        if event.type is EventType.BID and event.amount_cents > acc["winning_bid_cents"]:
            acc["winning_bid_cents"] = event.amount_cents
            acc["winner"] = event.bidder_id
        self.state.put(key, acc)
        return outcome

    def results(self) -> list[WindowResult]:
        out = []
        for key in self.state.keys():
            a = self.state.get(key)
            out.append(WindowResult(
                show_id=a["show_id"], window_start=a["window_start"],
                window_end=a["window_end"], winner=a["winner"],
                winning_bid_cents=max(a["winning_bid_cents"], 0),
                late_firing=a["fired"],
            ))
        return sorted(out, key=lambda r: (r.show_id, r.window_start))
```

- [ ] **Step 5: Run, verify pass.** **Step 6: Commit** (`git add stl/window.py stl/pipelines/__init__.py stl/pipelines/common.py tests/test_window.py`).

---

### Task 6: `IdempotentSink` + DuckDB serving table

**Files:**
- Create: `stl/sink.py`, `tests/test_sink.py`

**Interfaces:**
- Consumes: `WindowResult`.
- Produces: `IdempotentSink()`. `write(r: WindowResult) -> None` upserts keyed on `window_key(show_id, window_start)` (last write wins → replaying is idempotent). `gmv_by_show() -> dict[int,int]` = sum of `winning_bid_cents` over distinct windows per show. `materialize(path: str) -> None` writes a DuckDB table `show_gmv(show_id INT, gmv_cents BIGINT)`. Also `NaiveAppendSink` with the same `write` but appends (non-idempotent) and `gmv_by_show()` sums every appended row — used to demonstrate double-counting.

- [ ] **Step 1: Write failing test** `tests/test_sink.py`

```python
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
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/sink.py`**

```python
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
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 7: `PerShowMetrics` (SLO counters)

**Files:**
- Create: `stl/metrics.py`, `tests/test_metrics.py`

**Interfaces:**
- Produces: `PerShowMetrics()`. `record(show_id:int, outcome:str, event_time:float, watermark:float) -> None` where `outcome ∈ {"on_time","late_allowed","dropped"}`. `snapshot() -> dict[int, dict]` per show with keys: `events`, `late_allowed`, `dropped`, `max_event_time`, `watermark`, `watermark_lag` (= `max_event_time - watermark`).

- [ ] **Step 1: Write failing test** `tests/test_metrics.py`

```python
from stl.metrics import PerShowMetrics


def test_metrics_counts_outcomes_and_lag():
    m = PerShowMetrics()
    m.record(99, "on_time", event_time=6.0, watermark=1.0)
    m.record(99, "dropped", event_time=6.0, watermark=17.0)
    snap = m.snapshot()[99]
    assert snap["events"] == 2
    assert snap["dropped"] == 1
    assert snap["max_event_time"] == 6.0
    assert snap["watermark"] == 17.0
    assert snap["watermark_lag"] == 6.0 - 17.0
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/metrics.py`**

```python
class PerShowMetrics:
    def __init__(self):
        self._m: dict[int, dict] = {}

    def record(self, show_id: int, outcome: str, event_time: float, watermark: float) -> None:
        s = self._m.setdefault(show_id, {
            "events": 0, "late_allowed": 0, "dropped": 0,
            "max_event_time": float("-inf"), "watermark": float("-inf"),
        })
        s["events"] += 1
        if outcome == "late_allowed":
            s["late_allowed"] += 1
        elif outcome == "dropped":
            s["dropped"] += 1
        s["max_event_time"] = max(s["max_event_time"], event_time)
        s["watermark"] = watermark

    def snapshot(self) -> dict[int, dict]:
        out = {}
        for show_id, s in self._m.items():
            out[show_id] = {**s, "watermark_lag": s["max_event_time"] - s["watermark"]}
        return out
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 8: Seeds — `stl/data/*.jsonl` + `seeds.py`

**Files:**
- Create: `stl/data/shows.jsonl`, `stl/data/shows_late.jsonl`, `stl/seeds.py`, `tests/test_seeds.py`

**Interfaces:**
- Produces: `load_events(inject_late: bool = False) -> list[Event]`. Reads `shows_late.jsonl` when `inject_late` else `shows.jsonl`, one JSON object per line, returns `list[Event]`.

The two seeds differ only in **Cy's ingest_time** for Show #42 (clean: Cy arrives in order at `ingest_time=4.2`; late: Cy arrives at `ingest_time=11.0`, after the window's other bids). This is what makes naive processing-time windowing crown the wrong winner only in the late variant — proving the gate reacts to real lateness, not to a rigged seed.

- [ ] **Step 1: Write `stl/data/shows.jsonl`** (clean — Cy on time)

```json
{"show_id": 42, "type": "bid", "bidder_id": "Ana", "amount_cents": 10000, "event_time": 0.0, "ingest_time": 0.5}
{"show_id": 42, "type": "bid", "bidder_id": "Bo", "amount_cents": 12000, "event_time": 2.0, "ingest_time": 2.5}
{"show_id": 42, "type": "bid", "bidder_id": "Cy", "amount_cents": 15000, "event_time": 4.0, "ingest_time": 4.2}
{"show_id": 42, "type": "bid", "bidder_id": "Bo", "amount_cents": 14000, "event_time": 5.0, "ingest_time": 5.5}
{"show_id": 7, "type": "bid", "bidder_id": "Di", "amount_cents": 300, "event_time": 1.0, "ingest_time": 1.0}
{"show_id": 7, "type": "bid", "bidder_id": "Em", "amount_cents": 500, "event_time": 3.0, "ingest_time": 3.0}
{"show_id": 99, "type": "bid", "bidder_id": "Fi", "amount_cents": 8000, "event_time": 5.0, "ingest_time": 5.1}
{"show_id": 99, "type": "bid", "bidder_id": "Gus", "amount_cents": 20000, "event_time": 22.0, "ingest_time": 22.3}
{"show_id": 99, "type": "bid", "bidder_id": "Hal", "amount_cents": 9000, "event_time": 6.0, "ingest_time": 24.0}
```

- [ ] **Step 2: Write `stl/data/shows_late.jsonl`** — identical EXCEPT Show #42 Cy's line uses `"ingest_time": 11.0`:

```json
{"show_id": 42, "type": "bid", "bidder_id": "Ana", "amount_cents": 10000, "event_time": 0.0, "ingest_time": 0.5}
{"show_id": 42, "type": "bid", "bidder_id": "Bo", "amount_cents": 12000, "event_time": 2.0, "ingest_time": 2.5}
{"show_id": 42, "type": "bid", "bidder_id": "Cy", "amount_cents": 15000, "event_time": 4.0, "ingest_time": 11.0}
{"show_id": 42, "type": "bid", "bidder_id": "Bo", "amount_cents": 14000, "event_time": 5.0, "ingest_time": 5.5}
{"show_id": 7, "type": "bid", "bidder_id": "Di", "amount_cents": 300, "event_time": 1.0, "ingest_time": 1.0}
{"show_id": 7, "type": "bid", "bidder_id": "Em", "amount_cents": 500, "event_time": 3.0, "ingest_time": 3.0}
{"show_id": 99, "type": "bid", "bidder_id": "Fi", "amount_cents": 8000, "event_time": 5.0, "ingest_time": 5.1}
{"show_id": 99, "type": "bid", "bidder_id": "Gus", "amount_cents": 20000, "event_time": 22.0, "ingest_time": 22.3}
{"show_id": 99, "type": "bid", "bidder_id": "Hal", "amount_cents": 9000, "event_time": 6.0, "ingest_time": 24.0}
```

- [ ] **Step 3: Write failing test** `tests/test_seeds.py`

```python
from stl.seeds import load_events


def test_clean_vs_late_differ_only_in_cy_ingest():
    clean = {(e.show_id, e.bidder_id, e.event_time): e.ingest_time
             for e in load_events(inject_late=False)}
    late = {(e.show_id, e.bidder_id, e.event_time): e.ingest_time
            for e in load_events(inject_late=True)}
    diffs = {k for k in clean if clean[k] != late[k]}
    assert diffs == {(42, "Cy", 4.0)}
    assert late[(42, "Cy", 4.0)] == 11.0


def test_load_returns_events():
    evs = load_events()
    assert len(evs) == 9
    assert {e.show_id for e in evs} == {42, 7, 99}
```

- [ ] **Step 4: Run, verify fail.**

- [ ] **Step 5: Implement `stl/seeds.py`**

```python
import json
from importlib import resources
from stl.event import Event


def load_events(inject_late: bool = False) -> list[Event]:
    name = "shows_late.jsonl" if inject_late else "shows.jsonl"
    text = resources.files("stl.data").joinpath(name).read_text()
    return [Event(**json.loads(line)) for line in text.splitlines() if line.strip()]
```

Also create empty `stl/data/__init__.py` so `resources.files("stl.data")` resolves.

- [ ] **Step 6: Run, verify pass.** **Step 7: Commit** (`git add stl/data/ stl/seeds.py tests/test_seeds.py`).

---

### Task 9: Pipelines — `batch.py`, `naive.py`, `event_time.py`

**Files:**
- Create: `stl/pipelines/batch.py`, `stl/pipelines/naive.py`, `stl/pipelines/event_time.py`, `tests/test_pipelines.py`

**Interfaces:**
- Consumes: `Event`, `EventSource`, operator, watermark, state, metrics, sink.
- Produces three functions, each `(events: list[Event]) -> ResultTable` where `ResultTable = dict[str, dict]` keyed by `window_key`, value `{"show_id", "window_start", "winner", "winning_bid_cents"}`:
  - `run_batch(events)` — sort by `event_time`, bucket by event-time tumbling (size 10), highest bid wins.
  - `run_naive(events)` — bucket by **`ingest_time`** tumbling (size 10), highest bid wins. No watermark, no lateness.
  - `run_event_time(events)` — per-show `BoundedOutOfOrdernessWatermark(5)`, `EventTimeTumblingWindowOperator(10, 5)`, `PerShowMetrics`, `IdempotentSink`. Returns `(table, metrics, sink)`.
- Add to `stl/pipelines/common.py`: `SIZE = 10.0`, `ALLOWED_LATENESS = 5.0`, `MAX_LATENESS = 5.0`, and `to_table(results: list[WindowResult]) -> dict`.

- [ ] **Step 1: Add constants + `to_table` to `stl/pipelines/common.py`**

```python
SIZE = 10.0
ALLOWED_LATENESS = 5.0
MAX_LATENESS = 5.0


def to_table(results) -> dict:
    return {
        window_key(r.show_id, r.window_start): {
            "show_id": r.show_id, "window_start": r.window_start,
            "winner": r.winner, "winning_bid_cents": r.winning_bid_cents,
        }
        for r in results
    }
```

- [ ] **Step 2: Write failing test** `tests/test_pipelines.py`

```python
from stl.seeds import load_events
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time
from stl.pipelines.common import window_key

K42 = window_key(42, 0.0)


def test_batch_oracle_show42_winner_is_cy():
    t = run_batch(load_events(inject_late=True))
    assert t[K42]["winner"] == "Cy"
    assert t[K42]["winning_bid_cents"] == 15000


def test_naive_crowns_bo_under_late_arrival():
    t = run_naive(load_events(inject_late=True))
    assert t[K42]["winner"] == "Bo"
    assert t[K42]["winning_bid_cents"] == 14000


def test_event_time_matches_oracle():
    t, metrics, sink = run_event_time(load_events(inject_late=True))
    assert t[K42]["winner"] == "Cy"
    assert t[K42]["winning_bid_cents"] == 15000
    # show 99 straggler (event_time 6, arrives at 24) is dropped beyond allowed lateness
    assert metrics.snapshot()[99]["dropped"] == 1
```

- [ ] **Step 3: Run, verify fail.**

- [ ] **Step 4: Implement `stl/pipelines/batch.py`**

```python
from stl.state import KeyedStateBackend
from stl.window import EventTimeTumblingWindowOperator
from stl.pipelines.common import SIZE, to_table


def run_batch(events) -> dict:
    op = EventTimeTumblingWindowOperator(SIZE, allowed_lateness=float("inf"),
                                         state=KeyedStateBackend())
    for e in sorted(events, key=lambda e: e.event_time):
        op.process(e, watermark=float("-inf"))  # never drops; pure event-time buckets
    return to_table(op.results())
```

- [ ] **Step 5: Implement `stl/pipelines/naive.py`** (windows by ingest_time by overriding the timestamp the operator sees)

```python
from stl.event import Event
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
```

- [ ] **Step 6: Implement `stl/pipelines/event_time.py`**

```python
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
```

- [ ] **Step 7: Run, verify pass.** **Step 8: Commit** (`git add stl/pipelines/ tests/test_pipelines.py`).

> **Note for implementer:** trace Show #99 in the event-time pipeline to confirm the drop. Arrival order (by ingest): Fi(et5,ig5.1), Gus(et22,ig22.3), Hal(et6,ig24). After Gus, per-show watermark = 22−5 = 17. Hal's window is `[0,10)`; `17 >= 10+5` → `"dropped"`. Batch (sorted by event_time) buckets Hal into `[0,10)` with Fi → Fi(8000) vs Hal(9000) → batch winner Hal@9000, but event-time **drops** Hal (beyond allowed lateness). This is an intentional, documented divergence: event-time excludes Hal's beyond-lateness $90.00 bid that the all-inclusive oracle counts. Task 10's `reconcile` therefore excludes any show with `dropped > 0` from the `matches_oracle` parity set and reports Show #99 in `late_drops` instead — so parity stays True on the genuinely-comparable shows (#42, #7) while the SLO drop is surfaced honestly rather than hidden.

---

### Task 10: `reconcile` — the hero control

**Files:**
- Create: `stl/pipelines/reconcile.py`, `tests/test_reconcile.py`

**Interfaces:**
- Produces: `reconcile(inject_late: bool = True) -> dict` returning
  `{"event_time": ResultTable, "naive": ResultTable, "batch": ResultTable, "matches_oracle": bool, "naive_divergences": list[dict], "late_drops": list[dict], "metrics": dict}`.
  `matches_oracle` is True iff event-time and batch agree on every shared key whose **show had no allowed-lateness drops** — a show that intentionally dropped beyond-lateness data legitimately diverges from the all-inclusive oracle, so those shows are excluded from the parity check and reported in `late_drops` instead. `naive_divergences` lists keys where naive's winner differs from batch.

- [ ] **Step 1: Write failing test** `tests/test_reconcile.py`

```python
from stl.pipelines.reconcile import reconcile
from stl.pipelines.common import window_key

K42 = window_key(42, 0.0)


def test_hero_event_time_matches_oracle_naive_does_not():
    r = reconcile(inject_late=True)
    assert r["matches_oracle"] is True
    assert r["event_time"][K42]["winner"] == "Cy"
    assert r["event_time"][K42]["winning_bid_cents"] == 15000
    assert r["naive"][K42]["winner"] == "Bo"
    assert r["naive"][K42]["winning_bid_cents"] == 14000
    div = {d["key"]: d for d in r["naive_divergences"]}
    assert K42 in div
    assert div[K42]["naive_winner"] == "Bo"
    assert div[K42]["oracle_winner"] == "Cy"
    # show 99 dropped a beyond-lateness bid -> reported as an SLO drop, not a parity failure
    assert any(d["show_id"] == 99 for d in r["late_drops"])
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/pipelines/reconcile.py`**

```python
from stl.seeds import load_events
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time


def reconcile(inject_late: bool = True) -> dict:
    events = load_events(inject_late=inject_late)
    batch = run_batch(events)
    naive = run_naive(events)
    event_time, metrics, _sink = run_event_time(events)
    msnap = metrics.snapshot()
    dropped_shows = {sid for sid, m in msnap.items() if m["dropped"] > 0}

    # Parity excludes shows that intentionally dropped beyond-lateness data:
    # those legitimately diverge from the all-inclusive batch oracle.
    shared = {k for k in (set(event_time) & set(batch))
              if event_time[k]["show_id"] not in dropped_shows}
    matches = all(event_time[k]["winner"] == batch[k]["winner"]
                  and event_time[k]["winning_bid_cents"] == batch[k]["winning_bid_cents"]
                  for k in shared)

    divergences = []
    for k in set(naive) & set(batch):
        if naive[k]["winner"] != batch[k]["winner"]:
            divergences.append({
                "key": k, "naive_winner": naive[k]["winner"],
                "naive_bid_cents": naive[k]["winning_bid_cents"],
                "oracle_winner": batch[k]["winner"],
                "oracle_bid_cents": batch[k]["winning_bid_cents"],
            })

    late_drops = [{"show_id": sid, "dropped": msnap[sid]["dropped"]}
                  for sid in sorted(dropped_shows)]
    return {"event_time": event_time, "naive": naive, "batch": batch,
            "matches_oracle": matches, "naive_divergences": divergences,
            "late_drops": late_drops, "metrics": msnap}
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 11: `checkpoint.py` + `recovery.py` — exactly-once after crash

**Files:**
- Create: `stl/checkpoint.py`, `stl/pipelines/recovery.py`, `tests/test_recovery.py`

**Interfaces:**
- Produces `stl/checkpoint.py`: `Checkpoint` dataclass `{state: dict, offset: int, watermarks: dict[int, dict]}`. `take(state, source, watermarks) -> Checkpoint` (deep-copies state, reads `source.offset`, snapshots each watermark). `restore_into(ckpt, state, source, watermarks_factory) -> dict[int, watermark]` (restores state, `source.seek(ckpt.offset)`, rebuilds watermark objects).
- Produces `stl/pipelines/recovery.py`: `run_with_crash(events, crash_at: int) -> dict` returning `{"recovered_gmv": dict, "clean_gmv": dict, "double_count_gmv": dict, "exactly_once": bool}`. Runs event-time to completion normally for `clean_gmv`; runs again crashing after `crash_at` events, restores from the last checkpoint, replays the rest into the **same idempotent sink**, producing `recovered_gmv`; and demonstrates `double_count_gmv` by replaying from 0 into a `NaiveAppendSink`. `exactly_once = recovered_gmv == clean_gmv != double_count_gmv`.

- [ ] **Step 1: Write failing test** `tests/test_recovery.py`

```python
from stl.seeds import load_events
from stl.pipelines.recovery import run_with_crash


def test_exactly_once_recovery_matches_clean_run():
    r = run_with_crash(load_events(inject_late=True), crash_at=3)
    assert r["exactly_once"] is True
    assert r["recovered_gmv"] == r["clean_gmv"]
    assert r["recovered_gmv"] != r["double_count_gmv"]
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/checkpoint.py`**

```python
import copy
from dataclasses import dataclass


@dataclass
class Checkpoint:
    state: dict
    offset: int
    watermarks: dict


def take(state, source, watermarks) -> Checkpoint:
    return Checkpoint(
        state=copy.deepcopy(state.snapshot()),
        offset=source.offset,
        watermarks={sid: wm.snapshot() for sid, wm in watermarks.items()},
    )


def restore_into(ckpt: Checkpoint, state, source, watermarks_factory) -> dict:
    state.restore(ckpt.state)
    source.seek(ckpt.offset)
    watermarks = {}
    for sid, snap in ckpt.watermarks.items():
        wm = watermarks_factory()
        wm.restore(snap)
        watermarks[sid] = wm
    return watermarks
```

- [ ] **Step 4: Implement `stl/pipelines/recovery.py`**

```python
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
    _drain(source2, op2, restored_wms)  # replays only offset..end
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
    }
```

- [ ] **Step 5: Run, verify pass.** **Step 6: Commit** (`git add stl/checkpoint.py stl/pipelines/recovery.py tests/test_recovery.py`).

---

### Task 12: `explain.py` — deterministic narration + optional local Ollama

**Files:**
- Create: `stl/explain.py`, `tests/test_explain.py`

**Interfaces:**
- Consumes: `reconcile()` output.
- Produces: `explain(use_ollama: bool = False) -> str`. Builds a deterministic narrative from `reconcile(inject_late=True)`. When `use_ollama=True`, attempts `ollama run llama3.2` via subprocess with a 20s timeout, localhost only; on ANY failure (missing binary, timeout, nonzero exit) falls back to the deterministic text. The deterministic text MUST contain the substrings `"Cy"`, `"150.00"`, `"Bo"`, and `"140.00"`.

- [ ] **Step 1: Write failing test** `tests/test_explain.py`

```python
from stl.explain import explain


def test_deterministic_explanation_names_the_flip():
    text = explain(use_ollama=False)
    assert "Cy" in text and "150.00" in text
    assert "Bo" in text and "140.00" in text
    assert "watermark" in text.lower()
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/explain.py`**

```python
import subprocess
from stl.pipelines.reconcile import reconcile


def _dollars(cents: int) -> str:
    return f"{cents / 100:.2f}"


def _deterministic() -> str:
    r = reconcile(inject_late=True)
    lines = [
        "stream-truth-layer — what the watermark gate caught:",
        "",
        "Show #42 (Charizard final, window [0,10)):",
    ]
    for d in r["naive_divergences"]:
        if d["key"].startswith("42|"):
            lines.append(
                f"  Naive processing-time windowing crowned {d['naive_winner']} "
                f"at ${_dollars(d['naive_bid_cents'])} — it bucketed "
                f"{d['oracle_winner']}'s late ${_dollars(d['oracle_bid_cents'])} "
                f"bid into the next window."
            )
            lines.append(
                f"  Event-time windowing with a 5s watermark matched the batch "
                f"oracle: {d['oracle_winner']} wins at "
                f"${_dollars(d['oracle_bid_cents'])}."
            )
    lines.append("")
    lines.append(f"  Streaming == batch oracle: {r['matches_oracle']}.")
    return "\n".join(lines)


def explain(use_ollama: bool = False) -> str:
    base = _deterministic()
    if not use_ollama:
        return base
    try:
        out = subprocess.run(
            ["ollama", "run", "llama3.2",
             "Rewrite this incident note for an engineer, keep all names and "
             "dollar amounts exactly:\n\n" + base],
            capture_output=True, text=True, timeout=20, check=True,
        )
        return out.stdout.strip() or base
    except Exception:
        return base
```

- [ ] **Step 4: Run, verify pass.** **Step 5: Commit.**

---

### Task 13: Typer CLI — `stl/cli.py`

**Files:**
- Create: `stl/cli.py`, `tests/test_cli.py`

**Interfaces:**
- Consumes: all pipelines, `explain`, `metrics`, version.
- Produces: Typer `app` with commands: `run` (`--mode naive|event-time|batch`, default `event-time`), `reconcile` (`--inject-late/--no-inject-late`, default on), `checkpoint-restore` (`--crash-at INT`, default 3), `observe` (writes `demo/observe.html`, `--out PATH`), `explain` (`--use-ollama`), `version`. Tests use `typer.testing.CliRunner`.

- [ ] **Step 1: Write failing test** `tests/test_cli.py`

```python
from typer.testing import CliRunner
from stl.cli import app

runner = CliRunner()


def test_reconcile_command_reports_the_flip():
    res = runner.invoke(app, ["reconcile"])
    assert res.exit_code == 0
    assert "Cy" in res.stdout and "Bo" in res.stdout
    assert "matches oracle" in res.stdout.lower()


def test_checkpoint_restore_reports_exactly_once():
    res = runner.invoke(app, ["checkpoint-restore", "--crash-at", "3"])
    assert res.exit_code == 0
    assert "exactly-once" in res.stdout.lower()


def test_version():
    res = runner.invoke(app, ["version"])
    assert res.exit_code == 0
    assert "0.1.0" in res.stdout
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `stl/cli.py`**

```python
from pathlib import Path
import typer
from stl.pipelines.reconcile import reconcile as run_reconcile
from stl.pipelines.recovery import run_with_crash
from stl.pipelines.batch import run_batch
from stl.pipelines.naive import run_naive
from stl.pipelines.event_time import run_event_time
from stl.seeds import load_events
from stl.explain import explain as run_explain
from stl.observe import render_observe

app = typer.Typer(help="stream-truth-layer: a mini-Flink truth gate for live auctions")


def _dollars(c: int) -> str:
    return f"${c/100:.2f}"


@app.command()
def run(mode: str = "event-time", inject_late: bool = True):
    events = load_events(inject_late=inject_late)
    if mode == "naive":
        table = run_naive(events)
    elif mode == "batch":
        table = run_batch(events)
    else:
        table, _m, _s = run_event_time(events)
    for k in sorted(table):
        row = table[k]
        typer.echo(f"{k}: winner={row['winner']} @ {_dollars(row['winning_bid_cents'])}")


@app.command()
def reconcile(inject_late: bool = True):
    r = run_reconcile(inject_late=inject_late)
    typer.echo(f"event-time matches oracle: {r['matches_oracle']}")
    for d in r["naive_divergences"]:
        typer.echo(
            f"  DIVERGENCE {d['key']}: naive={d['naive_winner']} "
            f"{_dollars(d['naive_bid_cents'])}  oracle={d['oracle_winner']} "
            f"{_dollars(d['oracle_bid_cents'])}"
        )
    for d in r["late_drops"]:
        typer.echo(
            f"  SLO DROP show #{d['show_id']}: {d['dropped']} bid(s) beyond "
            f"allowed lateness (excluded from parity, surfaced in observe)"
        )
    if not r["matches_oracle"]:
        raise typer.Exit(1)


@app.command("checkpoint-restore")
def checkpoint_restore(crash_at: int = 3):
    r = run_with_crash(load_events(inject_late=True), crash_at=crash_at)
    typer.echo(f"clean GMV:     {r['clean_gmv']}")
    typer.echo(f"recovered GMV: {r['recovered_gmv']}")
    typer.echo(f"at-least-once would double-count to: {r['double_count_gmv']}")
    typer.echo(f"exactly-once preserved: {r['exactly_once']}")
    if not r["exactly_once"]:
        raise typer.Exit(1)


@app.command()
def observe(out: Path = Path("demo/observe.html")):
    r = run_reconcile(inject_late=True)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_observe(r["metrics"]))
    typer.echo(f"wrote {out}")


@app.command()
def explain(use_ollama: bool = False):
    typer.echo(run_explain(use_ollama=use_ollama))


@app.command()
def version():
    typer.echo("stream-truth-layer 0.1.0")
```

- [ ] **Step 4: Implement `stl/observe.py`** (used by `observe`; full HTML render of per-show SLO table)

```python
def render_observe(metrics: dict) -> str:
    rows = []
    for show_id in sorted(metrics):
        m = metrics[show_id]
        alert = "alert" if m["dropped"] else "ok"
        rows.append(
            f"<tr class='{alert}'><td>#{show_id}</td><td>{m['events']}</td>"
            f"<td>{m['late_allowed']}</td><td>{m['dropped']}</td>"
            f"<td>{m['watermark_lag']:.1f}s</td></tr>"
        )
    table = "\n".join(rows)
    return f"""<!doctype html><html><head><meta charset=utf-8>
<title>stream-truth-layer — per-tenant SLOs</title>
<style>
body{{background:#0a0a0a;color:#e5e5e5;font-family:ui-monospace,Menlo,monospace;padding:2rem}}
h1{{color:#22c55e;font-weight:600}} table{{border-collapse:collapse;width:100%;max-width:720px}}
th,td{{border:1px solid #262626;padding:.5rem .75rem;text-align:left}}
th{{color:#a3a3a3;font-weight:500}} tr.alert td{{color:#ef4444}}
caption{{text-align:left;color:#737373;margin-bottom:1rem}}
</style></head><body>
<h1>Per-tenant SLOs</h1>
<table><caption>watermark lag = liveness · dropped = correctness risk (late beyond allowed lateness)</caption>
<tr><th>show</th><th>events</th><th>late (allowed)</th><th>dropped</th><th>watermark lag</th></tr>
{table}
</table></body></html>"""
```

- [ ] **Step 5: Run CLI tests, verify pass.** **Step 6: Commit** (`git add stl/cli.py stl/observe.py tests/test_cli.py`).

---

### Task 14: CI + Pages workflows

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/pages.yml`

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: pip install -e ".[dev]"
      - run: python -m pytest -v
      - name: Hero control smoke (asserts the flip + exactly-once)
        run: |
          stl reconcile
          stl checkpoint-restore --crash-at 3
```

- [ ] **Step 2: Write `.github/workflows/pages.yml`**

```yaml
name: pages
on:
  push:
    branches: [main]
permissions:
  contents: read
  pages: write
  id-token: write
concurrency:
  group: pages
  cancel-in-progress: true
jobs:
  deploy:
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -e .
      - name: Generate observe widget
        run: stl observe --out demo/observe.html
      - uses: actions/configure-pages@v5
      - uses: actions/upload-pages-artifact@v3
        with:
          path: demo
      - id: deployment
        uses: actions/deploy-pages@v4
```

- [ ] **Step 3: Commit** (`git add .github/workflows/ci.yml .github/workflows/pages.yml`).

---

### Task 15: Hand-built demo landing page — `demo/index.html`

**Files:**
- Create: `demo/index.html`

This is a **hand-built** vanilla HTML/CSS/JS page (no build step, no frameworks). Do NOT regenerate it from a tool later. Dark-OLED aesthetic consistent with the rest of the portfolio: background `#0a0a0a`, truth/correct green `#22c55e`, bug/dropped red `#ef4444`, monospace UI font.

**Required content/structure:**
- Header: title "stream-truth-layer", one-line tagline "Event-time truth for Whatnot live auctions — watermarks, checkpoints, exactly-once."
- A "The bug" section stating the hero: naive processing-time windowing crowns **Bo @ $140.00**; event-time + 5s watermark crowns **Cy @ $150.00**, matching the batch oracle.
- An **animated late-event simulator** (vanilla JS, `requestAnimationFrame` or `setInterval`): a horizontal time axis 0–20s; four bid markers for Show #42 (Ana 0s, Bo 2s, Cy 4s, Bo 5s) where Cy's marker travels in late (arrives near x=11s); a vertical **watermark line** that advances; a toggle button "watermarking: on/off" that flips the declared winner between **Cy $150.00** (on) and **Bo $140.00** (off). Keep it under ~200 lines, all inline.
- A footer linking the GitHub repo and the generated `observe.html` (`<a href="./observe.html">per-tenant SLOs →</a>`).

- [ ] **Step 1: Write `demo/index.html`** with the structure above (inline `<style>` and `<script>`, no external assets).

- [ ] **Step 2: Verify it opens** — `open demo/index.html` (macOS) and confirm the toggle flips the winner. (Manual check; no automated test.)

- [ ] **Step 3: Commit** (`git add demo/index.html`).

---

### Task 16: Docs — README capability map, ADR, production architecture

**Files:**
- Create: `README.md`, `docs/adr/0001-when-flink.md`, `docs/production-architecture.md`

- [ ] **Step 1: Write `README.md`** containing, in order:
  1. Title + one-paragraph pitch (mini-Flink engine; event-time truth for live auctions).
  2. **Quickstart**: `python -m venv .venv && .venv/bin/pip install -e ".[dev]"` then `stl reconcile`, `stl checkpoint-restore`, `stl observe`, `stl explain`.
  3. **The hero control** — the Show #42 table (Ana/Bo/Cy/Bo) and the Bo $140 → Cy $150 flip, "streaming == batch oracle", exact CI-asserted numbers.
  4. **Capability map** table — copy the table from the spec §10 (JD requirement → file/command).
  5. **Architecture** — the `stl/` unit list with one line each.
  6. Links to the ADR and production-architecture docs, and the live Pages demo.

- [ ] **Step 2: Write `docs/adr/0001-when-flink.md`** — "When Flink is the right tool, and when it isn't." Cover: Flink fits stateful + event-time + exactly-once at scale (this repo's shape); Kafka Streams for JVM-coupled simpler topologies; a plain consumer for stateless transforms; batch when latency budget is hours. Include a short "what this toy omits vs real Flink" note (per-show watermarks here vs per-subtask in Flink; RocksDB state backend; barrier-aligned checkpoints).

- [ ] **Step 3: Write `docs/production-architecture.md`** — the would-be prod deployment: MSK/Kinesis → Flink-on-EKS, checkpoints to S3, RocksDB state backend, serving to Snowflake/BigQuery, per-tenant SLOs in Prometheus/Grafana. Include a short Terraform *sketch* fenced block (illustrative, not applied) for the Kinesis stream + Flink app. State explicitly it's documentation, not runnable IaC.

- [ ] **Step 4: Commit** (`git add README.md docs/adr/0001-when-flink.md docs/production-architecture.md`).

---

### Task 17: Ship — push public, verify green, add portfolio card

**Files:**
- Modify (separate repo): `~/Desktop/jabrunin001.github.io/src/data/content.ts`, `~/Desktop/jabrunin001.github.io/src/components/Projects.astro`

- [ ] **Step 1: Full local verification**

Run: `.venv/bin/python -m pytest -v && .venv/bin/stl reconcile && .venv/bin/stl checkpoint-restore --crash-at 3`
Expected: all tests pass; reconcile prints the divergence and exits 0; checkpoint-restore prints `exactly-once preserved: True`.

- [ ] **Step 2: Create the public repo and push** (gh CLI is authed as `jabrunin001`)

```bash
gh repo create jabrunin001/stream-truth-layer --public --source=. --remote=origin --push
```

- [ ] **Step 3: Enable Pages** — set Pages source to "GitHub Actions" (the `pages.yml` workflow). Verify both `ci` and `pages` workflows go green:

```bash
gh run list --limit 5
```

Wait for `ci` to pass on `main`; confirm `https://jabrunin001.github.io/stream-truth-layer/` serves the demo.

- [ ] **Step 4: Add a portfolio card.** In `~/Desktop/jabrunin001.github.io`: append a project object to the `projects` array in `src/data/content.ts` (`name: "stream-truth-layer"`, tagline about event-time truth for live auctions, a new unique `motif` — e.g. `"watermark"` (an advancing vertical line crossing out-of-order event dots) since `chain`/`gate`/`reorg` are taken — `blurb`, `stack: ["Python","Streaming","Event-time","DuckDB"]`, `links` to the Pages demo (primary) + GitHub repo). Define the `watermark` motif line-art SVG + `.thumb[data-motif="watermark"]` gradient in `src/components/Projects.astro`. Then `npm run build` to verify, `git pull --rebase` (iCloud-synced; expect concurrent commits), stage explicit paths, commit with co-author trailer, and push.

- [ ] **Step 5: Final confirmation** — confirm the new card links resolve and the heading count incremented.

---

## Self-Review

**Spec coverage:** every spec section maps to a task — engine units (Tasks 1–7, 11), seeds (8), three pipelines (9), hero reconcile (10), exactly-once (11), explain (12), CLI/observe (13), CI/Pages (14), demo (15), docs incl. capability map + ADR + prod arch (16), ship + portfolio card (17). Multi-tenancy = per-show keyed state + `PerShowMetrics` (Tasks 5, 7, 13 observe). SLOs = `observe` (13). Cloud-store serving = DuckDB sink (6).

**Placeholder scan:** no TBD/TODO; all code steps carry real code. The demo HTML (Task 15) and prose docs (Task 16) specify exact required content/sections rather than full text — appropriate for hand-built narrative artifacts; structure and the load-bearing strings (names, dollar amounts, links) are pinned.

**Type consistency:** `window_key`, `WindowResult`, `to_table`, `SIZE/ALLOWED_LATENESS/MAX_LATENESS`, `run_batch/run_naive/run_event_time`, `reconcile`, `run_with_crash`, `render_observe`, `explain` signatures are defined once and consumed with matching names/arities across tasks. `IdempotentSink.gmv_by_show()` / `NaiveAppendSink` used consistently in Tasks 6, 11, 13.

**Known intentional divergence:** Show #99's straggler (Hal @ $90.00) is dropped by event-time beyond allowed lateness but present in the all-inclusive batch oracle. `reconcile.matches_oracle` excludes any show with `dropped > 0` from the parity set (computed from `metrics`) and reports it in `late_drops`; the hero assertion holds on shows #42/#7. This is documented in Task 9's implementer note, asserted in Task 10's test, printed by the CLI, and surfaced as a red SLO row in `observe`. A reviewer should confirm the trace in Task 9's note (`dropped == 1` for show 99) before trusting the parity claim.
