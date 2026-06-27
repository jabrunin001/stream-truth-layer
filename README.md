# stream-truth-layer

A pure-Python mini-Flink streaming engine for Whatnot live auctions. It implements the platform layer itself — watermarking, keyed state, tumbling windows, exactly-once via idempotent sink + checkpointing, and per-tenant SLOs — not a pipeline built on top of an existing platform. The hero control is a reconciliation gate that proves event-time processing produces the correct auction winner where naive (processing-time) processing silently gets it wrong.

---

## Quickstart

```bash
python -m venv .venv && .venv/bin/pip install -e ".[dev]"
```

With the venv active (`source .venv/bin/activate`), run `stl <cmd>` directly. Without activating, prefix each command with `.venv/bin/`.

```bash
# Hero: event-time truth gate vs naive and batch oracle
stl reconcile

# Exactly-once: crash mid-stream, restore, verify recovered GMV == clean GMV
stl checkpoint-restore

# Per-tenant SLO dashboard (writes demo/observe.html)
stl observe

# Deterministic narration of what the gate caught
stl explain
```

Run the test suite:

```bash
.venv/bin/python -m pytest -q
# 34 passed
```

---

## The hero control

**Show #42 — "Vintage Pokemon Break" — final Charizard auction**

Window: 10 s tumbling. Watermark out-of-orderness bound: 5 s. Allowed lateness: 5 s.

| bidder | amount     | event_time | arrival    | note                                     |
|--------|------------|------------|------------|------------------------------------------|
| Ana    | $100.00    | 0 s        | ~0 s       | on time                                  |
| Bo     | $120.00    | 2 s        | ~2 s       | on time                                  |
| Cy     | **$150.00**| 4 s        | ~11 s      | laggy phone — arrives after window looks closed |
| Bo     | $140.00    | 5 s        | ~5 s       | on time                                  |

**Naive (processing-time):** The naive pipeline windows by arrival/ingest time. Cy arrives at ingest ~11 s, so naive buckets Cy into window [10–20s]. The [0–10s] window closes with Bo as the winner at $140.00. GMV is undercounted. The job still exits green.

**Event-time + 5 s watermark:** Cy's bid carries `event_time = 4 s`, so event-time windowing assigns it to window [0–10s] regardless of when it arrives. When Cy is processed (ingest ~11 s), show #42's watermark is `max(0, 2, 5) − 5 = 0`, far below the window end (10 s), so the engine classifies Cy as `on_time` — the `late_allowed` counter for show #42 stays at 0. The window result is emitted at end-of-stream via `results()`. Winner: **Cy @ $150.00** — matching the batch oracle.

The 5 s allowed-lateness budget governs how long window state is retained for late-arriving data (demonstrated by show #99's drop below), not what allows Cy's bid through.

```
event-time matches oracle: True
  DIVERGENCE 42|0: naive=Bo $140.00  oracle=Cy $150.00
  DIVERGENCE 99|0: naive=Fi $80.00  oracle=Hal $90.00
  SLO DROP show #99: 1 bid(s) beyond allowed lateness (excluded from parity, surfaced in observe)
```

This assertion runs in CI on every push.

**Show #99** diverges on two dimensions. First, the naive divergence: naive processing-time bucketing puts Fi's $80.00 bid (arrives early at ingest ~5 s) as the [0–10s] winner, while the batch oracle correctly includes Hal's $90.00 bid (event_time 6 s, arrives late at ingest 24 s) in [0–10s], making Hal the oracle winner. Second, the SLO drop: in the streaming event-time pipeline, Hal's bid arrives at ingest 24 s, by which point the watermark has advanced to 17 s — past `window_end (10) + allowed_lateness (5) = 15` — so Hal's bid is dropped. Because show #99 has dropped data, it is excluded from the parity check and reported under `late_drops` rather than counted as a parity failure.

---

## Capability map

| JD requirement | Where it lives |
|---|---|
| Build/evolve the platform layer itself | `stl/` engine (watermark, state, windows, checkpoint, sink) |
| Checkpointing, state management, watermarking | `checkpoint.py`, `state.py`, `watermark.py`, `window.py` |
| Stateful real-time pipelines at scale | `pipelines/event_time.py` + keyed state |
| Exactly-once | `sink.py` (idempotent) + `stl checkpoint-restore` proof |
| Multi-tenant streaming infra | `show_id` keyed state isolation + per-tenant `metrics.py` / `stl observe` |
| When Flink is right and when it is not | `docs/adr/0001-when-flink.md` |
| SLOs, reliability, monitoring/alerting | `metrics.py` + `stl observe` (watermark lag, dropped counts) |
| Serving data products / cloud data stores | `sink.py` DuckDB serving table (prod: Snowflake/BigQuery) |
| Cloud + IaC | `docs/production-architecture.md` (Terraform sketch) |
| Well-tested, Python | 34 pytest tests + Typer/Pydantic CLI + CI |

---

## Architecture

The `stl/` package is a set of small, independently testable units wired together by pipeline drivers.

**Engine units**

- `event.py` — `Event` Pydantic model: `show_id`, `type` (`bid`/`sold`/`view`), `bidder_id`, `amount_cents` (int, cents discipline), `event_time`, `ingest_time`.
- `source.py` — `EventSource`: replays events in arrival (`ingest_time`) order; exposes a resumable `offset` for checkpoint/restore via `seek()`.
- `watermark.py` — `BoundedOutOfOrdernessWatermark(max_lateness)`: tracks `max_event_time_seen`, emits `watermark = max_event_time_seen - max_lateness`.
- `state.py` — `KeyedStateBackend`: per-`show_id` dict state with `snapshot()` / `restore()` for serialization to disk.
- `window.py` — `EventTimeTumblingWindowOperator`: assigns events to tumbling windows by `event_time`; classifies each event as `on_time`, `late_allowed` (arrives within `window_end + allowed_lateness`), or `dropped` (beyond it — routed to a late side output and counted as an SLO signal); emits final per-window winners at end-of-stream via `results()`. This is a simplification of Flink's incremental on-watermark firing.
- `sink.py` — `IdempotentSink`: deduplicated on `(show_id, window_start)`; materializes results to DuckDB serving table. `NaiveAppendSink` is the non-idempotent foil used to demonstrate double-counting.
- `metrics.py` — `PerShowMetrics`: per-tenant counters — events processed, `late_allowed`, `dropped`, watermark lag.
- `checkpoint.py` — `Checkpoint` dataclass + `take()` / `restore_into()`: snapshots `(KeyedStateBackend state, source offset, per-show watermarks)` atomically.

**Pipelines**

- `pipelines/naive.py` — processing-time, no watermark, non-idempotent sink.
- `pipelines/event_time.py` — the real platform pipeline: watermark + tumbling windows + allowed lateness + idempotent sink + metrics.
- `pipelines/batch.py` — offline sort-by-`event_time` ground truth (the oracle).
- `pipelines/reconcile.py` — runs all three, diffs naive vs oracle, asserts event-time == oracle.
- `pipelines/recovery.py` — crash-at-N, restore from checkpoint, replay remaining events, assert recovered GMV equals clean run and differs from at-least-once double-count.

**CLI / output**

- `cli.py` — Typer app: `stl run`, `stl reconcile`, `stl checkpoint-restore`, `stl observe`, `stl explain`, `stl version`.
- `observe.py` — renders per-tenant SLO widget to `demo/observe.html`.
- `explain.py` — deterministic narration of gate results; optional local Ollama (`--use-ollama`).
- `seeds.py` — loads `stl/data/shows.jsonl` (clean) or `shows_late.jsonl` (late-injected) by flag.

---

## Further reading

- [ADR 0001 — When Flink is the right tool, and when it isn't](docs/adr/0001-when-flink.md)
- [Production architecture](docs/production-architecture.md)
- [Live demo (GitHub Pages)](https://jabrunin001.github.io/stream-truth-layer/)
