# stream-truth-layer — Design Spec

**Date:** 2026-06-27
**Target role:** Whatnot — Software Engineer, Data Platform (streaming / Flink platform)
**Repo:** `~/Desktop/stream-truth-layer` → public on `jabrunin001`

## 1. Purpose

A runnable, clone-and-run-free portfolio piece for a **streaming platform** role. The JD is about
*evolving the stream-processing platform layer itself* — watermarking, checkpointing, state
management, multi-tenancy, exactly-once, SLOs — not about building downstream pipelines on top of an
existing platform. So the deliverable is **a small streaming engine you can read**, plus a memorable
control that proves it handles event-time correctly where a naive pipeline silently does not.

Constraints (inherited from the established portfolio workflow):

- Pure Python. No Java, no real Flink/Kafka cluster, no network, no cloud keys. `pip install` + run.
- One memorable **hero control** demonstrating real engineering judgment.
- Typer + Pydantic CLI, GitHub Actions CI that asserts the proof, optional **local** AI angle
  (deterministic + optional Ollama, never cloud/keys), README capability map mapping each JD
  requirement to where it lives.

Explicitly **out of scope (YAGNI):** real PyFlink, real cloud / `terraform apply`, any network or
API keys. Production cloud architecture is documentation only.

## 2. The hero control — watermark / late-data truth gate

The streaming analog of a reconciliation gate. Three pipelines run over **one event log**:

1. **naive (processing-time)** — windows events by *arrival* time, no watermark, no late-data
   handling, non-idempotent sink → **wrong** when events arrive out of order.
2. **event-time (the platform)** — watermark (bounded out-of-orderness) + tumbling windows + allowed
   lateness + late side output + idempotent sink + checkpointing → **correct**.
3. **batch ground-truth** — sorts the full bounded log by `event_time` and windows it offline → the
   reference answer.

**Assertion:** `event-time == batch ground-truth`, and `naive ≠ ground-truth`. The proof exists twice
(streaming == batch when done right). Exact numbers are CI-asserted.

### Hero scenario (cents-precise, memorable)

**Show #42, "Vintage Pokémon Break"** — final Charizard auction. Tumbling window = 10s, allowed
lateness = 5s, watermark out-of-orderness bound = 5s.

| bidder | amount | event_time | note |
|--------|--------|-----------|------|
| Ana | $100.00 (10000¢) | 0s | on time |
| Bo  | $120.00 (12000¢) | 2s | on time |
| Cy  | **$150.00 (15000¢)** | 4s | laggy phone — *arrives* after the window looks closed |
| Bo  | $140.00 (14000¢) | 5s | on time |

- **Naive (processing-time):** Cy's $150 bid arrives too late to land in the window by arrival time →
  dropped → **winner = Bo @ 14000¢**, show GMV undercounted. Job still "runs green."
- **Event-time + allowed lateness:** Cy's bid has `event_time = 4s`, so event-time windowing assigns
  it to window [0–10s] regardless of arrival time. When Cy is processed (ingest ~11s), show #42's
  watermark is 0 (well below `window_end = 10`), so the engine classifies Cy as `on_time` — no late
  firing occurs. The window result is emitted at end-of-stream via `results()` →
  **winner = Cy @ 15000¢** == batch ground-truth.

**Multi-tenancy / SLO coverage via additional shows:**

- **Show #7** — clean stream, no late data (baseline; proves the gate does not "fix" what isn't broken).
- **Show #99** — heavy late data; one bid arrives *beyond* allowed lateness → correctly routed to the
  **late side output** (excluded from the result) and surfaced as a dropped-event SLO signal.

Each show is an isolated keyed-state partition (`show_id` is the multi-tenant key).

### Secondary proof — exactly-once recovery

`stl checkpoint-restore --crash-at N`: run the event-time pipeline, snapshot (operator state + source
offset + watermark) at event N, simulate a crash, `restore()`, replay N..end. Final GMV still totals to
ground-truth — **no double-count** — because the source resumes from the checkpointed offset and the
sink is idempotent on `(show_id, window)`. A naive at-least-once path (reprocess + non-idempotent sink)
would double-count; this is asserted.

## 3. Architecture — `stl/` package

Small, single-purpose, independently testable units:

- **`event.py`** — `Event(show_id, type, bidder_id, amount_cents, event_time, ingest_time)` as a
  Pydantic model. `type ∈ {bid, sold, view}`. `amount_cents` is an int (cents discipline; no floats for money).
- **`source.py`** — replays a seeded event log in **arrival (`ingest_time`) order**, exposing a
  resumable **offset** so checkpoint/restore can replay from a position.
- **`watermark.py`** — `BoundedOutOfOrdernessWatermark(max_lateness)`: tracks `max_event_time_seen`,
  emits `watermark = max_event_time_seen − max_lateness`.
- **`state.py`** — `KeyedStateBackend`: per-`show_id` state, with `snapshot()` / `restore()`
  serialization to disk. The unit that embodies "state management."
- **`window.py`** — `EventTimeTumblingWindowOperator(size, allowed_lateness)`: assigns events to
  tumbling windows by `event_time`; classifies each as `on_time`, `late_allowed` (within
  `window_end + allowed_lateness`), or `dropped` (beyond it — routed to a late side output and counted
  as an SLO signal); emits final per-window winners at end-of-stream via `results()`. This is a
  simplification of Flink's incremental on-watermark firing.
- **`checkpoint.py`** — `Checkpoint`: snapshot/restore of `(KeyedStateBackend state, source offset,
  watermark)`.
- **`sink.py`** — `IdempotentSink`: dedups on `(show_id, window)`; materializes results to a small
  **DuckDB** serving table for point-lookup of per-show GMV / winner (prod target noted as
  Snowflake/BigQuery; DuckDB is the local stand-in).
- **`metrics.py`** — per-tenant SLO counters: watermark lag, events processed, late-but-allowed count,
  dropped (too-late) count, throughput.

Pipelines (drivers wiring the units together):

- **`pipelines/naive.py`** — processing-time, no watermark/lateness, non-idempotent.
- **`pipelines/event_time.py`** — the real platform pipeline (all of the above).
- **`pipelines/batch.py`** — offline sort-by-event_time ground truth.
- **`pipelines/reconcile.py`** — runs all three, diffs naive vs truth, asserts event_time == truth.

## 4. CLI — `stl` (Typer + Pydantic)

- `stl run [--naive | --event-time | --batch]` — run one pipeline; print per-show GMV + winning bids.
- `stl reconcile [--inject-late]` — **the hero**: run all three, show naive's wrong winner/GMV, and
  event-time == batch truth.
- `stl checkpoint-restore [--crash-at N]` — demonstrate exactly-once recovery.
- `stl observe` — per-tenant SLO view (watermark lag, late/dropped counts, throughput); writes a
  generated widget to `demo/observe.html` (never `index.html`).
- `stl explain [--use-ollama]` — deterministic narration of what the gate caught; optional **local**
  Ollama (localhost only).
- `stl version`.

## 5. Data / seeds

Seeded event log (JSONL) for Shows #42, #7, #99. A default **clean** seed and a **late-injected**
variant selected by `--inject-late` (variants selected by flag — default seeds never edited). Crafted so
that under naive processing the wrong bidder wins Show #42's Charizard auction.

## 6. Tests & CI

- **~20+ pytest** unit tests: watermark generation, window assignment, allowed-lateness late firing,
  late side output routing, keyed-state isolation across shows, checkpoint snapshot/restore roundtrip,
  idempotent-sink dedup. Plus the hero `reconcile` assertion (exact numbers) and the exactly-once
  checkpoint-restore assertion.
- **`ci.yml`** — pip install, pytest (asserts hero numbers + exactly-once), `stl reconcile` smoke run.
- **`pages.yml`** — uploads `demo/`.

## 7. Demo / Pages

- Hand-built dark-OLED **`demo/index.html`** (vanilla HTML/CSS/JS, no build) with an animated
  late-event simulator: bids arriving out of order, a watermark line advancing, the window firing, and
  the winner flipping Bo → Cy when watermarking is enabled. Designed via `ui-ux-pro-max` Dark Mode
  (OLED): green = correct/truth, red = the naive bug/dropped.
- `stl observe` writes generated `demo/observe.html`. Both ship via `pages.yml`.

## 8. Docs (judgment the JD asks for)

- **README capability map** — every JD bullet → file/command in the repo.
- **`docs/adr/0001-when-flink.md`** — "When Flink is the right tool, and when it isn't" (Flink vs Kafka
  Streams vs batch vs a plain consumer; stateful + event-time + exactly-once at scale → Flink; simple
  stateless transforms → don't).
- **`docs/production-architecture.md`** — MSK/Kinesis → Flink-on-EKS, checkpoints to S3, RocksDB state
  backend, serving to Snowflake/BigQuery, with a Terraform *sketch* (documentation only).

## 9. Definition of done

Pure-Python engine + three pipelines + hero `reconcile` + checkpoint-restore exactly-once + multi-tenant
`observe`, all green in CI with exact-number assertions; hand-built demo + generated observe widget live
on Pages; README capability map + ADR + production-architecture doc; shipped public to `jabrunin001`; a
card added to the `jabrunin001.github.io` portfolio site.

## 10. JD → repo capability map (to live in README)

| JD requirement | Where it lives |
|---|---|
| Build/evolve the platform layer itself | `stl/` engine (watermark, state, windows, checkpoint, sink) |
| Checkpointing, state management, watermarking | `checkpoint.py`, `state.py`, `watermark.py`, `window.py` |
| Stateful real-time pipelines at scale | `pipelines/event_time.py` + keyed state |
| Exactly-once | `sink.py` (idempotent) + `checkpoint-restore` proof |
| Multi-tenant streaming infra | `show_id` keyed state isolation + per-tenant `metrics.py` / `observe` |
| When Flink is right and when it is not | `docs/adr/0001-when-flink.md` |
| SLOs, reliability, monitoring/alerting | `metrics.py` + `stl observe` (watermark lag, dropped counts) |
| Serving data products / cloud data stores | `sink.py` DuckDB serving table (prod: Snowflake/BigQuery) |
| Cloud + IaC | `docs/production-architecture.md` (Terraform sketch) |
| Well-tested, Python | pytest suite + Typer/Pydantic CLI + CI |
