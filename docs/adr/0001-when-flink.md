# ADR 0001 — When Flink is the right tool, and when it isn't

**Date:** 2026-06-27
**Status:** Accepted

---

## Context

Live auction platforms process a continuous stream of bids, views, and sale events across thousands of concurrent shows. The core question is whether that workload justifies the operational cost of Apache Flink. The answer depends on what the processing actually needs to do.

---

## The decision axes

Three questions determine the right tool:

1. Is the processing **stateful across events**? (auction leading-bid tracking, session windows, user-level aggregations)
2. Does **event time matter**? (bids can arrive late from laggy mobile clients; arrival order is not event order)
3. Is **exactly-once** a hard requirement? (double-counting GMV, triggering duplicate payouts, or missing a bid that changes the winner are real bugs)

When all three answers are yes, Flink is the right tool. When they are not, it is not.

---

## When Flink fits

**Stateful + event-time + exactly-once at scale.** This is the shape of the live auction platform:

- Each show is a keyed partition with independent state (leading bid, current watermark, window accumulator). State must survive operator restarts.
- Bids arrive out of order — a laggy mobile client might deliver a $150 bid at 11 s that has `event_time = 4 s`. Processing-time windowing silently drops it. Event-time windowing with bounded out-of-orderness watermarks and allowed lateness handles it correctly.
- GMV and auction winners are money. Exactly-once delivery (idempotent sink keyed on `(show_id, window_start)` + source offset in the checkpoint) prevents double-counting on restart.
- At the scale of a live-commerce platform — millions of events per second across tens of thousands of concurrent shows — the Flink runtime's parallelism, backpressure, and RocksDB state backend are not optional.

This repo is built in the shape of Flink to demonstrate that judgment. The engine units (`watermark.py`, `state.py`, `window.py`, `checkpoint.py`, `sink.py`) map directly to Flink's watermark strategy, keyed state backend, window operator, checkpoint mechanism, and idempotent sink.

---

## When Kafka Streams fits instead

Kafka Streams is the right choice when:

- The topology is **JVM-native** and the team is already in the Java/Scala ecosystem with no appetite to run a separate Flink cluster.
- State requirements are **moderate** — local RocksDB per partition, no need for Flink's distributed state migration.
- The topology is **simple** — filter, map, join, aggregate — without the complex windowing or late-data semantics that Flink's runtime handles more cleanly.

Kafka Streams is embedded in the application JAR. No separate cluster means lower ops burden for smaller teams. The trade-off is that scaling is coarser and exactly-once guarantees are harder to reason about across sinks that are not Kafka topics.

---

## When a plain consumer fits

A plain Kafka consumer (Python, Go, whatever) is the right tool when the processing is **stateless**: parse the event, validate it, write it somewhere, emit a metric. No aggregation, no joins, no windows.

Adding Flink or Kafka Streams to a stateless transform is pure overhead — additional infrastructure, additional failure modes, additional operational surface. Do not do it.

---

## When batch fits

Batch (Spark, dbt, SQL) is the right tool when the **latency budget is hours**. Daily GMV rollups, end-of-day settlement reports, ML feature backfills — these do not need a streaming engine. Streaming these workloads introduces complexity (state expiry, watermark tuning, late-data handling) with no benefit to the downstream consumer.

The batch pipeline in this repo (`pipelines/batch.py`) is the ground-truth oracle precisely because it sorts by `event_time` and processes the full bounded log. It is correct by construction and trivial to reason about. The job of the streaming engine is to match it in real time.

---

## What this toy omits vs real Flink

This engine is built for clarity, not production. The gaps are intentional:

**Per-show watermarks, not per-subtask.** In real Flink, each parallel subtask maintains its own watermark, and the operator receives the minimum watermark across all upstream subtasks. Watermark propagation is a distributed coordination problem. Here, each show carries its own `BoundedOutOfOrdernessWatermark` instance, which is correct for a single-threaded simulation but does not model the subtask-minimum behavior.

**No RocksDB state backend.** `KeyedStateBackend` here is an in-memory dict with `snapshot()` / `restore()`. Real Flink uses RocksDB for incremental checkpoints, which enables state larger than heap and efficient snapshot deltas. The interface (`put`, `get`, `snapshot`, `restore`) maps directly to Flink's `ValueState` API, so the shape is right.

**No barrier-aligned distributed checkpoints.** Flink's checkpoint protocol injects barriers into the event stream and aligns them across all parallel operators before snapshotting. Here, `checkpoint.take()` snapshots `(state, source offset, watermarks)` at an arbitrary point in a single-threaded loop. The semantics are equivalent for a single-partition source, but the distributed coordination is absent.

**No resource management.** Flink on EKS with TaskManagers, slot allocation, and network shuffle is omitted. The production deployment path is documented in `docs/production-architecture.md`.

These are not bugs — they are deliberate simplifications that keep the engine readable while demonstrating the right abstractions.
