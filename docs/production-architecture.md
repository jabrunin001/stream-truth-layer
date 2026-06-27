# Production Architecture

This document describes how the `stream-truth-layer` engine would be deployed at production scale. Nothing here is runnable IaC — it is documentation of the target architecture.

---

## Overview

```
MSK / Kinesis
     |
     v
Flink on EKS  ←→  S3 (checkpoints)
     |              RocksDB (task-local state)
     v
Snowflake / BigQuery  (serving)
     |
     v
Prometheus / Grafana  (per-tenant SLOs)
```

---

## Ingestion

**Source: Amazon MSK (Managed Streaming for Apache Kafka) or Amazon Kinesis Data Streams.**

Bid events, sale events, and view events arrive on a topic partitioned by `show_id`. Partitioning on `show_id` ensures all events for a given show land on the same Flink subtask, keeping keyed state local without cross-subtask state migration.

MSK is the default choice when the team already operates Kafka Connect pipelines. Kinesis is preferable when the platform is fully AWS-native and wants to avoid Kafka broker management entirely. The Flink connector (`FlinkKafkaConsumer` / `KinesisStreamsSource`) handles offset tracking and exactly-once source semantics as part of the checkpoint protocol.

---

## Processing: Flink on EKS

**Cluster topology:**

- One JobManager per job (HA via ZooKeeper or Kubernetes leader election).
- N TaskManagers sized to handle peak event volume during primetime show windows. Each TaskManager runs multiple slots; one slot per `show_id` partition shard.
- Flink 1.18+ with the Kubernetes operator for declarative job lifecycle management.

**Key operator configurations:**

| Setting | Value | Rationale |
|---|---|---|
| Watermark strategy | `BoundedOutOfOrderness(5 s)` | Matches mobile bid latency distribution at p99 |
| Allowed lateness | 5 s | Retains window state for late-but-allowed bids |
| Window size | 10 s tumbling | Matches auction pacing; configurable per show type |
| State backend | RocksDB (incremental) | State size unbounded as show count grows |
| Checkpoint interval | 30 s | Recovery RPO; balance against checkpoint overhead |
| Checkpoint mode | `EXACTLY_ONCE` | Required for GMV correctness |

**State backend: RocksDB.** Each TaskManager maintains task-local RocksDB for keyed state (leading bid per `show_id`). Incremental checkpoints write only changed SST files to S3, keeping checkpoint size manageable even with millions of active shows.

---

## Checkpoints: S3

Checkpoints consist of `(operator state snapshot, source offset, per-key watermark)` written to S3 under a versioned prefix:

```
s3://platform-checkpoints/stream-truth-layer/<job-id>/<checkpoint-id>/
```

On failure, the JobManager triggers a restart from the latest completed checkpoint. The Kinesis/Kafka source resumes from the checkpointed offset; the RocksDB state backend restores from the S3 snapshot. The idempotent sink (`(show_id, window_start)` dedup key) ensures that any events replayed from the checkpointed offset do not double-count GMV in the serving layer.

This is the production equivalent of `stl checkpoint-restore`: crash, restore, replay only the remaining events, recovered GMV equals the clean run.

---

## Serving: Snowflake / BigQuery

The Flink sink writes window results to a Snowflake (or BigQuery) table via the JDBC sink or a dedicated connector:

```sql
CREATE TABLE show_gmv (
    show_id         INTEGER,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    winner          VARCHAR,
    winning_bid_cents BIGINT,
    written_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Upsert on (show_id, window_start) for idempotency
```

Late firings update the existing row for the window rather than inserting a duplicate. Downstream consumers (seller dashboards, settlement jobs, ML feature pipelines) query the serving table with low latency.

In `stream-truth-layer`, DuckDB plays this role locally. The `IdempotentSink.materialize()` method writes to a local `.duckdb` file with the same `(show_id, window_start)` dedup semantics.

---

## SLOs: Prometheus / Grafana

`metrics.py` in this repo tracks the per-tenant counters that map directly to production SLO metrics:

| Local counter | Prometheus metric | SLO |
|---|---|---|
| `events` | `stl_events_processed_total{show_id}` | throughput |
| `late_allowed` | `stl_late_events_total{show_id}` | late-data rate < X% |
| `dropped` | `stl_dropped_events_total{show_id}` | dropped-event alert threshold |
| `watermark_lag` | `stl_watermark_lag_seconds{show_id}` | lag < 10 s p99 |

Grafana dashboards are per-tenant (`show_id` label). Alerts fire on:
- Watermark lag exceeding the allowed-lateness bound (indicates a stuck partition or producer stall).
- Dropped event count above threshold (bids arriving beyond allowed lateness are real GMV at risk).
- Checkpoint duration exceeding 60 s (indicates state backend pressure).

`stl observe` generates a local HTML equivalent (`demo/observe.html`) from the same metric snapshot.

---

## Illustrative Terraform sketch

This block is documentation only. It is not tested, not applied, and not complete IaC.

```hcl
# --------------------------------------------------------------------------
# Illustrative only — not runnable IaC
# --------------------------------------------------------------------------

resource "aws_kinesis_stream" "bid_events" {
  name             = "whatnot-bid-events"
  shard_count      = 64   # one shard per ~1 MB/s; size to peak show volume
  retention_period = 24   # hours; must exceed max recovery window
}

resource "aws_s3_bucket" "flink_checkpoints" {
  bucket = "whatnot-flink-checkpoints-${var.env}"

  lifecycle_rule {
    id      = "expire-old-checkpoints"
    enabled = true
    expiration {
      days = 7
    }
  }
}

resource "helm_release" "flink_operator" {
  name       = "flink-operator"
  repository = "https://downloads.apache.org/flink/flink-kubernetes-operator-1.9.0/"
  chart      = "flink-kubernetes-operator"
  namespace  = "flink-system"
}

resource "kubectl_manifest" "stream_truth_layer_job" {
  yaml_body = <<-YAML
    apiVersion: flink.apache.org/v1beta1
    kind: FlinkDeployment
    metadata:
      name: stream-truth-layer
      namespace: flink-jobs
    spec:
      image: whatnot/stream-truth-layer:${var.image_tag}
      flinkVersion: v1_18
      flinkConfiguration:
        taskmanager.numberOfTaskSlots: "4"
        state.backend: rocksdb
        state.backend.incremental: "true"
        state.checkpoints.dir: s3://whatnot-flink-checkpoints-${var.env}/stream-truth-layer
        execution.checkpointing.interval: "30000"
        execution.checkpointing.mode: EXACTLY_ONCE
      jobManager:
        resource:
          memory: "2048m"
          cpu: 1
      taskManager:
        resource:
          memory: "4096m"
          cpu: 2
      job:
        jarURI: local:///opt/flink/usrlib/stream-truth-layer.jar
        parallelism: 16
        upgradeMode: stateful
  YAML
}
```

The Kinesis source (`FlinkKinesisConsumer` / `KinesisStreamsSource`) and Snowflake sink connector configuration are omitted for brevity. Production deployments would add IAM roles, VPC config, KMS encryption for checkpoint data, and a secrets manager reference for the Snowflake JDBC credentials.
