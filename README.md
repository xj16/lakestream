# LakeStream

**Exactly-once click-stream ingestion from Kafka into a Delta Lake lakehouse — Scala + Spark Structured Streaming, with a partition-pruned idempotent `MERGE`, a dead-letter queue, live Prometheus metrics, and a one-command reproducible stack.**

[![CI](https://github.com/xj16/lakestream/actions/workflows/ci.yml/badge.svg)](https://github.com/xj16/lakestream/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/xj16/lakestream/branch/main/graph/badge.svg)](https://codecov.io/gh/xj16/lakestream)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Scala 2.12](https://img.shields.io/badge/Scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark 3.5](https://img.shields.io/badge/Spark-3.5.1-orange.svg)](https://spark.apache.org/)
[![Live demo](https://img.shields.io/badge/live-demo-39d0d8.svg)](https://xj16.github.io/lakestream/)

LakeStream is a compact, production-shaped reference for **streaming ingestion into an open lakehouse**. It reads a click-stream off Kafka, parses and enriches each event, and upserts it into a partitioned [Delta Lake](https://delta.io/) table on S3-compatible object storage (MinIO locally, real S3 in prod) — with **end-to-end exactly-once semantics** and **automatic schema evolution**, so a duplicate record or a replayed micro-batch never double-writes and a new producer field never breaks the pipeline.

It is deliberately **streaming-only**: no batch ELT, no orchestration, no Airflow. Just the hot path from Kafka to the lakehouse, done correctly — and instrumented, dead-letter-safe, and self-maintaining enough to run unattended.

> ### ▶ [Try the live, no-backend demo →](https://xj16.github.io/lakestream/)
> A dependency-free browser visualization of the exact pipeline: watch duplicates and replayed batches get collapsed by the idempotent `MERGE` while a live counter proves **stored rows always equal distinct event ids**.

---

## Why

Most "streaming to a data lake" demos append blindly. On failure, Spark replays the last micro-batch — and a naive append silently duplicates every row in it. LakeStream shows the correct pattern:

- **Checkpointed Kafka offsets** give at-least-once *delivery* from the source.
- An **idempotent `MERGE INTO` on the business key** inside `foreachBatch` upgrades that to exactly-once *processing* in the sink: re-running a batch (any number of times) is a no-op.
- **Delta schema auto-merge** lets upstream producers add fields without a manual migration or a broken job.

The whole thing is reproducible on your laptop — `docker compose up`, or a two-node `kind` cluster — with no cloud account and nothing paid.

---

## Architecture

```
                                                    ┌──► events      (Delta, partitioned by eventDate)
┌────────────┐  JSON   ┌───────────────────────────────────┐  MERGE   │
│  Producers │ ──────► │  Spark Structured Streaming        │ ────────►┤
│  (Kafka)   │ events  │  read→parse→enrich→dedupe→ MERGE    │  upsert  │
└────────────┘         │  (+ split malformed → DLQ)         │          └──► events_dlq  (Delta, bad records)
      ▲                └───────────────────────────────────┘
      │ EventProducer         │ checkpoint (offsets)   │ /metrics /healthz /readyz
      │ (~10% dup ids)        ▼                        ▼
                       s3a://…/_checkpoints      Prometheus + k8s probes
```

**Pipeline stages** (`dev.xj16.lakestream`):

| Stage | Component | What it does |
|-------|-----------|--------------|
| Source | `source.KafkaSource` | Structured Streaming Kafka reader; `maxOffsetsPerTrigger` bounds each batch. |
| Parse | `transform.EventTransforms.parseKafkaValue` | `from_json` against a fixed schema; unknown fields ignored, unparseable rows split off. |
| Enrich | `transform.EventTransforms.enrich` | Derives `eventTime` (from epoch millis) + `eventDate` partition + ingest timestamp. |
| Dedupe | `transform.EventTransforms.dedupeWithinBatch` | Collapses duplicate ids **within** a micro-batch. |
| Sink | `sink.DeltaUpsertSink` | `foreachBatch` → **partition-pruned** Delta `MERGE INTO … WHEN NOT MATCHED INSERT`, idempotent across batches; periodic `OPTIMIZE`/`VACUUM`. |
| DLQ | `sink.DlqSink` | Routes unparseable / key-less records to `events_dlq` with Kafka offset + reason (opt-in). |
| Observe | `metrics.*` | `StreamingQueryListener` + embedded `/healthz` `/readyz` `/metrics` server. |
| Orchestrate | `StreamingPipeline` | Wires it together; manages checkpoint, trigger, DLQ split, and listener. |

### How exactly-once works

Two layers combine:

1. **Within a batch** — `dropDuplicates(eventId)` removes duplicate producer records that landed in the same micro-batch.
2. **Across batches / on replay** — the sink does not append. It runs:

   ```sql
   MERGE INTO target USING source
     ON target.eventId = source.eventId
        AND target.eventDate BETWEEN <batch-min-date> AND <batch-max-date>
     WHEN NOT MATCHED THEN INSERT *
   ```

   Because the match is on the immutable business key and matched rows are left untouched, **re-processing the very same `batchId`** (which Spark can do after a driver failure) inserts nothing new. The table row count always equals the number of *distinct* event ids seen — never the number of records delivered.

   **Partition pruning matters here.** Without the `eventDate BETWEEN …` predicate, Delta must scan the *entire* target table on every micro-batch to find matches — an unbounded cost that worsens as the log grows. Since the table is partitioned by `eventDate`, `DeltaUpsertSink` computes the batch's actual date window and bounds the merge to it, so a batch spanning a day or two only touches one or two partitions. (`mergeConditionFor`)

The `DeltaUpsertSinkSpec` and `StreamingPipelineSpec` tests prove this: the former calls `upsertBatch(batch, 7L)` three times and asserts the row count stays constant; the latter drives a real `MemoryStream` through the streaming `foreachBatch`, restarts the pipeline mid-stream, and asserts `count() == distinct(eventId)` survives the restart.

### Schema evolution

The sink sets `spark.databricks.delta.schema.autoMerge.enabled=true`, so when a producer starts emitting a new column the `MERGE` **widens the table schema** automatically. Existing rows get `null` for the new column; new rows carry the value. Covered by the "schema evolution absorbs a new producer field" test.

---

## Features

- ✅ **Exactly-once processing** via idempotent Delta `MERGE` in `foreachBatch`.
- ✅ **Partition-pruned merge** — the merge is bounded to the batch's `eventDate` window, so cost does not grow with table size.
- ✅ **Delta auto-maintenance** — periodic `OPTIMIZE` (`ZORDER`) + `VACUUM` counter the streaming small-files problem, no external cron.
- ✅ **Dead-letter queue** — malformed / key-less records go to `events_dlq` with Kafka offset + reason, never silently dropped.
- ✅ **Automatic schema evolution** — producers can add fields freely.
- ✅ **Runtime observability** — `StreamingQueryListener` + a dependency-free `/metrics` (Prometheus) `/healthz` `/readyz` server.
- ✅ **Graceful shutdown** — SIGTERM stops the query so the in-flight batch commits its checkpoint before exit.
- ✅ **Open lakehouse format** — Delta Lake tables on S3-compatible storage, no vendor lock-in.
- ✅ **Date-partitioned event log** for efficient time-range reads.
- ✅ **Backpressure-safe** — `maxOffsetsPerTrigger` bounds recovery batches.
- ✅ **12-factor config** — one JAR, everything via env vars; identical across local / Docker / k8s.
- ✅ **Fully reproducible locally** — `docker compose` stack *and* a `kind` Kubernetes deployment.
- ✅ **Verifiable** — `DeltaVerify` asserts `count == distinct(eventId)` with a PASS/FAIL exit code; a `demo` compose profile seeds + verifies in one command.
- ✅ **Tested + covered** — ScalaTest specs (incl. a real `MemoryStream` streaming test) run the whole exactly-once path against a local-filesystem Delta table; scoverage in CI.
- ✅ **Live browser demo** — a self-contained visualization of the pipeline, deployed to GitHub Pages.

---

## Tech stack

| Concern | Choice |
|---------|--------|
| Language | **Scala 2.12** (Spark's supported dialect) |
| Streaming engine | **Apache Spark 3.5.1** Structured Streaming |
| Message bus | **Apache Kafka** (KRaft, no ZooKeeper) |
| Storage runtime | **Apache Hadoop 3.3** S3A committer |
| Table format | **Delta Lake 3.2** |
| Object store | **MinIO** (S3-compatible) locally |
| Packaging | **Docker** multi-stage + `sbt-assembly` fat JAR |
| Orchestration | **Kubernetes** manifests + **kind** for local clusters |
| Config | Typesafe Config + PureConfig |
| Observability | `StreamingQueryListener` + embedded JDK `HttpServer` (Prometheus, no deps) |
| CI | **GitHub Actions** (compile, coverage test, assembly, Docker build, manifest validation, Pages deploy) |
| Coverage | **scoverage** → Codecov |
| Build | sbt 1.9 |

---

## Getting started

### Prerequisites

- JDK 11 and [sbt](https://www.scala-sbt.org/) (to build / test / assemble)
- Docker + Docker Compose (for the local stack)
- Optionally [kind](https://kind.sigs.k8s.io/) + kubectl (for the Kubernetes path)

### Build & test

```bash
sbt scalafmtCheckAll                 # formatting
sbt test                             # ScalaTest specs (exactly-once, streaming, DLQ, metrics)
sbt clean coverage test coverageReport   # + scoverage HTML report (make coverage)
sbt assembly                         # fat JAR at target/scala-2.12/lakestream-assembly-0.1.0.jar
```

The tests spin up a local `SparkSession` and write Delta tables to a temp dir — **no Kafka or MinIO required**, so they run anywhere including CI. `StreamingPipelineSpec` additionally drives a real Spark `MemoryStream` through the streaming `foreachBatch` + checkpoint.

### Run the full stack with Docker Compose

**One command, self-seeding + self-verifying** (recommended):

```bash
docker compose --profile demo up --build   # or: make demo
```

This starts Kafka (KRaft), MinIO (bucket auto-created) and the LakeStream job, then a one-shot `producer` publishes 2000 events (~10% **duplicate ids**), and finally a `verify` job asserts `count() == distinct(eventId)` and prints **PASS/FAIL** — no manual steps, no eyeballing.

Core stack only (drive the producer yourself):

```bash
docker compose up --build
make produce   # publishes 2000 events; needs sbt assembly + a host JDK
```

While it runs, watch it live:

```bash
curl localhost:9464/metrics   # Prometheus counters: rows upserted, dlq rows, rate…
curl localhost:9464/readyz    # 200 while the streaming query is active
```

Open the MinIO console at <http://localhost:9001> (`minioadmin` / `minioadmin`) and browse `lakehouse/delta/events/` — Delta files partitioned by `eventDate`, with a row count equal to the number of *distinct* ids. Or prove it programmatically:

```bash
make verify   # DeltaVerify: PASS iff count == distinct(eventId), else non-zero exit
```

### Run on a local kind cluster

```bash
make kind-up      # creates the cluster, builds+loads the image, applies all manifests
kubectl -n lakestream get pods
```

`make kind-up` runs:

```bash
kind create cluster --name lakestream --config k8s/kind-cluster.yaml
docker build -t lakestream:local .
kind load docker-image lakestream:local --name lakestream
kubectl apply -f k8s/00-namespace.yaml -f k8s/10-minio.yaml -f k8s/20-kafka.yaml -f k8s/30-lakestream.yaml
```

MinIO's S3 API and console are port-mapped to `localhost:9000` / `localhost:9001`. Tear down with `make kind-down`.

---

## Configuration

All settings live in [`src/main/resources/reference.conf`](src/main/resources/reference.conf) and every value has an env-var override, so the same JAR runs unchanged everywhere.

| Env var | Default | Meaning |
|---------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_TOPIC` | `events` | Source topic |
| `KAFKA_STARTING_OFFSETS` | `earliest` | Where a fresh checkpoint begins |
| `KAFKA_MAX_OFFSETS_PER_TRIGGER` | `100000` | Backlog cap per micro-batch |
| `STORAGE_BASE_PATH` | `s3a://lakehouse/delta` | Delta table root |
| `STORAGE_CHECKPOINT_PATH` | `s3a://lakehouse/_checkpoints` | Streaming checkpoint root |
| `S3_ENDPOINT` | `http://localhost:9000` | MinIO/S3 endpoint (empty ⇒ real AWS S3) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `minioadmin` | Object-store creds |
| `STREAM_TRIGGER_INTERVAL` | `10 seconds` | Processing-time trigger |
| `STREAM_ONCE` | `false` | `true` ⇒ run once (`AvailableNow`) and stop |
| `DLQ_ENABLED` | `false` | Route unparseable records to `events_dlq` instead of dropping |
| `DLQ_TABLE_NAME` | `events_dlq` | Dead-letter table name (under `STORAGE_BASE_PATH`) |
| `MAINTENANCE_ENABLED` | `false` | Run periodic `OPTIMIZE`/`VACUUM` from the stream |
| `MAINTENANCE_EVERY_BATCHES` | `50` | Maintenance cadence, in committed micro-batches |
| `MAINTENANCE_ZORDER_COLUMN` | `eventId` | Column to `ZORDER BY` during `OPTIMIZE` (empty ⇒ none) |
| `MAINTENANCE_VACUUM_RETENTION_HOURS` | `168` | `VACUUM` retention window (hours) |
| `METRICS_ENABLED` | `false` | Start the embedded `/healthz` `/readyz` `/metrics` server |
| `METRICS_PORT` | `9464` | Port for the metrics/health server |

---

## Observability

Set `METRICS_ENABLED=true` (default in the k8s manifest) to start a
dependency-free HTTP server — built on the JDK's `com.sun.net.httpserver`, no
metrics library — on `METRICS_PORT` (default `9464`):

| Endpoint | Purpose |
|----------|---------|
| `GET /healthz` | Liveness — 200 once the process is up. Backs the k8s `livenessProbe`. |
| `GET /readyz` | Readiness — 200 **only while the streaming query is active**, else 503. Backs the `readinessProbe`. |
| `GET /metrics` | Prometheus text exposition. |

Exported metrics include `lakestream_rows_upserted_total`,
`lakestream_dlq_rows_total`, `lakestream_batches_total`,
`lakestream_last_rows_per_second`, `lakestream_last_batch_duration_ms`, and
`lakestream_query_active`. A `StreamingQueryListener` feeds the per-batch gauges
and logs `batch=… inputRows=… rowsPerSecond=… durationMs=…` each trigger.

On `SIGTERM`/`SIGINT` the app stops the `StreamingQuery` first, so the in-flight
micro-batch commits its offset checkpoint before Spark shuts down — no partial
batch on `kubectl delete` or Ctrl-C.

---

## Project layout

```
build.sbt                     # sbt build (Spark provided, Delta/Kafka/S3A on classpath)
project/Dependencies.scala    # pinned versions in one place
src/main/scala/dev/xj16/lakestream/
  LakeStreamApp.scala         # entrypoint (metrics server + graceful shutdown)
  StreamingPipeline.scala     # source → transform → (sink + DLQ) wiring
  config/                     # typed PureConfig config (kafka/storage/stream/dlq/maintenance/metrics)
  schema/EventSchema.scala    # canonical event schema
  source/KafkaSource.scala    # Structured Streaming Kafka reader
  transform/EventTransforms.scala  # pure DataFrame transforms + dead-letter split
  sink/DeltaUpsertSink.scala  # partition-pruned exactly-once MERGE + OPTIMIZE/VACUUM
  sink/DlqSink.scala          # idempotent dead-letter table writer
  metrics/                    # PipelineMetrics, StreamingQueryListener, embedded HTTP server
  tools/EventProducer.scala   # synthetic demo producer (injects duplicates)
  tools/DeltaVerify.scala     # asserts count == distinct(eventId) (PASS/FAIL)
  tools/DeltaMaintenance.scala# one-shot OPTIMIZE + VACUUM
src/main/resources/           # reference.conf, logback.xml
src/test/scala/...            # ScalaTest specs (incl. MemoryStream streaming test)
demo/index.html               # self-contained live pipeline visualization (GitHub Pages)
Dockerfile, docker-compose.yml (+ demo profile), docker/entrypoint.sh
k8s/                          # kind cluster + manifests (probes, metrics Service)
.github/workflows/ci.yml      # compile, coverage test, assembly, docker build, manifest validation
.github/workflows/pages.yml   # deploy /demo to GitHub Pages
```

---

## Testing strategy

- `EventTransformsSpec` — parsing, enrichment, forward-compat with unknown fields, within-batch dedup.
- `DeltaUpsertSinkSpec` — the core guarantees: unique inserts, **idempotent replay**, overlapping batches, **schema evolution**.
- `StreamingPipelineSpec` — **the flagship**: drives a real Spark `MemoryStream` through the streaming `foreachBatch` + Delta sink with an on-disk checkpoint, restarts the pipeline mid-stream, and asserts `count() == distinct(eventId)` survives; plus the streaming DLQ split.
- `DeltaMaintenanceSpec` — partition-pruned merge condition, empty-batch fallback, exactly-once under pruning, and `OPTIMIZE`/`VACUUM` row-preservation.
- `DeadLetterSpec` — the valid/invalid split is an exact complement; the DLQ captures offsets + reasons.
- `DeltaVerifySpec` — the verifier PASSes on a clean table and FAILs when duplicates leak.
- `PipelineMetricsSpec` / `MetricsServerSpec` — counter/gauge accounting and the live `/healthz` `/readyz` `/metrics` endpoints over real HTTP.
- `LakeStreamConfigSpec` — config loading, path derivation, and the new `dlq`/`maintenance`/`metrics` sections (incl. defaults).

All run against a local `SparkSession` + local-filesystem Delta, so CI needs no external services. Coverage is measured with **scoverage** and uploaded to Codecov.

---

## License

MIT © 2026 xj16 — see [LICENSE](LICENSE).
