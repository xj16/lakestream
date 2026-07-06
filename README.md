# LakeStream

**A Scala + Apache Spark Structured Streaming lakehouse: ingest from Kafka, process exactly-once with schema evolution into Delta Lake storage, reproducible on a local kind cluster.**

[![CI](https://github.com/xj16/lakestream/actions/workflows/ci.yml/badge.svg)](https://github.com/xj16/lakestream/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Scala 2.12](https://img.shields.io/badge/Scala-2.12-red.svg)](https://www.scala-lang.org/)
[![Spark 3.5](https://img.shields.io/badge/Spark-3.5.1-orange.svg)](https://spark.apache.org/)

LakeStream is a compact, production-shaped reference for **streaming ingestion into an open lakehouse**. It reads a click-stream off Kafka, parses and enriches each event, and upserts it into a partitioned [Delta Lake](https://delta.io/) table on S3-compatible object storage (MinIO locally, real S3 in prod) — with **end-to-end exactly-once semantics** and **automatic schema evolution**, so a duplicate record or a replayed micro-batch never double-writes and a new producer field never breaks the pipeline.

It is deliberately **streaming-only**: no batch ELT, no orchestration, no Airflow. Just the hot path from Kafka to the lakehouse, done correctly.

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
┌────────────┐   JSON events    ┌──────────────────────────────────────┐   MERGE    ┌─────────────────┐
│  Producers │ ───────────────► │  Spark Structured Streaming (LakeStream) │ ────────► │  Delta Lake table │
│  (Kafka)   │   topic: events  │                                        │  upsert    │  on S3A / MinIO   │
└────────────┘                  │  read → parse → enrich → dedupe → merge │           │  partitioned by   │
      ▲                         └──────────────────────────────────────┘           │  eventDate        │
      │ demo EventProducer                 │  checkpoint (offsets)                  └─────────────────┘
      │ (10% duplicate ids)                ▼
                                   s3a://lakehouse/_checkpoints
```

**Pipeline stages** (`dev.xj16.lakestream`):

| Stage | Component | What it does |
|-------|-----------|--------------|
| Source | `source.KafkaSource` | Structured Streaming Kafka reader, `maxOffsetsPerTrigger` bounds each batch. |
| Parse | `transform.EventTransforms.parseKafkaValue` | `from_json` against a fixed schema; unknown fields ignored, unparseable rows dropped. |
| Enrich | `transform.EventTransforms.enrich` | Derives `eventTime` (from epoch millis) + `eventDate` partition + ingest timestamp. |
| Dedupe | `transform.EventTransforms.dedupeWithinBatch` | Collapses duplicate ids **within** a micro-batch. |
| Sink | `sink.DeltaUpsertSink` | `foreachBatch` → Delta `MERGE INTO ... WHEN NOT MATCHED INSERT`, idempotent across batches. |
| Orchestrate | `StreamingPipeline` | Wires it together, manages checkpoint + trigger. |

### How exactly-once works

Two layers combine:

1. **Within a batch** — `dropDuplicates(eventId)` removes duplicate producer records that landed in the same micro-batch.
2. **Across batches / on replay** — the sink does not append. It runs:

   ```sql
   MERGE INTO target USING source
     ON target.eventId = source.eventId
     WHEN NOT MATCHED THEN INSERT *
   ```

   Because the match is on the immutable business key and matched rows are left untouched, **re-processing the very same `batchId`** (which Spark can do after a driver failure) inserts nothing new. The table row count always equals the number of *distinct* event ids seen — never the number of records delivered.

The `DeltaUpsertSinkSpec` test proves this: it calls `upsertBatch(batch, 7L)` three times and asserts the row count stays constant.

### Schema evolution

The sink sets `spark.databricks.delta.schema.autoMerge.enabled=true`, so when a producer starts emitting a new column the `MERGE` **widens the table schema** automatically. Existing rows get `null` for the new column; new rows carry the value. Covered by the "schema evolution absorbs a new producer field" test.

---

## Features

- ✅ **Exactly-once processing** via idempotent Delta `MERGE` in `foreachBatch`.
- ✅ **Automatic schema evolution** — producers can add fields freely.
- ✅ **Open lakehouse format** — Delta Lake tables on S3-compatible storage, no vendor lock-in.
- ✅ **Date-partitioned event log** for efficient time-range reads.
- ✅ **Backpressure-safe** — `maxOffsetsPerTrigger` bounds recovery batches.
- ✅ **12-factor config** — one JAR, everything via env vars; identical across local / Docker / k8s.
- ✅ **Fully reproducible locally** — `docker compose` stack *and* a `kind` Kubernetes deployment.
- ✅ **Tested** — ScalaTest specs run the whole exactly-once path against a local-filesystem Delta table (no Kafka/MinIO needed in CI).
- ✅ **Demo producer** that injects ~10% duplicate ids so you can *see* dedup working.

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
| CI | **GitHub Actions** (compile, test, assembly, Docker build, manifest validation) |
| Build | sbt 1.9 |

---

## Getting started

### Prerequisites

- JDK 11 and [sbt](https://www.scala-sbt.org/) (to build / test / assemble)
- Docker + Docker Compose (for the local stack)
- Optionally [kind](https://kind.sigs.k8s.io/) + kubectl (for the Kubernetes path)

### Build & test

```bash
sbt scalafmtCheckAll   # formatting
sbt test               # ScalaTest specs (exactly-once + schema evolution)
sbt assembly           # fat JAR at target/scala-2.12/lakestream-assembly-0.1.0.jar
```

The tests spin up a local `SparkSession` and write Delta tables to a temp dir — **no Kafka or MinIO required**, so they run anywhere including CI.

### Run the full stack with Docker Compose

```bash
docker compose up --build
```

This starts Kafka (KRaft), MinIO (with the `lakehouse` bucket auto-created), and the LakeStream job. Then, from the host, publish some demo events:

```bash
# Needs the assembled JAR (sbt assembly) and a JDK on the host.
make produce
# equivalently:
java -cp target/scala-2.12/lakestream-assembly-0.1.0.jar \
  dev.xj16.lakestream.tools.EventProducer localhost:9092 events 2000 0.1
```

The producer sends 2000 events with ~10% **duplicate ids**. Open the MinIO console at <http://localhost:9001> (`minioadmin` / `minioadmin`) and browse `lakehouse/delta/events/` — you'll find Delta files partitioned by `eventDate`, and the row count equals the number of *distinct* ids, proving dedup.

Verify the count with a quick spark-shell (or any Delta reader):

```scala
spark.read.format("delta").load("s3a://lakehouse/delta/events").count()
// == number of distinct eventIds, NOT 2000
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

---

## Project layout

```
build.sbt                     # sbt build (Spark provided, Delta/Kafka/S3A on classpath)
project/Dependencies.scala    # pinned versions in one place
src/main/scala/dev/xj16/lakestream/
  LakeStreamApp.scala         # entrypoint
  StreamingPipeline.scala     # source → transform → sink wiring
  config/                     # typed PureConfig config
  schema/EventSchema.scala    # canonical event schema
  source/KafkaSource.scala    # Structured Streaming Kafka reader
  transform/EventTransforms.scala  # pure, testable DataFrame transforms
  sink/DeltaUpsertSink.scala  # idempotent exactly-once Delta MERGE
  tools/EventProducer.scala   # synthetic demo producer (injects duplicates)
src/main/resources/           # reference.conf, logback.xml
src/test/scala/...            # ScalaTest specs
Dockerfile, docker-compose.yml, docker/entrypoint.sh
k8s/                          # kind cluster + Kubernetes manifests
.github/workflows/ci.yml      # compile, test, assembly, docker build, manifest validation
```

---

## Testing strategy

- `EventTransformsSpec` — parsing, enrichment, forward-compat with unknown fields, within-batch dedup.
- `DeltaUpsertSinkSpec` — the core guarantees: unique inserts, **idempotent replay**, overlapping batches, **schema evolution**.
- `LakeStreamConfigSpec` — config loading and path derivation.

All run against a local `SparkSession` + local-filesystem Delta, so CI needs no external services.

---

## License

MIT © 2026 xj16 — see [LICENSE](LICENSE).
