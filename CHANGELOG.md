# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Partition-pruned exactly-once MERGE.** `DeltaUpsertSink` now computes the
  `[minDate, maxDate]` window of each incoming micro-batch and adds a
  `target.eventDate BETWEEN …` predicate to the merge condition. Delta prunes
  every partition outside that window, so a batch touching one or two days no
  longer scans the entire event log — the merge cost stops growing with table
  size. (`mergeConditionFor`)
- **Delta table auto-maintenance.** Optional periodic `OPTIMIZE` (with
  `ZORDER BY`) and `VACUUM` runs every _N_ committed micro-batches to counter the
  streaming small-files problem, plus a one-shot `tools.DeltaMaintenance` CLI.
  Configurable via `MAINTENANCE_*` env vars.
- **Dead-letter queue.** Records that fail to parse or lack a business key are no
  longer silently dropped — with `DLQ_ENABLED=true` they are written to a second
  Delta table (`events_dlq`) with their Kafka partition/offset, raw payload, and
  an error reason. Idempotent under batch replay via an offset-keyed MERGE.
  (`EventTransforms.deadLetters`, `sink.DlqSink`)
- **Runtime observability.** A `StreamingQueryListener` records per-batch input
  rows, processing rate, and batch duration; a dependency-free embedded HTTP
  server (JDK `HttpServer`) exposes `/healthz` (liveness), `/readyz` (readiness —
  is the query active?), and `/metrics` (Prometheus text). Enabled via
  `METRICS_ENABLED`. (`metrics.*`)
- **Graceful shutdown.** A SIGTERM/SIGINT hook stops the `StreamingQuery` before
  `spark.stop()`, so the in-flight micro-batch commits its checkpoint on
  `kubectl delete` / Ctrl-C instead of being cut off mid-batch.
- **Verifiable proof tool.** `tools.DeltaVerify` reads the Delta table and asserts
  `count() == countDistinct(eventId)`, printing a per-date / per-type breakdown
  and `PASS`/`FAIL` with a non-zero exit code — a reproducible one-command proof
  of the exactly-once guarantee (and a CI smoke gate).
- **Live interactive demo.** A self-contained, dependency-free
  [`/demo`](demo/index.html) page animates the pipeline (`parse → enrich → dedupe
  → MERGE`) and shows, live, that many records are delivered while the table
  stores each distinct id exactly once — including a "replay last batch" button
  that mirrors a Spark driver-failure replay. Deployed to GitHub Pages by CI.
- **Coverage reporting.** `sbt-scoverage` with a CI coverage step, Codecov upload,
  and an HTML report artifact.
- **Kubernetes hardening.** Liveness/readiness probes wired to the metrics server,
  a `Service` for Prometheus scraping, a `preStop` hook + longer
  `terminationGracePeriodSeconds`, and documented guidance on secret management
  and the spark-operator `SparkApplication` path.
- **Self-seeding demo stack.** A `demo` docker-compose profile
  (`docker compose --profile demo up`) auto-produces 2000 events and then runs
  `DeltaVerify` to assert exactly-once — no manual steps or MinIO-console
  eyeballing.

### Changed

- The streaming query now reads the **raw** Kafka source and performs all
  transformation (valid-event upsert + dead-letter routing) inside a single
  `foreachBatch`, so both writes commit against the same checkpointed offsets.
- Config gained `dlq`, `maintenance`, and `metrics` sections; all default to
  off/safe values and are overridable via env vars, preserving the 12-factor,
  one-JAR-everywhere model.

### Tested

- `StreamingPipelineSpec` — drives a real Spark `MemoryStream` through the actual
  `foreachBatch` + Delta sink with an on-disk checkpoint, restarts the pipeline
  mid-stream, and asserts row count still equals distinct ids. Closes the
  previously-untested streaming/foreachBatch/checkpoint gap.
- `DeltaMaintenanceSpec` — partition-pruned merge condition, empty-batch
  fallback, exactly-once under pruning, and OPTIMIZE/VACUUM row-preservation.
- `DeadLetterSpec` — valid/invalid split is an exact complement; DLQ captures
  offsets + reasons.
- `DeltaVerifySpec` — the verifier PASSes on a clean table and FAILs when
  duplicates leak.
- `PipelineMetricsSpec` / `MetricsServerSpec` — counter/gauge accounting and the
  live `/healthz` `/readyz` `/metrics` endpoints over real HTTP.

## [0.1.0] — 2026

Initial release: Scala + Spark Structured Streaming lakehouse ingesting Kafka
click-stream events into a partitioned Delta Lake table on S3/MinIO with
end-to-end exactly-once processing (idempotent `MERGE INTO` in `foreachBatch`)
and automatic schema evolution. Reproducible via Docker Compose and a local
`kind` Kubernetes cluster, with a GitHub Actions CI that compiles, unit-tests,
builds the fat JAR, builds the Docker image, and validates the k8s manifests.
