package dev.xj16.lakestream.metrics

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

/**
 * Process-wide, dependency-free metric registry for the streaming job.
 *
 * Structured Streaming's `foreachBatch` callback and the `StreamingQueryListener`
 * both run on the driver, so a small set of atomics is enough — we deliberately
 * avoid pulling in Dropwizard/Micrometer for a single-JVM streaming driver. The
 * values are rendered as Prometheus text by [[MetricsServer]] and drive the
 * readiness probe.
 */
object PipelineMetrics {

  // Monotonic counters.
  private val batchesTotal    = new AtomicLong(0L)
  private val rowsUpsertTotal = new AtomicLong(0L)
  private val dlqRowsTotal    = new AtomicLong(0L)

  // Last-batch gauges (from the StreamingQueryListener).
  private val lastInputRows        = new AtomicLong(0L)
  private val lastBatchDurationMs  = new AtomicLong(0L)
  private val lastRowsPerSecondCts = new AtomicLong(0L) // stored *1000 for 3dp

  // Liveness/readiness state.
  private val queryActive = new AtomicReference[Boolean](false)
  private val startedAtMs = System.currentTimeMillis()

  /** Records an upsert of `rows` unique rows into the events table. */
  def recordUpsert(rows: Long): Unit = {
    batchesTotal.incrementAndGet()
    rowsUpsertTotal.addAndGet(rows)
  }

  /** Records `rows` rejected records routed to the dead-letter table. */
  def recordDlq(rows: Long): Unit = dlqRowsTotal.addAndGet(rows)

  /** Records the per-batch progress reported by the StreamingQueryListener. */
  def recordProgress(
      inputRows: Long,
      durationMs: Long,
      rowsPerSecond: Double
  ): Unit = {
    lastInputRows.set(inputRows)
    lastBatchDurationMs.set(durationMs)
    lastRowsPerSecondCts.set(math.round(rowsPerSecond * 1000.0))
  }

  /** Marks the streaming query active/inactive for the readiness probe. */
  def setQueryActive(active: Boolean): Unit = queryActive.set(active)

  /** True once a streaming query has started and not yet terminated. */
  def isQueryActive: Boolean = queryActive.get()

  /** Snapshot for tests and the /metrics renderer. */
  def snapshot: MetricsSnapshot =
    MetricsSnapshot(
      batchesTotal = batchesTotal.get(),
      rowsUpsertTotal = rowsUpsertTotal.get(),
      dlqRowsTotal = dlqRowsTotal.get(),
      lastInputRows = lastInputRows.get(),
      lastBatchDurationMs = lastBatchDurationMs.get(),
      lastRowsPerSecond = lastRowsPerSecondCts.get() / 1000.0,
      queryActive = queryActive.get(),
      uptimeSeconds = (System.currentTimeMillis() - startedAtMs) / 1000L
    )

  /** Reset all state — used between test cases. */
  private[metrics] def reset(): Unit = {
    batchesTotal.set(0L)
    rowsUpsertTotal.set(0L)
    dlqRowsTotal.set(0L)
    lastInputRows.set(0L)
    lastBatchDurationMs.set(0L)
    lastRowsPerSecondCts.set(0L)
    queryActive.set(false)
  }

  /** Render the current metrics in Prometheus text exposition format. */
  def renderPrometheus(): String = {
    val s = snapshot
    val sb = new StringBuilder
    def metric(name: String, help: String, typ: String, value: String): Unit = {
      sb.append(s"# HELP $name $help\n")
      sb.append(s"# TYPE $name $typ\n")
      sb.append(s"$name $value\n")
    }
    metric(
      "lakestream_batches_total",
      "Total number of committed micro-batches.",
      "counter",
      s.batchesTotal.toString
    )
    metric(
      "lakestream_rows_upserted_total",
      "Total unique rows merged into the events table.",
      "counter",
      s.rowsUpsertTotal.toString
    )
    metric(
      "lakestream_dlq_rows_total",
      "Total records routed to the dead-letter table.",
      "counter",
      s.dlqRowsTotal.toString
    )
    metric(
      "lakestream_last_input_rows",
      "Rows read by the most recent micro-batch.",
      "gauge",
      s.lastInputRows.toString
    )
    metric(
      "lakestream_last_batch_duration_ms",
      "Wall-clock duration of the most recent micro-batch, in ms.",
      "gauge",
      s.lastBatchDurationMs.toString
    )
    metric(
      "lakestream_last_rows_per_second",
      "Processing rate of the most recent micro-batch.",
      "gauge",
      f"${s.lastRowsPerSecond}%.3f"
    )
    metric(
      "lakestream_query_active",
      "1 if the streaming query is currently active, else 0.",
      "gauge",
      if (s.queryActive) "1" else "0"
    )
    metric(
      "lakestream_uptime_seconds",
      "Seconds since the metrics registry was initialised.",
      "gauge",
      s.uptimeSeconds.toString
    )
    sb.toString
  }
}

/** Immutable snapshot of the metric registry. */
final case class MetricsSnapshot(
    batchesTotal: Long,
    rowsUpsertTotal: Long,
    dlqRowsTotal: Long,
    lastInputRows: Long,
    lastBatchDurationMs: Long,
    lastRowsPerSecond: Double,
    queryActive: Boolean,
    uptimeSeconds: Long
)
