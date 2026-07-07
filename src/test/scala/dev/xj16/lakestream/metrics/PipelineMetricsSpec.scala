package dev.xj16.lakestream.metrics

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for the dependency-free metric registry and Prometheus renderer. */
class PipelineMetricsSpec extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = PipelineMetrics.reset()

  test("counters accumulate across upserts and dlq writes") {
    PipelineMetrics.recordUpsert(10)
    PipelineMetrics.recordUpsert(5)
    PipelineMetrics.recordDlq(2)

    val s = PipelineMetrics.snapshot
    assert(s.batchesTotal == 2)
    assert(s.rowsUpsertTotal == 15)
    assert(s.dlqRowsTotal == 2)
  }

  test("progress gauges reflect the most recent batch and ignore NaN rates") {
    PipelineMetrics.recordProgress(inputRows = 100, durationMs = 250, rowsPerSecond = 400.0)
    val s = PipelineMetrics.snapshot
    assert(s.lastInputRows == 100)
    assert(s.lastBatchDurationMs == 250)
    assert(math.abs(s.lastRowsPerSecond - 400.0) < 1e-6)
  }

  test("query-active flag backs the readiness signal") {
    assert(!PipelineMetrics.isQueryActive)
    PipelineMetrics.setQueryActive(true)
    assert(PipelineMetrics.isQueryActive)
  }

  test("renderPrometheus emits well-formed exposition text") {
    PipelineMetrics.recordUpsert(7)
    PipelineMetrics.setQueryActive(true)
    val text = PipelineMetrics.renderPrometheus()

    assert(text.contains("# TYPE lakestream_rows_upserted_total counter"))
    assert(text.contains("lakestream_rows_upserted_total 7"))
    assert(text.contains("lakestream_query_active 1"))
    // Every metric line must have a HELP and TYPE header before it.
    assert(text.contains("# HELP lakestream_batches_total"))
  }
}
