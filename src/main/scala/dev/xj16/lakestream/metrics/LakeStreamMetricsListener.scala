package dev.xj16.lakestream.metrics

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{
  QueryProgressEvent,
  QueryStartedEvent,
  QueryTerminatedEvent
}

/**
 * Bridges Spark's Structured Streaming progress events into [[PipelineMetrics]]
 * and the log. Registering this gives the job real runtime observability:
 * per-batch input rows, processing rate, and batch duration, plus the
 * active/terminated signal that backs the readiness probe.
 */
class LakeStreamMetricsListener extends StreamingQueryListener with StrictLogging {

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    PipelineMetrics.setQueryActive(true)
    logger.info(s"Streaming query started: id=${event.id} name=${event.name}")
  }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val p          = event.progress
    val inputRows  = p.numInputRows
    val durationMs = p.batchDuration
    val rps        = safeRate(p.processedRowsPerSecond)
    PipelineMetrics.recordProgress(inputRows, durationMs, rps)
    logger.info(
      f"batch=${p.batchId} inputRows=$inputRows " +
        f"rowsPerSecond=$rps%.1f durationMs=$durationMs"
    )
  }

  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
    PipelineMetrics.setQueryActive(false)
    event.exception.foreach(e => logger.error(s"Streaming query terminated with error: $e"))
    logger.info(s"Streaming query terminated: id=${event.id}")
  }

  // processedRowsPerSecond can be NaN/Infinity for an empty batch.
  private def safeRate(v: Double): Double =
    if (v.isNaN || v.isInfinite) 0.0 else v
}
