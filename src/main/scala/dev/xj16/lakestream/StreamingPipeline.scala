package dev.xj16.lakestream

import dev.xj16.lakestream.config.LakeStreamConfig
import dev.xj16.lakestream.metrics.LakeStreamMetricsListener
import dev.xj16.lakestream.schema.EventSchema
import dev.xj16.lakestream.sink.{DeltaUpsertSink, DlqSink}
import dev.xj16.lakestream.source.KafkaSource
import dev.xj16.lakestream.transform.EventTransforms
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}

/**
 * Wires source → transform → exactly-once Delta sink into a single running
 * Structured Streaming query.
 *
 * The design gives end-to-end exactly-once processing:
 *   1. Kafka offsets are tracked in the streaming checkpoint (Spark replays
 *      from the last committed offset on restart).
 *   2. Each micro-batch is deduped within the batch, then MERGE-ed into Delta
 *      on the business key, so re-processing a batch (or a duplicate producer
 *      record) never double-writes.
 *
 * The stream is written on the RAW Kafka source and all transformation happens
 * inside `foreachBatch`. That keeps the valid-event upsert and the dead-letter
 * routing driven off the exact same batch of raw records, so both writes commit
 * (idempotently) against the same checkpointed offsets.
 */
class StreamingPipeline(spark: SparkSession, cfg: LakeStreamConfig)
    extends StrictLogging {

  private val sink = new DeltaUpsertSink(spark, cfg.storage, cfg.maintenance)
  private val dlq  = new DlqSink(spark, cfg.storage, cfg.dlq.tableName)

  /** The metrics listener registered on the SparkSession (exposed for cleanup). */
  private val metricsListener: StreamingQueryListener = new LakeStreamMetricsListener

  /** Build (but do not await) the streaming query. */
  def start(): StreamingQuery = {
    logger.info(
      s"Starting LakeStream: topic=${cfg.kafka.topic} -> ${cfg.storage.tablePath} " +
        s"(dlq=${cfg.dlq.enabled}, maintenance=${cfg.maintenance.enabled})"
    )

    // Make sure the target table exists before the first batch fires.
    sink.ensureTable()
    if (cfg.dlq.enabled) dlq.ensureTable()

    spark.streams.addListener(metricsListener)

    val raw = KafkaSource.read(spark, cfg.kafka)

    val trigger =
      if (cfg.stream.once) Trigger.AvailableNow()
      else Trigger.ProcessingTime(cfg.stream.triggerInterval)

    raw.writeStream
      .queryName("lakestream-clickstream")
      .option(
        "checkpointLocation",
        cfg.storage.checkpointFor(cfg.storage.tableName)
      )
      .trigger(trigger)
      // foreachBatch is what lets us run an idempotent MERGE per micro-batch.
      .foreachBatch { (rawBatch: DataFrame, id: Long) =>
        processBatch(rawBatch, id)
      }
      .start()
  }

  /**
   * Per-micro-batch work: transform the raw Kafka batch into unique valid
   * events and upsert them exactly-once; and, when the DLQ is enabled, route
   * the records that failed to parse to the dead-letter table.
   */
  private[lakestream] def processBatch(rawBatch: DataFrame, batchId: Long): Unit = {
    val valid = EventTransforms.clickstreamPipeline(rawBatch)
    sink.upsertBatch(valid, batchId)
    if (cfg.dlq.enabled) {
      dlq.writeBatch(EventTransforms.deadLetters(rawBatch), batchId)
    }
  }

  /** Start and block until the query terminates (production entrypoint). */
  def run(): Unit = {
    val query = start()
    query.awaitTermination()
  }

  /** Remove the metrics listener; call during shutdown. */
  def cleanup(): Unit =
    try spark.streams.removeListener(metricsListener)
    catch { case _: Throwable => () }
}

object StreamingPipeline {

  /** Convenience constructor that also validates schema wiring at startup. */
  def apply(spark: SparkSession, cfg: LakeStreamConfig): StreamingPipeline = {
    require(
      EventSchema.eventSchema.fieldNames.contains(EventSchema.idColumn),
      s"id column ${EventSchema.idColumn} must exist in the event schema"
    )
    new StreamingPipeline(spark, cfg)
  }
}
