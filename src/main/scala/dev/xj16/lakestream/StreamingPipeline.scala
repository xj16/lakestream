package dev.xj16.lakestream

import dev.xj16.lakestream.config.LakeStreamConfig
import dev.xj16.lakestream.schema.EventSchema
import dev.xj16.lakestream.sink.DeltaUpsertSink
import dev.xj16.lakestream.source.KafkaSource
import dev.xj16.lakestream.transform.EventTransforms
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

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
 */
class StreamingPipeline(spark: SparkSession, cfg: LakeStreamConfig)
    extends StrictLogging {

  private val sink = new DeltaUpsertSink(spark, cfg.storage)

  /** Build (but do not await) the streaming query. */
  def start(): StreamingQuery = {
    logger.info(
      s"Starting LakeStream: topic=${cfg.kafka.topic} -> ${cfg.storage.tablePath}"
    )

    // Make sure the target table exists before the first batch fires.
    sink.ensureTable()

    val raw         = KafkaSource.read(spark, cfg.kafka)
    val transformed = EventTransforms.clickstreamPipeline(raw)

    val trigger =
      if (cfg.stream.once) Trigger.AvailableNow()
      else Trigger.ProcessingTime(cfg.stream.triggerInterval)

    transformed.writeStream
      .queryName("lakestream-clickstream")
      .option(
        "checkpointLocation",
        cfg.storage.checkpointFor(cfg.storage.tableName)
      )
      .outputMode(cfg.stream.outputMode)
      .trigger(trigger)
      // foreachBatch is what lets us run an idempotent MERGE per micro-batch.
      .foreachBatch { (batch: org.apache.spark.sql.DataFrame, id: Long) =>
        sink.upsertBatch(batch, id)
      }
      .start()
  }

  /** Start and block until the query terminates (production entrypoint). */
  def run(): Unit = {
    val query = start()
    query.awaitTermination()
  }
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
