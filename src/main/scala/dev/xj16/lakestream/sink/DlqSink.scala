package dev.xj16.lakestream.sink

import dev.xj16.lakestream.config.StorageConfig
import dev.xj16.lakestream.metrics.PipelineMetrics
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.StrictLogging

/**
 * Sink for rejected records (the dead-letter queue).
 *
 * Records that fail to parse or lack a business key would otherwise vanish; here
 * they are persisted to a second Delta table with their Kafka coordinates, the
 * raw payload, and an error reason so a producer bug is debuggable rather than
 * invisible. Like the main sink, writes are idempotent under batch replay — we
 * MERGE on `(kafkaPartition, kafkaOffset)` so re-processing a batch never
 * double-writes a bad record.
 */
class DlqSink(spark: SparkSession, storage: StorageConfig, tableName: String)
    extends StrictLogging {

  private val tablePath = s"${storage.basePath.stripSuffix("/")}/$tableName"

  private val dlqSchema = StructType(
    Seq(
      StructField("kafkaPartition", IntegerType, nullable = true),
      StructField("kafkaOffset", LongType, nullable = true),
      StructField("rawValue", StringType, nullable = true),
      StructField("errorReason", StringType, nullable = true),
      StructField("ingestTime", TimestampType, nullable = true)
    )
  )

  def ensureTable(): Unit =
    if (!DeltaTable.isDeltaTable(spark, tablePath)) {
      logger.info(s"Creating empty DLQ table at $tablePath")
      spark
        .createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], dlqSchema)
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)
    }

  /**
   * Persist a batch of rejected records. No-op for an empty batch. `batchId`
   * is used only for logging; idempotency comes from the offset-keyed MERGE.
   */
  def writeBatch(deadLetters: DataFrame, batchId: Long): Unit = {
    val batch = deadLetters.persist()
    try {
      val n = batch.count()
      if (n == 0L) return
      ensureTable()
      logger.warn(s"DLQ batchId=$batchId: routing $n rejected record(s) to $tablePath")
      PipelineMetrics.recordDlq(n)

      DeltaTable
        .forPath(spark, tablePath)
        .as("t")
        .merge(
          batch.as("s"),
          "t.kafkaPartition = s.kafkaPartition AND t.kafkaOffset = s.kafkaOffset"
        )
        .whenNotMatched()
        .insertAll()
        .execute()
    } finally {
      batch.unpersist()
    }
  }
}
