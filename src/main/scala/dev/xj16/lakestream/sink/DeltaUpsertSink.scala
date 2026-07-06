package dev.xj16.lakestream.sink

import dev.xj16.lakestream.config.StorageConfig
import dev.xj16.lakestream.schema.EventSchema
import dev.xj16.lakestream.transform.EventTransforms
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.StrictLogging

/**
 * Idempotent Delta sink implementing exactly-once upserts.
 *
 * The streaming query hands each micro-batch to [[upsertBatch]] through
 * `foreachBatch`. Because `foreachBatch` may re-invoke the same `batchId` on
 * recovery, a plain append would double-write. We instead `MERGE INTO` on the
 * business key so re-processing a batch is a no-op — the "match ⇒ do nothing,
 * not-match ⇒ insert" pattern makes the write idempotent regardless of
 * how many times a batch is retried.
 *
 * Schema evolution: `spark.databricks.delta.schema.autoMerge.enabled=true`
 * lets producers add new fields without a manual `ALTER TABLE` — the MERGE
 * widens the table schema to accommodate the incoming columns.
 */
class DeltaUpsertSink(spark: SparkSession, storage: StorageConfig)
    extends StrictLogging {

  private val tablePath = storage.tablePath

  /** Ensure the Delta table exists so the first MERGE has a target. */
  def ensureTable(): Unit = {
    if (!DeltaTable.isDeltaTable(spark, tablePath)) {
      logger.info(s"Creating empty Delta table at $tablePath")
      // Create an empty table with the current schema, partitioned by date.
      val empty = spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        EventTransforms
          .enrich(
            EventTransforms.parseKafkaValue(
              emptyKafkaShaped(spark)
            )
          )
          .schema
      )
      empty.write
        .format("delta")
        .mode("overwrite")
        .partitionBy(EventSchema.partitionColumns: _*)
        .save(tablePath)
    }
  }

  /**
   * The `foreachBatch` callback. `batchDf` is the (already transformed) batch
   * of unique events; `batchId` is Spark's monotonic micro-batch id.
   */
  def upsertBatch(batchDf: DataFrame, batchId: Long): Unit = {
    // Enable schema auto-merge for THIS session so new producer fields land.
    spark.conf
      .set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    ensureTable()

    // foreachBatch may hand us a stream-backed DataFrame; cache so the count
    // (for the log line) and the MERGE don't re-execute the upstream stages.
    val batch = batchDf.persist()
    try {
      val target = DeltaTable.forPath(spark, tablePath)

      logger.info(
        s"Merging batchId=$batchId (${batch.count()} unique rows) into $tablePath"
      )

      target
        .as("target")
        .merge(batch.as("source"), EventTransforms.mergeCondition)
        // Exactly-once: an event we've already stored is NOT re-inserted, and
        // we deliberately do not update it (events are immutable facts).
        .whenNotMatched()
        .insertAll()
        .execute()
    } finally {
      batch.unpersist()
    }
  }

  /**
   * A zero-row DataFrame shaped exactly like the Kafka source, so we can run
   * it through the same transform to derive the table schema for creation.
   */
  private def emptyKafkaShaped(spark: SparkSession): DataFrame = {
    import org.apache.spark.sql.types._
    val kafkaSchema = StructType(
      Seq(
        StructField("value", BinaryType),
        StructField("partition", IntegerType),
        StructField("offset", LongType)
      )
    )
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
      kafkaSchema
    )
  }
}
