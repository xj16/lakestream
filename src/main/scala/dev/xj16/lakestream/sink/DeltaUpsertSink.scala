package dev.xj16.lakestream.sink

import dev.xj16.lakestream.config.{MaintenanceConfig, StorageConfig}
import dev.xj16.lakestream.metrics.PipelineMetrics
import dev.xj16.lakestream.schema.EventSchema
import dev.xj16.lakestream.transform.EventTransforms
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{max => sqlMax, min => sqlMin}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.scalalogging.StrictLogging

import java.util.concurrent.atomic.AtomicLong

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
 * '''Partition pruning:''' the naive `MERGE ... ON target.eventId = source.eventId`
 * forces Delta to scan the entire target table on every micro-batch to look for
 * matches — an unbounded, ever-worsening cost as the event log grows. Because the
 * table is partitioned by `eventDate`, we additionally constrain the merge to the
 * `[minDate, maxDate]` window actually present in the incoming batch. Delta then
 * prunes every partition outside that window, so a batch spanning one or two days
 * only touches one or two partitions instead of the whole history.
 *
 * '''Schema evolution:''' `spark.databricks.delta.schema.autoMerge.enabled=true`
 * lets producers add new fields without a manual `ALTER TABLE` — the MERGE widens
 * the table schema to accommodate the incoming columns.
 *
 * '''Maintenance:''' streaming writes create the classic small-files problem.
 * When enabled, every N committed batches we run `OPTIMIZE` (compaction, with an
 * optional `ZORDER BY` for data-skipping on point lookups) and `VACUUM` to reclaim
 * space from tombstoned files.
 */
class DeltaUpsertSink(
    spark: SparkSession,
    storage: StorageConfig,
    maintenance: MaintenanceConfig = MaintenanceConfig.default
) extends StrictLogging {

  private val tablePath = storage.tablePath

  // Number of successfully committed batches, used to schedule maintenance.
  private val committedBatches = new AtomicLong(0L)

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
      val rowCount = batch.count()
      val target   = DeltaTable.forPath(spark, tablePath)
      val condition = mergeConditionFor(batch)

      logger.info(
        s"Merging batchId=$batchId ($rowCount unique rows) into $tablePath " +
          s"[$condition]"
      )
      PipelineMetrics.recordUpsert(rowCount)

      target
        .as("target")
        .merge(batch.as("source"), condition)
        // Exactly-once: an event we've already stored is NOT re-inserted, and
        // we deliberately do not update it (events are immutable facts).
        .whenNotMatched()
        .insertAll()
        .execute()

      maybeRunMaintenance(batchId)
    } finally {
      batch.unpersist()
    }
  }

  /**
   * Build the MERGE `ON` predicate for this batch. Always matches on the
   * business key; when the batch is non-empty and carries the `eventDate`
   * partition column, additionally bound the search to the batch's date range
   * so Delta prunes all other partitions.
   */
  private[sink] def mergeConditionFor(batch: DataFrame): String = {
    val base = EventTransforms.mergeCondition
    if (!batch.columns.contains("eventDate")) return base

    val bounds = batch.agg(
      sqlMin("eventDate").as("minDate"),
      sqlMax("eventDate").as("maxDate")
    )
    val row     = bounds.collect().headOption
    val minDate = row.flatMap(r => Option(r.get(0))).map(_.toString)
    val maxDate = row.flatMap(r => Option(r.get(1))).map(_.toString)

    (minDate, maxDate) match {
      case (Some(lo), Some(hi)) =>
        // eventDate is a DATE column; literal comparison prunes partitions.
        s"$base AND target.eventDate BETWEEN date'$lo' AND date'$hi'"
      case _ =>
        // Empty batch: no rows to prune against, keep the plain key match.
        base
    }
  }

  /**
   * Periodically compact the table (OPTIMIZE, optionally ZORDER) and reclaim
   * space (VACUUM). No-op unless maintenance is enabled and this batch lands on
   * the configured cadence.
   */
  private[sink] def maybeRunMaintenance(batchId: Long): Unit = {
    if (!maintenance.enabled || maintenance.everyBatches <= 0) return
    val n = committedBatches.incrementAndGet()
    if (n % maintenance.everyBatches != 0L) return
    logger.info(s"Maintenance cadence hit after batchId=$batchId ($n committed batches)")
    runMaintenance()
  }

  /** Run OPTIMIZE (+ ZORDER) and VACUUM immediately. Exposed for tools/tests. */
  def runMaintenance(): Unit = {
    // Backtick-quoted identifier: escape any embedded backtick by doubling it.
    val sqlPath = tablePath.replace("`", "``")
    val zorder =
      Option(maintenance.zorderColumn).filter(_.nonEmpty) match {
        case Some(col) => s" ZORDER BY ($col)"
        case None      => ""
      }
    logger.info(s"Running OPTIMIZE$zorder on $tablePath")
    spark.sql(s"OPTIMIZE delta.`$sqlPath`$zorder")

    // Allow sub-week retention for demos/tests; safe because the streaming
    // checkpoint, not old data files, is the source of recovery truth.
    val previous =
      spark.conf.getOption("spark.databricks.delta.retentionDurationCheck.enabled")
    try {
      if (maintenance.vacuumRetentionHours < 168) {
        spark.conf
          .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      }
      logger.info(
        s"Running VACUUM RETAIN ${maintenance.vacuumRetentionHours} HOURS on $tablePath"
      )
      spark.sql(
        s"VACUUM delta.`$sqlPath` RETAIN ${maintenance.vacuumRetentionHours} HOURS"
      )
    } finally {
      previous match {
        case Some(v) =>
          spark.conf
            .set("spark.databricks.delta.retentionDurationCheck.enabled", v)
        case None =>
          spark.conf
            .unset("spark.databricks.delta.retentionDurationCheck.enabled")
      }
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
