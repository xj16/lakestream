package dev.xj16.lakestream.sink

import dev.xj16.lakestream.SparkTestBase
import dev.xj16.lakestream.config.{MaintenanceConfig, StorageConfig}
import dev.xj16.lakestream.transform.EventTransforms
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
 * Covers the partition-pruned MERGE condition and the OPTIMIZE/VACUUM
 * maintenance path — the perf/prod-shape work the sink gained.
 */
class DeltaMaintenanceSpec extends SparkTestBase {

  private val kafkaSchema = StructType(
    Seq(
      StructField("value", BinaryType),
      StructField("partition", IntegerType),
      StructField("offset", LongType)
    )
  )

  private def rawFrom(jsons: Seq[String]): DataFrame = {
    val rows = jsons.map(j => Row(j.getBytes("UTF-8"), 0, 0L))
    spark.createDataFrame(spark.sparkContext.parallelize(rows), kafkaSchema)
  }

  private def storageAt(dir: String): StorageConfig =
    StorageConfig(
      basePath = s"file:///${dir.replace('\\', '/')}/delta",
      tableName = "events",
      checkpointPath = s"file:///${dir.replace('\\', '/')}/ckpt",
      endpoint = "",
      accessKey = "",
      secretKey = "",
      pathStyleAccess = false
    )

  private def eventJson(id: String, ts: Long): String =
    s"""{"eventId":"$id","userId":"u1","eventType":"click","timestamp":$ts}"""

  test("mergeConditionFor bounds the merge to the batch's eventDate window") {
    val storage = storageAt(s"$tmpDir/prune")
    val sink    = new DeltaUpsertSink(spark, storage)

    // Two events on two different dates (2024-07-03 and 2024-07-04).
    val batch = EventTransforms.clickstreamPipeline(
      rawFrom(
        Seq(
          eventJson("a", 1720000000000L), // 2024-07-03
          eventJson("b", 1720100000000L)  // 2024-07-04
        )
      )
    )
    val cond = sink.mergeConditionFor(batch)
    assert(cond.contains("target.eventId = source.eventId"))
    assert(cond.contains("target.eventDate BETWEEN date'2024-07-03' AND date'2024-07-04'"))
  }

  test("mergeConditionFor falls back to key-only match on an empty batch") {
    val storage = storageAt(s"$tmpDir/prune-empty")
    val sink    = new DeltaUpsertSink(spark, storage)
    val empty   = EventTransforms.clickstreamPipeline(rawFrom(Seq.empty))
    assert(sink.mergeConditionFor(empty) == EventTransforms.mergeCondition)
  }

  test("partition-pruned upsert still yields exactly-once results") {
    val storage = storageAt(s"$tmpDir/prune-upsert")
    val sink    = new DeltaUpsertSink(spark, storage)

    val b1 = EventTransforms.clickstreamPipeline(
      rawFrom(Seq(eventJson("a", 1720000000000L), eventJson("b", 1720100000000L)))
    )
    sink.upsertBatch(b1, 0L)
    // Replays + an overlap "b" plus a new "c" on a third date.
    val b2 = EventTransforms.clickstreamPipeline(
      rawFrom(Seq(eventJson("b", 1720100000000L), eventJson("c", 1720200000000L)))
    )
    sink.upsertBatch(b2, 1L)
    sink.upsertBatch(b2, 1L) // replay

    val df = spark.read.format("delta").load(storage.tablePath)
    assert(df.count() == 3)
    assert(
      df.select("eventId").collect().map(_.getString(0)).toSet == Set("a", "b", "c")
    )
  }

  test("runMaintenance compacts without changing row count or dropping ids") {
    val storage = storageAt(s"$tmpDir/maint")
    val maint = MaintenanceConfig(
      enabled = true,
      everyBatches = 1,
      zorderColumn = "eventId",
      vacuumRetentionHours = 0
    )
    val sink = new DeltaUpsertSink(spark, storage, maint)

    // Several small batches create several small files.
    (0 until 5).foreach { i =>
      sink.upsertBatch(
        EventTransforms.clickstreamPipeline(
          rawFrom(Seq(eventJson(s"id-$i", 1720000000000L + i)))
        ),
        i.toLong
      )
    }
    val before = spark.read.format("delta").load(storage.tablePath).count()

    // Explicit maintenance run: OPTIMIZE (+ ZORDER) then VACUUM RETAIN 0 HOURS.
    sink.runMaintenance()

    val df = spark.read.format("delta").load(storage.tablePath)
    assert(df.count() == before, "maintenance must not lose or add rows")
    assert(df.count() == 5)
  }
}
