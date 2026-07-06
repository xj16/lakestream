package dev.xj16.lakestream.sink

import dev.xj16.lakestream.SparkTestBase
import dev.xj16.lakestream.config.StorageConfig
import dev.xj16.lakestream.transform.EventTransforms
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
 * Proves the two guarantees that make LakeStream "exactly-once":
 *   1. Re-running the same batch (same batchId) does NOT duplicate rows.
 *   2. New producer fields flow in via Delta schema evolution.
 *
 * The whole thing runs against a local-filesystem Delta table — no Kafka,
 * no MinIO — so it is fully reproducible in CI.
 */
class DeltaUpsertSinkSpec extends SparkTestBase {

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
      s3Endpoint = "",
      s3AccessKey = "",
      s3SecretKey = "",
      s3PathStyleAccess = false
    )

  private def tableCount(storage: StorageConfig): Long =
    spark.read.format("delta").load(storage.tablePath).count()

  test("upsertBatch inserts unique events") {
    val storage = storageAt(s"$tmpDir/insert")
    val sink    = new DeltaUpsertSink(spark, storage)

    val batch = EventTransforms.clickstreamPipeline(
      rawFrom(
        Seq(
          """{"eventId":"a","userId":"u1","eventType":"click","timestamp":1720000000000}""",
          """{"eventId":"b","userId":"u2","eventType":"click","timestamp":1720000000000}"""
        )
      )
    )
    sink.upsertBatch(batch, 0L)
    assert(tableCount(storage) == 2)
  }

  test("re-processing the same batch is idempotent (exactly-once)") {
    val storage = storageAt(s"$tmpDir/idempotent")
    val sink    = new DeltaUpsertSink(spark, storage)

    val json = Seq(
      """{"eventId":"x","userId":"u1","eventType":"click","timestamp":1720000000000}""",
      """{"eventId":"y","userId":"u2","eventType":"click","timestamp":1720000000000}"""
    )
    val batch = EventTransforms.clickstreamPipeline(rawFrom(json))

    // Simulate Spark replaying the same micro-batch after a failure.
    sink.upsertBatch(batch, 7L)
    sink.upsertBatch(batch, 7L)
    sink.upsertBatch(batch, 7L)

    // Still exactly 2 rows — the MERGE did not re-insert.
    assert(tableCount(storage) == 2)
  }

  test("overlapping batches only insert genuinely new ids") {
    val storage = storageAt(s"$tmpDir/overlap")
    val sink    = new DeltaUpsertSink(spark, storage)

    val batch1 = EventTransforms.clickstreamPipeline(
      rawFrom(
        Seq(
          """{"eventId":"1","userId":"u","eventType":"click","timestamp":1720000000000}""",
          """{"eventId":"2","userId":"u","eventType":"click","timestamp":1720000000000}"""
        )
      )
    )
    val batch2 = EventTransforms.clickstreamPipeline(
      rawFrom(
        Seq(
          // "2" overlaps batch1 and must NOT be re-inserted.
          """{"eventId":"2","userId":"u","eventType":"click","timestamp":1720000000000}""",
          """{"eventId":"3","userId":"u","eventType":"click","timestamp":1720000000000}"""
        )
      )
    )
    sink.upsertBatch(batch1, 0L)
    sink.upsertBatch(batch2, 1L)

    val ids = spark.read
      .format("delta")
      .load(storage.tablePath)
      .select("eventId")
      .collect()
      .map(_.getString(0))
      .toSet
    assert(ids == Set("1", "2", "3"))
  }

  test("schema evolution absorbs a new producer field") {
    val storage = storageAt(s"$tmpDir/evolve")
    val sink    = new DeltaUpsertSink(spark, storage)

    // Seed with the base schema.
    sink.upsertBatch(
      EventTransforms.clickstreamPipeline(
        rawFrom(
          Seq(
            """{"eventId":"e1","userId":"u1","eventType":"click","timestamp":1720000000000}"""
          )
        )
      ),
      0L
    )

    // A later batch carries a brand-new "channel" column. We hand-build a
    // DataFrame that includes it and MERGE with autoMerge on.
    import org.apache.spark.sql.functions._
    val evolved = EventTransforms
      .clickstreamPipeline(
        rawFrom(
          Seq(
            """{"eventId":"e2","userId":"u2","eventType":"click","timestamp":1720000000000}"""
          )
        )
      )
      .withColumn("channel", lit("mobile"))

    sink.upsertBatch(evolved, 1L)

    val df = spark.read.format("delta").load(storage.tablePath)
    // The column now exists on the table (widened schema).
    assert(df.schema.fieldNames.contains("channel"))
    assert(df.count() == 2)
    // Old row has null for the new column; new row has the value.
    val channelForE2 = df
      .where(col("eventId") === "e2")
      .select("channel")
      .collect()
      .head
      .getString(0)
    assert(channelForE2 == "mobile")
  }
}
