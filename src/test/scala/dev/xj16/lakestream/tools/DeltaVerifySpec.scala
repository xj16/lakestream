package dev.xj16.lakestream.tools

import dev.xj16.lakestream.SparkTestBase
import dev.xj16.lakestream.config.StorageConfig
import dev.xj16.lakestream.sink.DeltaUpsertSink
import dev.xj16.lakestream.transform.EventTransforms
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/** Verifies the DeltaVerify exactly-once assertion tool. */
class DeltaVerifySpec extends SparkTestBase {

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

  private def json(id: String): String =
    s"""{"eventId":"$id","userId":"u","eventType":"click","timestamp":1720000000000}"""

  test("verify PASSes when the table holds each id exactly once") {
    val storage = storageAt(s"$tmpDir/verify-pass")
    val sink    = new DeltaUpsertSink(spark, storage)

    // Feed duplicates and a replay; exactly-once should keep the table clean.
    val batch = EventTransforms.clickstreamPipeline(
      rawFrom(Seq(json("a"), json("a"), json("b")))
    )
    sink.upsertBatch(batch, 0L)
    sink.upsertBatch(batch, 0L) // replay

    val result = DeltaVerify.verify(spark, storage.tablePath)
    assert(result.pass)
    assert(result.total == 2)
    assert(result.distinct == 2)
  }

  test("verify FAILs when duplicate ids leak into the table") {
    val storage = storageAt(s"$tmpDir/verify-fail")

    // Bypass the MERGE and APPEND duplicates directly, simulating a naive
    // (broken) pipeline, so the verifier has something to catch.
    val df = EventTransforms.clickstreamPipeline(rawFrom(Seq(json("dup"))))
    df.write.format("delta").mode("overwrite").save(storage.tablePath)
    df.write.format("delta").mode("append").save(storage.tablePath)

    val result = DeltaVerify.verify(spark, storage.tablePath)
    assert(!result.pass)
    assert(result.total == 2)
    assert(result.distinct == 1)
  }
}
