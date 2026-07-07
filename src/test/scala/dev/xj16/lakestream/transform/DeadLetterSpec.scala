package dev.xj16.lakestream.transform

import dev.xj16.lakestream.SparkTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Verifies the dead-letter split: valid events flow through `clickstreamPipeline`
 * while malformed / key-less records are captured by `deadLetters` with their
 * Kafka coordinates. The two are exact complements — no record is both kept and
 * dead-lettered, and none is silently lost.
 */
class DeadLetterSpec extends SparkTestBase {

  private val kafkaSchema = StructType(
    Seq(
      StructField("value", BinaryType),
      StructField("partition", IntegerType),
      StructField("offset", LongType)
    )
  )

  private def rawFrom(rows: Seq[(String, Int, Long)]) =
    spark.createDataFrame(
      spark.sparkContext.parallelize(
        rows.map { case (j, p, o) => Row(j.getBytes("UTF-8"), p, o) }
      ),
      kafkaSchema
    )

  test("deadLetters captures unparseable and key-less records only") {
    val raw = rawFrom(
      Seq(
        ("""{"eventId":"ok","userId":"u","eventType":"click","timestamp":1}""", 0, 1L),
        ("not json", 0, 2L),
        ("""{"userId":"u","eventType":"click","timestamp":1}""", 1, 3L) // missing eventId
      )
    )

    val valid = EventTransforms.clickstreamPipeline(raw)
    val dead  = EventTransforms.deadLetters(raw)

    assert(valid.count() == 1)
    assert(valid.select("eventId").collect().head.getString(0) == "ok")

    val deadRows = dead.collect()
    assert(deadRows.length == 2)

    val offsets = deadRows.map(_.getAs[Long]("kafkaOffset")).toSet
    assert(offsets == Set(2L, 3L))
    // Every dead letter has an error reason and preserves the raw payload.
    assert(deadRows.forall(r => r.getAs[String]("errorReason") != null))
    assert(deadRows.forall(r => r.getAs[String]("rawValue") != null))
  }

  test("deadLetters is empty when every record is valid") {
    val raw = rawFrom(
      Seq(
        ("""{"eventId":"a","userId":"u","eventType":"click","timestamp":1}""", 0, 1L),
        ("""{"eventId":"b","userId":"u","eventType":"view","timestamp":2}""", 0, 2L)
      )
    )
    assert(EventTransforms.deadLetters(raw).count() == 0)
  }
}
