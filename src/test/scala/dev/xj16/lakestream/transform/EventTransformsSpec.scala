package dev.xj16.lakestream.transform

import dev.xj16.lakestream.SparkTestBase
import dev.xj16.lakestream.schema.EventSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** Unit tests for the pure DataFrame transforms. */
class EventTransformsSpec extends SparkTestBase {

  private def kafkaRow(json: String): Row =
    Row(json.getBytes("UTF-8"), 0, 0L)

  private val kafkaSchema = StructType(
    Seq(
      StructField("value", BinaryType),
      StructField("partition", IntegerType),
      StructField("offset", LongType)
    )
  )

  private def rawFrom(jsons: Seq[String]) = {
    val rows = jsons.map(kafkaRow)
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      kafkaSchema
    )
  }

  test("parseKafkaValue extracts canonical event fields from JSON") {
    val raw = rawFrom(
      Seq(
        """{"eventId":"e1","userId":"u1","eventType":"click","timestamp":1720000000000}"""
      )
    )
    val parsed = EventTransforms.parseKafkaValue(raw)
    val out    = parsed.collect()
    assert(out.length == 1)
    assert(out.head.getAs[String]("eventId") == "e1")
    assert(out.head.getAs[String]("eventType") == "click")
  }

  test("parseKafkaValue tolerates unknown extra fields (forward compat)") {
    // Producer added `sessionId` we don't yet model — must not break parsing.
    val raw = rawFrom(
      Seq(
        """{"eventId":"e2","userId":"u2","eventType":"page_view","timestamp":1720000000000,"sessionId":"s-99"}"""
      )
    )
    val parsed = EventTransforms.parseKafkaValue(raw).collect()
    assert(parsed.length == 1)
    assert(parsed.head.getAs[String]("eventId") == "e2")
  }

  test("parseKafkaValue drops records that are not valid JSON") {
    val raw    = rawFrom(Seq("not-json-at-all"))
    val parsed = EventTransforms.parseKafkaValue(raw).collect()
    assert(parsed.isEmpty)
  }

  test("enrich derives eventTime and eventDate from epoch millis") {
    val raw = rawFrom(
      Seq(
        """{"eventId":"e3","userId":"u3","eventType":"click","timestamp":1720000000000}"""
      )
    )
    val enriched = EventTransforms.enrich(EventTransforms.parseKafkaValue(raw))
    assert(
      enriched.schema.fieldNames.contains(EventSchema.eventTimeColumn)
    )
    assert(enriched.schema.fieldNames.contains("eventDate"))
    val row = enriched.collect().head
    assert(row.getAs[java.sql.Timestamp](EventSchema.eventTimeColumn) != null)
    assert(row.getAs[java.sql.Date]("eventDate") != null)
  }

  test("enrich drops rows missing the dedup id column") {
    val raw = rawFrom(
      Seq(
        """{"userId":"u4","eventType":"click","timestamp":1720000000000}"""
      )
    )
    val enriched = EventTransforms.enrich(EventTransforms.parseKafkaValue(raw))
    assert(enriched.count() == 0)
  }

  test("dedupeWithinBatch collapses duplicate ids in the same batch") {
    val raw = rawFrom(
      Seq(
        """{"eventId":"dup","userId":"u1","eventType":"click","timestamp":1720000000000}""",
        """{"eventId":"dup","userId":"u1","eventType":"click","timestamp":1720000000000}""",
        """{"eventId":"uniq","userId":"u2","eventType":"click","timestamp":1720000000000}"""
      )
    )
    val out = EventTransforms.clickstreamPipeline(raw)
    assert(out.count() == 2)
    assert(
      out.select("eventId").collect().map(_.getString(0)).toSet ==
        Set("dup", "uniq")
    )
  }
}
