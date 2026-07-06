package dev.xj16.lakestream.schema

import org.apache.spark.sql.types._

/**
 * Canonical schema for the events flowing through LakeStream.
 *
 * The pipeline reads JSON payloads off Kafka and parses them against this
 * schema. Fields added to producers over time are handled by Delta's schema
 * evolution (`mergeSchema`); this object is the "current known" shape used to
 * parse and to seed the table.
 */
object EventSchema {

  /**
   * v1 of the click-stream event. New optional fields (e.g. `sessionId`,
   * `experimentGroup`) can be appended by producers and will be absorbed via
   * `mergeSchema=true` on write without a manual migration.
   */
  val eventSchema: StructType = StructType(
    Seq(
      StructField("eventId", StringType, nullable = false),
      StructField("userId", StringType, nullable = false),
      StructField("eventType", StringType, nullable = false),
      StructField("url", StringType, nullable = true),
      StructField("referrer", StringType, nullable = true),
      StructField("amount", DoubleType, nullable = true),
      // Epoch millis as produced upstream; converted to a real timestamp.
      StructField("timestamp", LongType, nullable = false)
    )
  )

  /** Column that carries the exactly-once dedup key. */
  val idColumn: String = "eventId"

  /** Event-time column name after conversion from epoch millis. */
  val eventTimeColumn: String = "eventTime"

  /** Partition columns for the Delta table (date-partitioned event log). */
  val partitionColumns: Seq[String] = Seq("eventDate")
}
