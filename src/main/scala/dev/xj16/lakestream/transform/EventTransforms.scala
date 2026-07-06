package dev.xj16.lakestream.transform

import dev.xj16.lakestream.schema.EventSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/**
 * Pure DataFrame transformations for LakeStream.
 *
 * Each method takes a DataFrame and returns a new one — no side effects, no
 * Spark actions — which makes them trivially unit-testable against a local
 * SparkSession.
 */
object EventTransforms {

  /**
   * Parse the raw Kafka `value` (bytes) as JSON into the canonical event
   * columns, keeping Kafka metadata for observability.
   *
   * Input columns (from the Kafka source): `key`, `value`, `topic`,
   * `partition`, `offset`, `timestamp`.
   */
  def parseKafkaValue(raw: DataFrame): DataFrame = {
    val parsed = raw
      .select(
        col("partition").as("kafkaPartition"),
        col("offset").as("kafkaOffset"),
        // `from_json` with the fixed schema is null-tolerant: unknown fields
        // are ignored, missing optional fields become null.
        from_json(col("value").cast("string"), EventSchema.eventSchema)
          .as("payload")
      )
      .select(col("kafkaPartition"), col("kafkaOffset"), col("payload.*"))
      // Drop unparseable records. Note: for malformed input, Spark's PERMISSIVE
      // `from_json` yields a NON-null struct whose fields are all null (not a
      // null struct), so we filter on a required field rather than the struct
      // itself. A record with no `eventId` cannot be a valid event.
      .where(col(EventSchema.idColumn).isNotNull)

    parsed
  }

  /**
   * Derive the event-time timestamp and partition date from the epoch-millis
   * `timestamp` field, and drop malformed records missing the dedup key.
   */
  def enrich(parsed: DataFrame): DataFrame =
    parsed
      .where(col(EventSchema.idColumn).isNotNull)
      .withColumn(
        EventSchema.eventTimeColumn,
        (col("timestamp") / 1000L).cast("timestamp")
      )
      .withColumn(
        "eventDate",
        to_date(col(EventSchema.eventTimeColumn))
      )
      // Ingestion-time stamp for lineage / debugging.
      .withColumn("ingestTime", current_timestamp())

  /**
   * Within-batch deduplication on the business key. Combined with the
   * `MERGE INTO ... WHEN NOT MATCHED` write, this gives end-to-end
   * exactly-once semantics: a duplicate that arrives in the same micro-batch
   * is collapsed here, and one that arrives in a later batch is rejected by
   * the MERGE's match predicate.
   */
  def dedupeWithinBatch(enriched: DataFrame): DataFrame =
    enriched.dropDuplicates(EventSchema.idColumn)

  /** Full pull-through transform used by the streaming sink. */
  def clickstreamPipeline(raw: DataFrame): DataFrame =
    dedupeWithinBatch(enrich(parseKafkaValue(raw)))

  /** Predicate string for the exactly-once MERGE match condition. */
  val mergeCondition: String =
    s"target.${EventSchema.idColumn} = source.${EventSchema.idColumn}"

  /** Helper exposed for tests: the derived event-time column reference. */
  def eventTimeCol: Column = col(EventSchema.eventTimeColumn)
}
