package dev.xj16.lakestream.source

import dev.xj16.lakestream.config.KafkaConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Builds the Structured Streaming reader for the Kafka source. */
object KafkaSource {

  /**
   * Read the configured Kafka topic as an unbounded streaming DataFrame.
   *
   * `maxOffsetsPerTrigger` caps how much of a backlog a single micro-batch may
   * pull, which keeps recovery batches bounded and predictable — important for
   * the exactly-once MERGE downstream.
   */
  def read(spark: SparkSession, cfg: KafkaConfig): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.topic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("maxOffsetsPerTrigger", cfg.maxOffsetsPerTrigger.toString)
      .option("failOnDataLoss", cfg.failOnDataLoss.toString)
      .load()
}
