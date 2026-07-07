package dev.xj16.lakestream.tools

import dev.xj16.lakestream.config.LakeStreamConfig
import dev.xj16.lakestream.spark.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, count => sqlCount}

/**
 * Programmatic proof of the core exactly-once guarantee.
 *
 * Reads the Delta events table and asserts that `count() == countDistinct(eventId)`.
 * If they match, every distinct event id is stored exactly once — duplicates and
 * replayed batches did NOT double-write — and it prints `PASS` and exits 0. If
 * they differ it prints `FAIL` and exits 1, so it doubles as a CI smoke gate.
 *
 * This replaces "eyeball the MinIO console file counts" with a one-command,
 * reproducible check. It also prints a small per-date / per-type breakdown so a
 * demo has something concrete to show.
 *
 * Run (after `sbt assembly`, against a running MinIO/S3 or a local path):
 *   java -cp lakestream-assembly-0.1.0.jar dev.xj16.lakestream.tools.DeltaVerify
 */
object DeltaVerify {

  /** Result of the exactly-once check, returned for tests. */
  final case class Result(total: Long, distinct: Long) {
    def pass: Boolean = total == distinct
  }

  /**
   * Core check against an already-built session and table path. Returns the
   * total vs. distinct counts and prints a human-readable report.
   */
  def verify(spark: SparkSession, tablePath: String): Result = {
    val df = spark.read.format("delta").load(tablePath)

    val agg = df
      .agg(
        sqlCount(col("eventId")).as("total"),
        countDistinct(col("eventId")).as("distinct")
      )
      .collect()
      .head
    val total    = agg.getLong(0)
    val distinct = agg.getLong(1)
    val result   = Result(total, distinct)

    // Concise report.
    println("== LakeStream exactly-once verification ==")
    println(s"table:            $tablePath")
    println(s"rows (count):     $total")
    println(s"distinct eventId: $distinct")

    if (df.columns.contains("eventDate")) {
      println("\nrows per eventDate:")
      df.groupBy("eventDate")
        .count()
        .orderBy("eventDate")
        .collect()
        .foreach(r => println(f"  ${r.get(0)}%-12s ${r.getLong(1)}%8d"))
    }
    if (df.columns.contains("eventType")) {
      println("\ntop eventTypes:")
      df.groupBy("eventType")
        .count()
        .orderBy(col("count").desc)
        .limit(10)
        .collect()
        .foreach(r => println(f"  ${r.get(0)}%-16s ${r.getLong(1)}%8d"))
    }

    println()
    if (result.pass) println(s"PASS: count == distinct ($total)")
    else
      println(
        s"FAIL: count ($total) != distinct ($distinct) — " +
          s"${total - distinct} duplicate row(s) leaked past the MERGE"
      )
    result
  }

  def main(args: Array[String]): Unit = {
    val cfg = LakeStreamConfig.load()
    val spark = SparkSessionFactory.build(
      appName = "LakeStreamVerify",
      storage = cfg.storage,
      shufflePartitions = cfg.stream.shufflePartitions
    )
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val result = verify(spark, cfg.storage.tablePath)
      if (!result.pass) sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}
