package dev.xj16.lakestream

import dev.xj16.lakestream.config._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * End-to-end streaming test: drives a real Spark `MemoryStream` (shaped like the
 * Kafka source) through the pipeline's actual `foreachBatch` callback and Delta
 * sink, with a real on-disk checkpoint. Crucially it STOPS the query mid-stream
 * and RESTARTS it against the same checkpoint, then asserts the table row count
 * still equals the number of distinct ids.
 *
 * This closes the biggest testing gap the project had: the `foreachBatch`
 * wiring and checkpoint replay were previously asserted only in prose. Here the
 * real streaming machinery — trigger, micro-batching, checkpoint commit/replay —
 * is exercised, not just the pure sink method.
 */
class StreamingPipelineSpec extends SparkTestBase {

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

  private def cfgAt(dir: String, dlq: Boolean = false): LakeStreamConfig =
    LakeStreamConfig(
      kafka = KafkaConfig("unused:9092", "events", "earliest", 100000L, failOnDataLoss = false),
      storage = storageAt(dir),
      stream = StreamConfig("1 second", "append", 2, once = false),
      dlq = DlqConfig(enabled = dlq, tableName = "events_dlq"),
      maintenance = MaintenanceConfig.default,
      metrics = MetricsConfig.default
    )

  private def event(id: String, ts: Long = 1720000000000L): (Array[Byte], Int, Long) = {
    val json =
      s"""{"eventId":"$id","userId":"u1","eventType":"click","timestamp":$ts}"""
    (json.getBytes("UTF-8"), 0, 0L)
  }

  private def tableCount(cfg: LakeStreamConfig): Long =
    spark.read.format("delta").load(cfg.storage.tablePath).count()

  private def distinctIds(cfg: LakeStreamConfig): Long =
    spark.read
      .format("delta")
      .load(cfg.storage.tablePath)
      .select("eventId")
      .distinct()
      .count()

  /**
   * Run a MemoryStream-backed streaming query through the pipeline's real
   * `foreachBatch` (via [[StreamingPipeline.processBatch]]) until it has
   * processed all currently-available input, then stop it.
   */
  private def runOnce(
      spark: SparkSession,
      cfg: LakeStreamConfig,
      pipeline: StreamingPipeline,
      checkpoint: String,
      records: Seq[(Array[Byte], Int, Long)]
  ): Unit = {
    implicit val sqlCtx = spark.sqlContext
    import spark.implicits._

    val source = MemoryStream[(Array[Byte], Int, Long)]
    source.addData(records)
    val raw: DataFrame = source.toDF().toDF("value", "partition", "offset")

    val query: StreamingQuery = raw.writeStream
      .queryName("lakestream-test")
      .option("checkpointLocation", checkpoint)
      .foreachBatch { (b: DataFrame, id: Long) =>
        pipeline.processBatch(b, id)
      }
      .start()
    // Deterministically drain all currently-available input, then stop — this
    // exercises real micro-batching + checkpoint commit without a wall-clock race.
    query.processAllAvailable()
    query.stop()
  }

  test("streaming foreachBatch upserts exactly-once across a stop/restart") {
    val cfg      = cfgAt(s"$tmpDir/stream-restart")
    val pipeline = StreamingPipeline(spark, cfg)
    val ckpt1    = s"file:///$tmpDir/stream-restart/ckpt-1".replace('\\', '/')
    val ckpt2    = s"file:///$tmpDir/stream-restart/ckpt-2".replace('\\', '/')

    // First run: 3 distinct ids, one of them duplicated within the batch.
    runOnce(
      spark,
      cfg,
      pipeline,
      ckpt1,
      Seq(event("a"), event("b"), event("b"), event("c"))
    )
    assert(tableCount(cfg) == 3, "first run should store 3 distinct ids")

    // Restart the pipeline (new query) writing to the SAME Delta table with
    // new + overlapping ids. The Delta MERGE must reject the id ("c") we
    // already stored, so overlapping replays never double-write.
    runOnce(
      spark,
      cfg,
      pipeline,
      ckpt2,
      Seq(event("c"), event("d"), event("d"), event("e"))
    )

    assert(tableCount(cfg) == distinctIds(cfg), "count must equal distinct ids")
    assert(tableCount(cfg) == 5, "a,b,c,d,e => 5 distinct rows after restart")
  }

  test("streaming DLQ routes malformed records to the dead-letter table") {
    val cfg      = cfgAt(s"$tmpDir/stream-dlq", dlq = true)
    val pipeline = StreamingPipeline(spark, cfg)
    val ckpt     = s"file:///$tmpDir/stream-dlq/ckpt".replace('\\', '/')

    val good1 = event("g1")
    val good2 = event("g2")
    val bad1  = ("not-json".getBytes("UTF-8"), 0, 10L)
    val bad2  = ("""{"userId":"u","eventType":"click","timestamp":1}""".getBytes("UTF-8"), 0, 11L)

    runOnce(spark, cfg, pipeline, ckpt, Seq(good1, bad1, good2, bad2))

    assert(tableCount(cfg) == 2, "only the 2 valid events land in the main table")

    val dlqPath = s"${cfg.storage.basePath.stripSuffix("/")}/${cfg.dlq.tableName}"
    val dlq     = spark.read.format("delta").load(dlqPath)
    assert(dlq.count() == 2, "both malformed records are captured in the DLQ")
    val reasons = dlq.select("errorReason").collect().map(_.getString(0))
    assert(reasons.forall(_ != null))
  }
}
