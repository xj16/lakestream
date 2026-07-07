package dev.xj16.lakestream.tools

import dev.xj16.lakestream.config.{LakeStreamConfig, MaintenanceConfig}
import dev.xj16.lakestream.sink.DeltaUpsertSink
import dev.xj16.lakestream.spark.SparkSessionFactory

/**
 * One-shot Delta table maintenance: OPTIMIZE (+ ZORDER) and VACUUM.
 *
 * Streaming writes create many small files; run this periodically (a cron Job,
 * or after a demo load) to compact them and reclaim space. It reuses the same
 * maintenance routine the streaming sink runs inline, driven by the same config.
 *
 * Run:
 *   java -cp lakestream-assembly-0.1.0.jar dev.xj16.lakestream.tools.DeltaMaintenance
 */
object DeltaMaintenance {

  def main(args: Array[String]): Unit = {
    val cfg = LakeStreamConfig.load()
    val spark = SparkSessionFactory.build(
      appName = "LakeStreamMaintenance",
      storage = cfg.storage,
      shufflePartitions = cfg.stream.shufflePartitions
    )
    spark.sparkContext.setLogLevel("WARN")
    try {
      // Force maintenance on regardless of the streaming toggle when invoked
      // explicitly as a tool.
      val maint: MaintenanceConfig = cfg.maintenance.copy(enabled = true)
      val sink = new DeltaUpsertSink(spark, cfg.storage, maint)
      sink.ensureTable()
      sink.runMaintenance()
      println(s"Maintenance complete on ${cfg.storage.tablePath}")
    } finally {
      spark.stop()
    }
  }
}
