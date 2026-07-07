package dev.xj16.lakestream

import dev.xj16.lakestream.config.LakeStreamConfig
import dev.xj16.lakestream.metrics.MetricsServer
import dev.xj16.lakestream.spark.SparkSessionFactory
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Production entrypoint for the LakeStream streaming lakehouse.
 *
 * Config is loaded from the classpath (`application.conf` layered over
 * `reference.conf`), so the same JAR runs unchanged in local dev, Docker
 * Compose, and Kubernetes — only the config/env differs.
 *
 * Lifecycle: it starts the streaming query (non-blocking), optionally starts the
 * embedded metrics/health server, installs a SIGTERM/SIGINT shutdown hook that
 * stops the query gracefully (so the current micro-batch commits its checkpoint
 * before the JVM exits), then blocks on the query.
 *
 * Run locally:
 *   spark-submit --class dev.xj16.lakestream.LakeStreamApp \
 *     target/scala-2.12/lakestream-assembly-0.1.0.jar
 */
object LakeStreamApp extends StrictLogging {

  def main(args: Array[String]): Unit = {
    val cfg = LakeStreamConfig.load()
    logger.info(s"Loaded config: kafka.topic=${cfg.kafka.topic}")

    val spark = SparkSessionFactory.build(
      appName = "LakeStream",
      storage = cfg.storage,
      shufflePartitions = cfg.stream.shufflePartitions
    )
    spark.sparkContext.setLogLevel("WARN")

    val metricsServer =
      if (cfg.metrics.enabled) {
        val s = new MetricsServer(cfg.metrics.port)
        s.start()
        Some(s)
      } else None

    val pipeline = StreamingPipeline(spark, cfg)
    var query: StreamingQuery = null

    // Graceful shutdown: on SIGTERM/SIGINT stop the query first so the active
    // micro-batch finishes and commits its offset checkpoint, THEN release
    // Spark and the metrics server. Without this, a `kubectl delete` mid-batch
    // could leave the query mid-flight.
    val shutdown = new Thread("lakestream-shutdown") {
      override def run(): Unit = {
        logger.info("Shutdown signal received; stopping streaming query gracefully")
        try if (query != null && query.isActive) query.stop()
        catch { case e: Throwable => logger.warn(s"query.stop() failed: $e") }
        pipeline.cleanup()
        metricsServer.foreach(_.stop())
        try spark.stop()
        catch { case e: Throwable => logger.warn(s"spark.stop() failed: $e") }
      }
    }
    Runtime.getRuntime.addShutdownHook(shutdown)

    try {
      query = pipeline.start()
      query.awaitTermination()
    } finally {
      // Normal termination (e.g. AvailableNow finished): tidy up. The shutdown
      // hook covers signal-driven exit; guard against double-stop there.
      try Runtime.getRuntime.removeShutdownHook(shutdown)
      catch { case _: IllegalStateException => () }
      pipeline.cleanup()
      metricsServer.foreach(_.stop())
      if (!spark.sparkContext.isStopped) spark.stop()
    }
  }
}
