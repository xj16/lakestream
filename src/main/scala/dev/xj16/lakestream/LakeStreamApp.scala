package dev.xj16.lakestream

import dev.xj16.lakestream.config.LakeStreamConfig
import dev.xj16.lakestream.spark.SparkSessionFactory
import com.typesafe.scalalogging.StrictLogging

/**
 * Production entrypoint for the LakeStream streaming lakehouse.
 *
 * Config is loaded from the classpath (`application.conf` layered over
 * `reference.conf`), so the same JAR runs unchanged in local dev, Docker
 * Compose, and Kubernetes — only the config/env differs.
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

    try {
      StreamingPipeline(spark, cfg).run()
    } finally {
      spark.stop()
    }
  }
}
