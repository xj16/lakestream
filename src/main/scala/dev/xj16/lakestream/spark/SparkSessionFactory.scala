package dev.xj16.lakestream.spark

import dev.xj16.lakestream.config.StorageConfig
import org.apache.spark.sql.SparkSession

/**
 * Builds a [[SparkSession]] wired for Delta Lake and (optionally) S3A/MinIO.
 *
 * The Delta SQL extensions and catalog are required for `MERGE INTO` and
 * schema evolution to work through the DataFrame/SQL API.
 */
object SparkSessionFactory {

  /**
   * @param appName   Spark application name.
   * @param storage   storage config; if `s3Endpoint` is set, S3A is configured
   *                  for MinIO-style path-access.
   * @param master    optional master override (used by tests: "local[2]").
   * @param shufflePartitions keep small for local / test runs.
   */
  def build(
      appName: String,
      storage: StorageConfig,
      shufflePartitions: Int,
      master: Option[String] = None
  ): SparkSession = {
    var builder = SparkSession
      .builder()
      .appName(appName)
      // Delta Lake integration.
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      // Deterministic, backlog-safe streaming.
      .config("spark.sql.streaming.schemaInference", "false")

    master.foreach(m => builder = builder.master(m))

    val spark = builder.getOrCreate()

    // Configure S3A only when a MinIO/S3 endpoint is provided. For local FS
    // paths (tests, single-node dev) this block is skipped entirely.
    if (storage.endpoint.nonEmpty) {
      val hc = spark.sparkContext.hadoopConfiguration
      hc.set("fs.s3a.endpoint", storage.endpoint)
      hc.set("fs.s3a.access.key", storage.accessKey)
      hc.set("fs.s3a.secret.key", storage.secretKey)
      hc.set(
        "fs.s3a.path.style.access",
        storage.pathStyleAccess.toString
      )
      hc.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
      )
      // MinIO uses plain HTTP in dev; SSL is turned off explicitly.
      hc.set("fs.s3a.connection.ssl.enabled", "false")
      hc.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      )
    }

    spark
  }
}
