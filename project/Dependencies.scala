import sbt._

/** Centralised dependency definitions so versions stay in one place. */
object Dependencies {

  val sparkVersion  = "3.5.1"
  val deltaVersion  = "3.2.0"
  val hadoopVersion = "3.3.4"

  // --- Spark ---
  val sparkSql       = "org.apache.spark" %% "spark-sql"       % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion

  // Kafka source for Structured Streaming (not Provided — must be on the classpath).
  val sparkSqlKafka  = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  // Plain Kafka client for the demo/synthetic event producer.
  val kafkaClients   = "org.apache.kafka" % "kafka-clients" % "3.5.1"

  // --- Delta Lake (open-source lakehouse table format) ---
  val deltaCore    = "io.delta" %% "delta-spark"    % deltaVersion
  val deltaStorage = "io.delta"  % "delta-storage"  % deltaVersion

  // --- S3A / MinIO access ---
  val hadoopAws    = "org.apache.hadoop" % "hadoop-aws"           % hadoopVersion
  val awsSdkBundle = "com.amazonaws"     % "aws-java-sdk-bundle"  % "1.12.262"

  // --- Config & logging ---
  val pureconfig   = "com.github.pureconfig" %% "pureconfig"     % "0.17.6"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  val logback      = "ch.qos.logback" % "logback-classic" % "1.4.14"

  // --- Test ---
  val scalaTest = "org.scalatest" %% "scalatest" % "3.2.18"
}
