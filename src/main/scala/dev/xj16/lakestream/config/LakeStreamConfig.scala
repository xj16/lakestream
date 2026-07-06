package dev.xj16.lakestream.config

import pureconfig._
import pureconfig.generic.auto._

/**
 * Typed configuration for the whole pipeline, loaded from `application.conf`
 * (Typesafe/HOCON) and overridable via environment variables — see
 * `reference.conf` for the default wiring.
 */
final case class LakeStreamConfig(
    kafka: KafkaConfig,
    storage: StorageConfig,
    stream: StreamConfig
)

/** Kafka source settings for the Structured Streaming reader. */
final case class KafkaConfig(
    bootstrapServers: String,
    topic: String,
    startingOffsets: String,
    // Bound per-trigger work so a backlog cannot overwhelm one micro-batch.
    maxOffsetsPerTrigger: Long,
    // "true" only for local dev where Kafka retention may have moved on.
    failOnDataLoss: Boolean
)

/**
 * Where the Delta lakehouse tables live. `basePath` is typically an
 * `s3a://` URI (MinIO in dev, real S3 in prod) or a local path for tests.
 */
final case class StorageConfig(
    basePath: String,
    tableName: String,
    checkpointPath: String,
    // S3A / MinIO endpoint config; empty endpoint => real AWS S3.
    // Field names avoid a digit boundary so PureConfig's kebab-case mapping
    // yields clean keys (`endpoint`, `access-key`, ...) under `storage`.
    endpoint: String,
    accessKey: String,
    secretKey: String,
    pathStyleAccess: Boolean
) {
  def tablePath: String      = s"${basePath.stripSuffix("/")}/$tableName"
  def checkpointFor(name: String): String =
    s"${checkpointPath.stripSuffix("/")}/$name"
}

/** Runtime tuning for the streaming query. */
final case class StreamConfig(
    // Structured Streaming trigger interval, e.g. "10 seconds".
    triggerInterval: String,
    // Micro-batch output mode (append for an immutable event log).
    outputMode: String,
    // Number of shuffle partitions — keep small for local runs.
    shufflePartitions: Int,
    // When true, run one micro-batch and stop (used by CI / smoke tests).
    once: Boolean
)

object LakeStreamConfig {

  /** Load config from the classpath (`application.conf` / `reference.conf`). */
  def load(): LakeStreamConfig =
    ConfigSource.default.loadOrThrow[LakeStreamConfig]

  /** Load config from an explicit HOCON string (used in tests). */
  def fromString(hocon: String): LakeStreamConfig =
    ConfigSource.string(hocon).loadOrThrow[LakeStreamConfig]
}
