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
    stream: StreamConfig,
    dlq: DlqConfig = DlqConfig.default,
    maintenance: MaintenanceConfig = MaintenanceConfig.default,
    metrics: MetricsConfig = MetricsConfig.default
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

/**
 * Dead-letter-queue settings. Records that fail to parse (no valid business
 * key) are, when enabled, written to a second Delta table with the raw Kafka
 * value plus partition/offset/error context instead of being silently dropped.
 */
final case class DlqConfig(
    enabled: Boolean,
    // Table name (under the same storage `basePath`) for rejected records.
    tableName: String
)

object DlqConfig {
  val default: DlqConfig = DlqConfig(enabled = false, tableName = "events_dlq")
}

/**
 * Delta table maintenance. Streaming writes create many small files; running
 * OPTIMIZE (with an optional ZORDER) compacts them and VACUUM reclaims space
 * from tombstoned files. Maintenance runs every `everyBatches` micro-batches
 * so it amortises over the stream instead of needing a separate cron job.
 */
final case class MaintenanceConfig(
    enabled: Boolean,
    // Run OPTIMIZE/VACUUM once every N committed micro-batches (0 disables).
    everyBatches: Int,
    // Column to ZORDER by during OPTIMIZE for data-skipping on point lookups.
    zorderColumn: String,
    // VACUUM retention in hours; Delta's floor is 168 (7 days) unless the
    // retention-duration check is disabled.
    vacuumRetentionHours: Int
)

object MaintenanceConfig {
  val default: MaintenanceConfig = MaintenanceConfig(
    enabled = false,
    everyBatches = 50,
    zorderColumn = "eventId",
    vacuumRetentionHours = 168
  )
}

/**
 * Observability: an embedded HTTP server exposing `/healthz` (liveness),
 * `/readyz` (readiness — is the streaming query active?), and `/metrics`
 * (Prometheus text format) so k8s probes and a Prometheus scraper work
 * without pulling in a heavy metrics framework.
 */
final case class MetricsConfig(
    enabled: Boolean,
    // TCP port for the embedded metrics/health server.
    port: Int
)

object MetricsConfig {
  val default: MetricsConfig = MetricsConfig(enabled = false, port = 9464)
}

object LakeStreamConfig {

  /** Load config from the classpath (`application.conf` / `reference.conf`). */
  def load(): LakeStreamConfig =
    ConfigSource.default.loadOrThrow[LakeStreamConfig]

  /** Load config from an explicit HOCON string (used in tests). */
  def fromString(hocon: String): LakeStreamConfig =
    ConfigSource.string(hocon).loadOrThrow[LakeStreamConfig]
}
