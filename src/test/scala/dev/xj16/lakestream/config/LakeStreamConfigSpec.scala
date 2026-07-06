package dev.xj16.lakestream.config

import org.scalatest.funsuite.AnyFunSuite

/** Config parsing / derivation tests (no Spark needed). */
class LakeStreamConfigSpec extends AnyFunSuite {

  private val hocon =
    """
      |kafka {
      |  bootstrap-servers = "kafka:9092"
      |  topic = "events"
      |  starting-offsets = "earliest"
      |  max-offsets-per-trigger = 5000
      |  fail-on-data-loss = false
      |}
      |storage {
      |  base-path = "s3a://bucket/delta"
      |  table-name = "events"
      |  checkpoint-path = "s3a://bucket/_ckpt"
      |  endpoint = "http://minio:9000"
      |  access-key = "ak"
      |  secret-key = "sk"
      |  path-style-access = true
      |}
      |stream {
      |  trigger-interval = "5 seconds"
      |  output-mode = "append"
      |  shuffle-partitions = 4
      |  once = false
      |}
      |""".stripMargin

  test("loads a full config from HOCON") {
    val cfg = LakeStreamConfig.fromString(hocon)
    assert(cfg.kafka.bootstrapServers == "kafka:9092")
    assert(cfg.kafka.maxOffsetsPerTrigger == 5000L)
    assert(cfg.stream.shufflePartitions == 4)
  }

  test("tablePath and checkpointFor derive correct paths") {
    val cfg = LakeStreamConfig.fromString(hocon)
    assert(cfg.storage.tablePath == "s3a://bucket/delta/events")
    assert(
      cfg.storage.checkpointFor("events") == "s3a://bucket/_ckpt/events"
    )
  }

  test("trailing slashes in base paths are normalised") {
    val cfg = LakeStreamConfig.fromString(
      hocon.replace(
        """base-path = "s3a://bucket/delta"""",
        """base-path = "s3a://bucket/delta/""""
      )
    )
    assert(cfg.storage.tablePath == "s3a://bucket/delta/events")
  }
}
