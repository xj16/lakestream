package dev.xj16.lakestream

import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Shared local SparkSession for the test suite, wired with the Delta
 * extensions and a temp warehouse. Tests run against the local filesystem —
 * no Kafka or MinIO required — so the whole exactly-once path is exercised in
 * CI on a plain runner.
 */
trait SparkTestBase extends AnyFunSuite with BeforeAndAfterAll {

  @transient protected var spark: SparkSession = _
  protected lazy val tmpDir: String =
    Files.createTempDirectory("lakestream-test").toAbsolutePath.toString

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .appName("lakestream-test")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      // Pin the timezone so epoch-millis -> eventDate derivation is
      // deterministic regardless of the CI runner's local time.
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.warehouse.dir", s"$tmpDir/warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }
}
