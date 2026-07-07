package dev.xj16.lakestream.metrics

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.{HttpURLConnection, ServerSocket, URL}
import java.nio.charset.StandardCharsets

/**
 * Boots the embedded metrics/health server on a free port and exercises every
 * endpoint over real HTTP, including the readiness state transition.
 */
class MetricsServerSpec extends AnyFunSuite with BeforeAndAfterAll {

  private var server: MetricsServer = _
  private var port: Int             = _

  private def freePort(): Int = {
    val s = new ServerSocket(0)
    try s.getLocalPort
    finally s.close()
  }

  override def beforeAll(): Unit = {
    PipelineMetrics.reset()
    port = freePort()
    server = new MetricsServer(port)
    server.start()
  }

  override def afterAll(): Unit = if (server != null) server.stop()

  private def get(path: String): (Int, String) = {
    val conn = new URL(s"http://127.0.0.1:$port$path").openConnection()
      .asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    val code = conn.getResponseCode
    val stream =
      if (code >= 400) conn.getErrorStream else conn.getInputStream
    val body =
      if (stream == null) ""
      else {
        val r  = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        val sb = new StringBuilder
        var ln = r.readLine()
        while (ln != null) { sb.append(ln).append('\n'); ln = r.readLine() }
        r.close()
        sb.toString
      }
    conn.disconnect()
    (code, body)
  }

  test("/healthz is always 200") {
    val (code, body) = get("/healthz")
    assert(code == 200)
    assert(body.trim == "ok")
  }

  test("/readyz reflects the query-active flag") {
    PipelineMetrics.setQueryActive(false)
    assert(get("/readyz")._1 == 503)
    PipelineMetrics.setQueryActive(true)
    assert(get("/readyz")._1 == 200)
  }

  test("/metrics serves Prometheus text with our counters") {
    PipelineMetrics.recordUpsert(3)
    val (code, body) = get("/metrics")
    assert(code == 200)
    assert(body.contains("lakestream_rows_upserted_total"))
  }
}
