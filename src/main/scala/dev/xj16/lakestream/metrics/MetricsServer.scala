package dev.xj16.lakestream.metrics

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.scalalogging.StrictLogging

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

/**
 * A tiny embedded HTTP server exposing operational endpoints, built entirely on
 * the JDK's `com.sun.net.httpserver` so it adds zero dependencies:
 *
 *   - `GET /healthz`  — liveness. Always 200 once the process is up.
 *   - `GET /readyz`   — readiness. 200 only while the streaming query is active,
 *                       503 otherwise, so k8s stops routing to a dead driver.
 *   - `GET /metrics`  — Prometheus text exposition of the pipeline counters.
 *
 * This is what turns the "production-shaped" pitch into something a Kubernetes
 * probe and a Prometheus scraper can actually talk to.
 */
class MetricsServer(port: Int) extends StrictLogging {

  private var server: Option[HttpServer] = None

  def start(): Unit = {
    val http = HttpServer.create(new InetSocketAddress(port), 0)
    http.setExecutor(Executors.newSingleThreadExecutor())

    http.createContext("/healthz", plain(_ => (200, "ok\n")))
    http.createContext(
      "/readyz",
      plain { _ =>
        if (PipelineMetrics.isQueryActive) (200, "ready\n")
        else (503, "not ready\n")
      }
    )
    http.createContext(
      "/metrics",
      new PrometheusHandler
    )

    http.start()
    server = Some(http)
    logger.info(s"Metrics/health server listening on :$port (/healthz /readyz /metrics)")
  }

  def stop(): Unit = {
    server.foreach(_.stop(0))
    server = None
  }

  private def plain(f: HttpExchange => (Int, String)): HttpHandler =
    (exchange: HttpExchange) => {
      val (status, body) = f(exchange)
      respond(exchange, status, "text/plain; charset=utf-8", body)
    }

  private final class PrometheusHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit =
      respond(
        exchange,
        200,
        "text/plain; version=0.0.4; charset=utf-8",
        PipelineMetrics.renderPrometheus()
      )
  }

  private def respond(
      exchange: HttpExchange,
      status: Int,
      contentType: String,
      body: String
  ): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", contentType)
    exchange.sendResponseHeaders(status, bytes.length.toLong)
    val os = exchange.getResponseBody
    try os.write(bytes)
    finally os.close()
  }
}
