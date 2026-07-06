package dev.xj16.lakestream.tools

import java.time.Instant
import java.util.{Properties, UUID}
import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord
}
import org.apache.kafka.common.serialization.StringSerializer
import com.typesafe.scalalogging.StrictLogging

/**
 * A tiny demo producer that publishes synthetic click-stream events to Kafka
 * so the pipeline has something to consume end-to-end on a fresh cluster.
 *
 * It intentionally emits a controlled fraction of DUPLICATE events (same
 * `eventId`) so you can observe the exactly-once MERGE collapsing them: the
 * Delta table row count stays equal to the number of *distinct* ids, not the
 * number of records sent.
 *
 * Run (after `sbt assembly`):
 *   java -cp lakestream-assembly-0.1.0.jar \
 *     dev.xj16.lakestream.tools.EventProducer localhost:9092 events 1000
 */
object EventProducer extends StrictLogging {

  private val eventTypes = Vector("page_view", "click", "add_to_cart", "purchase")
  private val rng        = new scala.util.Random(42)

  def main(args: Array[String]): Unit = {
    val bootstrap = args.lift(0).getOrElse("localhost:9092")
    val topic     = args.lift(1).getOrElse("events")
    val count     = args.lift(2).map(_.toInt).getOrElse(1000)
    val dupeRatio = args.lift(3).map(_.toDouble).getOrElse(0.1)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("acks", "all")

    val producer = new KafkaProducer[String, String](props)
    var lastId: String = null
    try {
      var sent = 0
      while (sent < count) {
        // With probability `dupeRatio`, resend the previous id to exercise
        // downstream deduplication.
        val reuse = lastId != null && rng.nextDouble() < dupeRatio
        val id    = if (reuse) lastId else UUID.randomUUID().toString
        val json  = randomEvent(id)
        producer.send(new ProducerRecord[String, String](topic, id, json))
        lastId = id
        sent += 1
        if (sent % 100 == 0) logger.info(s"Sent $sent/$count events")
      }
      producer.flush()
      logger.info(s"Done. Sent $count events (~${(dupeRatio * 100).toInt}% dupes) to $topic")
    } finally {
      producer.close()
    }
  }

  private def randomEvent(id: String): String = {
    val userId    = s"u${rng.nextInt(500)}"
    val eventType = eventTypes(rng.nextInt(eventTypes.length))
    val amount =
      if (eventType == "purchase") f""", "amount": ${rng.nextInt(20000) / 100.0}%.2f"""
      else ""
    val ts = Instant.now().toEpochMilli
    s"""{"eventId":"$id","userId":"$userId","eventType":"$eventType",""" +
      s""""url":"/p/${rng.nextInt(1000)}","referrer":"/home"$amount,""" +
      s""""timestamp":$ts}"""
  }
}
