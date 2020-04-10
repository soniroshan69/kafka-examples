package practice.kafka

import java.time.Instant
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.streams.kstream.SessionWindows
import java.time.Duration
import org.apache.kafka.streams.scala.kstream.Suppressed
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.Printed

object JoinOpKtToKt extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  val prop = new Properties()
  prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "active-session-count-app")
  prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  val jsonDes: Deserializer[JsonNode] = new JsonDeserializer
  val jsonSe: Serializer[JsonNode] = new JsonSerializer

  implicit val jsonSerde = Serdes.serdeFrom(jsonSe, jsonDes)
  implicit val stringSerde = Serdes.String()
  implicit val longSerde = Serdes.Long().asInstanceOf[Serde[scala.Long]]

  val builder = new StreamsBuilder
  val latestLogin: KTable[String, JsonNode] =
    builder.table[String, JsonNode]("users-latest-login")

  val loginRecords: KTable[String, JsonNode] =
    builder.table[String, JsonNode]("user-logins")


  val updatedLatestLogins: KTable[String, String] = latestLogin.join(loginRecords)(
    (v1, v2) => v2.get("loginTime").asText())

  updatedLatestLogins.toStream.print(Printed.toSysOut())

  val streams = new KafkaStreams(builder.build(), prop)
  streams.start()

  sys.ShutdownHookThread {
    streams.cleanUp()
  }

  class TimeExtractor extends TimestampExtractor {
    @Override
    def extract(cr: ConsumerRecord[Object, Object], prevTime: Long): Long = {
      val userClicks = cr.value().asInstanceOf[JsonNode]
      val time = Instant.parse(userClicks.get("time").asText()).toEpochMilli()
      time
    }
  }

}