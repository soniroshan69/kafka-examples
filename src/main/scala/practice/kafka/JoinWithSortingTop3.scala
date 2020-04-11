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
import com.fasterxml.jackson.annotation.JsonInclude
import scala.collection.mutable.TreeSet
import practice.pojo._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.annotation.JsonProperty
import practice.custom.serde.CustomDeserializer
import practice.custom.serde.CustomSerializer
import org.apache.kafka.streams.kstream.GlobalKTable

object JoinWithSortingTop3 extends App {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  val prop = new Properties()
  prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "active-session-count-app")
  prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  val adClickSe = new CustomSerializer[AdClick]
  val adClickDes = new CustomDeserializer[AdClick]

  val adInventSe = new CustomSerializer[AdInventories]
  val adInventDes = new CustomDeserializer[AdInventories]

  val clicksByNewsTypesSe = new CustomSerializer[ClicksByNewsType]
  val clicksByNewsTypesDes = new CustomDeserializer[ClicksByNewsType]

  val top3NewsTypesSe = new CustomSerializer[Top3NewsTypes]
  val top3NewsTypesDes = new CustomDeserializer[Top3NewsTypes]

  implicit val adClickSerde = Serdes.serdeFrom(adClickSe, adClickDes)
  implicit val adInventSerde = Serdes.serdeFrom(adInventSe, adInventDes)
  implicit val clicksByNewsTypeSerde = Serdes.serdeFrom(clicksByNewsTypesSe, clicksByNewsTypesDes)
  implicit val top3NewsTypesSerde = Serdes.serdeFrom(top3NewsTypesSe, top3NewsTypesDes)

  implicit val stringSerde = Serdes.String()
  implicit val longSerde = Serdes.Long().asInstanceOf[Serde[scala.Long]]

  val builder = new StreamsBuilder
  val adClicks: KStream[String, AdClick] =
    builder.stream[String, AdClick]("ad-clicks")

  val adInvent: GlobalKTable[String, AdInventories] =
    builder.globalTable[String, AdInventories]("ad-inventories")

  val newsTypeWiseCount: KTable[String, Long] = adClicks.join(adInvent)((k, v) ⇒ k, (v1, v2) ⇒ v2)
    .groupBy((k, v) ⇒ v.getNewsType).count()

  //grouping by randon same key so that all records are in one partition
  newsTypeWiseCount.groupBy((k, v) ⇒ {
    val clicks = new ClicksByNewsType
    clicks.setNewsType(k)
    clicks.setClicks(v)
    ("abc", clicks)
  }).aggregate(new Top3NewsTypes)(
    (k, newVal, aggVal) ⇒
      {
        aggVal.add(newVal)
        aggVal
      },
    (k, newVal, aggVal) ⇒
      {
        aggVal.add(newVal)
        aggVal
      }).toStream.print(Printed.toSysOut())

  val streams = new KafkaStreams(builder.build(), prop)
  streams.start()

  sys.ShutdownHookThread {
    streams.cleanUp()
  }
}