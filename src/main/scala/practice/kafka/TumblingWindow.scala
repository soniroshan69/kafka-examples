package practice.kafka

import java.time.Duration
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
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.TimestampExtractor
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Consumed
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala.kstream.Materialized

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.streams.KeyValue

object TumblingWindow extends App {

  TransactionProducer.proceed()
  WindowedCount.proceed()
  
  object TransactionProducer {

    def proceed() {
      val prop = new Properties()
      prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer")
      prop.setProperty(ProducerConfig.ACKS_CONFIG, "all")
      prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      prop.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
      prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")

      val prod = new KafkaProducer[String, JsonNode](prop)

      for (x <- 1 to 3) {
        try {
          print("loop "+x)
          prod.send(createRecord("divya", x * 2))
          prod.send(createRecord("roshan", x))
          prod.send(createRecord("shubham", x * 3))
          if(x==2)
          Thread.sleep(40000)

        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }

      prod.close()

    }

    def createRecord(name: String, amount: Int): ProducerRecord[String, JsonNode] = {
    
      var transaction = JsonNodeFactory.instance.objectNode()

      val transTime = Instant.now()

      transaction.put("name", name)
      transaction.put("amount", amount)
      transaction.put("time", transTime.toString())

      return new ProducerRecord[String, JsonNode]("windowed-transaction", name, transaction)

    }

  }

  object WindowedCount {

    import org.apache.kafka.streams.scala.ImplicitConversions._
    def proceed() = {
      val prop = new Properties()
      prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-count-app1")
      prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

      val jsonDes: Deserializer[JsonNode] = new JsonDeserializer
      val jsonSe: Serializer[JsonNode] = new JsonSerializer

      implicit val jsonSerde = Serdes.serdeFrom(jsonSe, jsonDes)
      implicit val stringSerde = Serdes.String()
      implicit val longSerde = Serdes.Long().asInstanceOf[Serde[scala.Long]]

      val builder = new StreamsBuilder
      val transactions: KStream[String, JsonNode] =
        builder.stream[String, JsonNode]("windowed-transaction")(Consumed.`with`(new TimeExtractor)(stringSerde, jsonSerde))

      val ks1: KTable[Windowed[String], Long] = transactions.groupByKey.windowedBy(TimeWindows.of(Duration.ofSeconds(30))).count()(Materialized.as("windowed-count-store")(stringSerde, longSerde))

      ks1.toStream.foreach((wkey, v) => println("name= " + wkey.key() + " window id=" + wkey.window().hashCode() + " window start=" + wkey.window().startTime()
        + " window end" + wkey.window().endTime() + " count= " + v))
        
        ks1.toStream.map[String, Long]((key, value) => (key.key(), value)).to("windowed-count")

      val streams = new KafkaStreams(builder.build(), prop)
      streams.start()

      sys.ShutdownHookThread {
        streams.cleanUp()
      }
    }
  }
  class TimeExtractor extends TimestampExtractor {
    @Override
    def extract(cr: ConsumerRecord[Object, Object], prevTime: Long): Long = {
      val transaction = cr.value().asInstanceOf[JsonNode]
      val time = Instant.parse(transaction.get("time").asText()).toEpochMilli()
      time
    }
  }

}