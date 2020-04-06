package practice.kafka

import java.time.Instant
import java.util.Properties
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.connect.json.JsonDeserializer
import org.apache.kafka.connect.json.JsonSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
//import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.scala.kstream.KStream

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory

object BankBalance extends App {

  TransactionProducer.proceed()
  BalanceCalculator.proceed()
  
  
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

      for(x <- 1 to 50) {
        try {
          prod.send(createRecord("divya", x*2))
          prod.send(createRecord("roshan", x))
          prod.send(createRecord("shubham",x*3))         
          
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

      return new ProducerRecord[String, JsonNode]("bank-transactions", name, transaction)

    }

  }

  object BalanceCalculator {

    import org.apache.kafka.streams.scala.ImplicitConversions._

    def proceed() {

      val prop = new Properties()
      prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app")
      prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

      val jsonDes: Deserializer[JsonNode] = new JsonDeserializer
      val jsonSe: Serializer[JsonNode] = new JsonSerializer

      implicit val jsonSerde = Serdes.serdeFrom(jsonSe, jsonDes)
      implicit val stringSerde = Serdes.String()

      val builder = new StreamsBuilder
      val transactions: KStream[String, JsonNode] = builder.stream[String, JsonNode]("bank-transactions")

      val initialBal = JsonNodeFactory.instance.objectNode()
      initialBal.put("count", 0)
      initialBal.put("balance", 0)
      initialBal.put("time", Instant.ofEpochMilli(0l).toString())

      val aggregateR = transactions.groupByKey.aggregate[JsonNode](initialBal)((key, transaction, bal) => createAggBal(transaction, bal))
      aggregateR.toStream.to("bank-balance-exactly-once")
      
      val streams = new KafkaStreams(builder.build(), prop)
      streams.start()

      sys.ShutdownHookThread {
        streams.close(10, TimeUnit.SECONDS)
      }

    }

    def createAggBal(transaction: JsonNode, bal: JsonNode): JsonNode = {

      val newBal = JsonNodeFactory.instance.objectNode()
      newBal.put("count", bal.get("count").asInt() + 1)
      newBal.put("balance", bal.get("balance").asInt() + transaction.get("amount").asInt())

      val transEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli()
      val balEpoch = Instant.parse(bal.get("time").asText()).toEpochMilli()
      val newEpochMilli = Instant.ofEpochMilli(Math.max(transEpoch, balEpoch))

      newBal.put("time", newEpochMilli.toString())

      newBal
    }

  }

}