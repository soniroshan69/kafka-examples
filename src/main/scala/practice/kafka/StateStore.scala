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
import org.apache.kafka.streams.scala.kstream.KStream
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.kstream.ValueTransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import java.util.concurrent._
import org.apache.kafka.streams.scala.kstream.Produced
import org.apache.kafka.streams.processor.StreamPartitioner

object StateStore extends App {

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
          prod.send(createRecord("pnb","divya", x * 2))
          prod.send(createRecord("icici","roshan", x))
          prod.send(createRecord("axis","shubham", x * 3))

        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }

      prod.close()

    }

    def createRecord(bank:String, name: String, amount: Int): ProducerRecord[String, JsonNode] = {

      var transaction = JsonNodeFactory.instance.objectNode()

      val transTime = Instant.now()

      transaction.put("name", name)
      transaction.put("amount", amount)
      transaction.put("time", transTime.toString())

      return new ProducerRecord[String, JsonNode]("transactions", bank, transaction)

    }

  }

  object RunnigBalCalc {
    import org.apache.kafka.streams.scala.ImplicitConversions._

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
    val transactions: KStream[String, JsonNode] = builder.stream[String, JsonNode]("transactions")

    val storeBuilder = Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore("stateStore-2"), stringSerde, Serdes.Integer())

    builder.addStateStore(storeBuilder)

    //customer partitioner to partition data using name so that multithread stream can work with state store
    //bcz state store is for per task and per task is for per partition
    transactions.through("intermediateTopic")(Produced.`with`(new TransactionPartitioner)(stringSerde, jsonSerde))

    transactions.transformValues[JsonNode](new RunningBalance, "stateStore-2").to("transactionsWithRunningBal")

    val kstream = new KafkaStreams(builder.build(), prop)
    kstream.start
    sys.addShutdownHook(kstream.cleanUp())
  }
}

class RunningBalance extends ValueTransformerSupplier[JsonNode, JsonNode] {

  @Override def get() = {
    new ValueTransformer[JsonNode, JsonNode]() {

      var stateStore: KeyValueStore[String, Int] = _
      //var stateStore1 = _

      @Override def init(pc: ProcessorContext) = {
        stateStore = pc.getStateStore("stateStore-2").asInstanceOf[KeyValueStore[String, Int]]
      }
      @Override def transform(transaction: JsonNode): JsonNode = {

        val amount = transaction.get("amount").asInt()

        val accumulatedBal = stateStore.get(transaction.get("name").asText())

        val runningBal = amount + accumulatedBal

        stateStore.put(transaction.get("name").asText(), runningBal)

        var transactionWithRunningBal = JsonNodeFactory.instance.objectNode()
        transactionWithRunningBal.put("name", transaction.get("name").asText())
        transactionWithRunningBal.put("amount", transaction.get("amount").asInt())
        transactionWithRunningBal.put("time", transaction.get("time").asText())
        transactionWithRunningBal.put("runningBal", runningBal)

        transactionWithRunningBal

      }

      @Override def close() = {

      }
    }
  }

}

class TransactionPartitioner extends StreamPartitioner[String, JsonNode] {

  @Override def partition(topic: String, key: String, value: JsonNode, numParttions: Int): Integer = {
    return value.get("name").asText().hashCode() % numParttions
  }

}