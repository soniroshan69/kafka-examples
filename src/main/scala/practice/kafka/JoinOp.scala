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

object JoinOp extends App {

  Producer.proceed()
  Joins.proceed()

  object Producer {

    def proceed() {

      val prop = new Properties()
      prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      prop.setProperty(ProducerConfig.ACKS_CONFIG, "all")
      prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
      prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

      val prod = new KafkaProducer[String, String](prop)

      println("new user")
      prod.send(userInfoRec("roshan", "first=roshan, last=soni")).get
      prod.send(userPurchaseRec("roshan", "rapple(1)")).get

      Thread.sleep(5000)

      println("non existing user")
      prod.send(userPurchaseRec("divya", "dbanana(2)")).get

      Thread.sleep(5000)

      println("update user info and send new transaction")
      prod.send(userInfoRec("roshan", "first=roshan, last=sonyy")).get
      prod.send(userPurchaseRec("roshan", "rorange(3)")).get

      Thread.sleep(5000)

      println("purchase first and then create user")
      prod.send(userPurchaseRec("shubham", "sgrapes(4)")).get
      prod.send(userInfoRec("shubham", "first=shubham, last=soni")).get
      prod.send(userPurchaseRec("shubham", "scherry(5)")).get
      prod.send(userInfoRec("shubham", null)).get

      Thread.sleep(5000)

      println("create user but it gets deleted before purchase")
      prod.send(userInfoRec("deepa", "first=deepa, last=soni")).get
      prod.send(userInfoRec("deepa", null)).get
      prod.send(userPurchaseRec("deepa", "deepomo(6)")).get

      Thread.sleep(5000)

      prod.close()

      def userInfoRec(key: String, value: String): ProducerRecord[String, String] = {
        new ProducerRecord[String, String]("user-table", key, value)
      }

      def userPurchaseRec(key: String, value: String): ProducerRecord[String, String] = {
        new ProducerRecord[String, String]("user-purchases", key, value)
      }

    }

  }

  object Joins {

    import org.apache.kafka.streams.scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes._

    def proceed() {

      val prop = new Properties()
      prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-balance-app")
      prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

      val builder = new StreamsBuilder
      val userPurchase = builder.stream[String, String]("user-purchases")
      val userInfo = builder.globalTable[String, String]("user-table")

      //inner join
      val innerJoinOutput: KStream[String, String] = userPurchase.join(userInfo)((purchaseKey, purchaseVal) => purchaseKey, (purchaseVal, infoVal) => "purchase " + purchaseVal + " info=[" + infoVal + "]")
      innerJoinOutput.to("user-purchases-enriched-inner-join")

      //left join
      val leftJoinOutput: KStream[String, String] = userPurchase.leftJoin(userInfo)((purchaseKey, purchaseVal) => purchaseKey, (purchaseVal, infoVal) =>
        {
          if (userInfo != null)
            "purchase " + purchaseVal + " info[" + infoVal + "]"
          else
            "purchase " + infoVal + " info=null"
        })

      leftJoinOutput.to("user-purchases-enriched-left-join")

      val kafkaStream = new KafkaStreams(builder.build(), prop)
      kafkaStream.cleanUp()
      kafkaStream.start()

      sys.ShutdownHookThread { kafkaStream.close(10, TimeUnit.SECONDS) }

    }

  }

}