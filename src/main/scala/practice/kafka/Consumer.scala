package practice.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

object Consumer extends App {

  
  val logger = LoggerFactory.getLogger(this.getClass)
  val prop = new Properties()

  prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "second-group3")
  prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2")
  prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[String, String](prop)

  //val topicPart = new TopicPartition("second-topic", 0)

  //consumer.assign(List(topicPart))
  //consumer.seek(topicPart, 1L)

  consumer.subscribe(List("output-stream-topic"))

  while (true) {
    val consumerRecords = consumer.poll(9000)
    for (consumerRecord <- consumerRecords) {
      println(consumerRecord.topic())
      println(consumerRecord.partition())
      println(consumerRecord.offset())
      println(consumerRecord.value())
      println("")
    }
    logger.info("commiting offset")
    consumer.commitSync()
  }

}