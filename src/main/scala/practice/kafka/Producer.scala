package practice.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

object Producer extends App{
  
 val prop = new Properties()
  
  prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  prop.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
  prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "2")
  prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
  prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "9000")
  prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024))
  
  val kafkaProd = new KafkaProducer[String, String](prop)
 
  val prodRec = new ProducerRecord[String, String]("second-topic", "1", "hii")
  
  kafkaProd.send(prodRec);
 
 //kafkaProd.close()
 
 Thread.sleep(30000)
  
}