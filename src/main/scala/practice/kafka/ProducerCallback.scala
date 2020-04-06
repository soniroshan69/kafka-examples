package practice.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.clients.producer.Callback

object ProducerCallback extends App{
  
 val prop = new Properties()
  
  prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  
  val kafkaProd = new KafkaProducer[String, String](prop)
 
  val prodRec = new ProducerRecord[String, String]("first-topic", 1.toString(), "hello world")
  
  kafkaProd.send(prodRec, new Callback() {
    override def onCompletion(metadata: RecordMetadata, ex: Exception) = {
    if (ex != null) {
      println(ex)
    } else {
      
      println(metadata.offset()+ "  "+metadata.partition()+"  "+metadata.topic())
      println(s"Successfully sent message : $metadata")
    }
  }
  })
 
 kafkaProd.close()
  
}