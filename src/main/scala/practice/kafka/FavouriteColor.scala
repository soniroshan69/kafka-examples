package practice.kafka

import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig


object FavouriteColor extends App {
  
   import org.apache.kafka.streams.scala.Serdes._
  
  val prop = new Properties()
  prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "fav-color-app")
  prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  
  val streamBuild = new StreamsBuilder
  
  val inputTable:KTable[String,String] = streamBuild.table[String, String]("input-color-topic")
  
  val groupedTable:KGroupedTable[String, String] =  inputTable.groupBy((k,v)=> (v,v))
  
  val outputKTable:KTable[String, Long]= groupedTable.count()
  outputKTable.toStream.to("fav-color-topic")
  
  val kafkaStream = new KafkaStreams(streamBuild.build(), prop)  
   
  println("----------------------------- "+kafkaStream) 
  
  kafkaStream.start()
  
  sys.ShutdownHookThread{ kafkaStream.close()}
}