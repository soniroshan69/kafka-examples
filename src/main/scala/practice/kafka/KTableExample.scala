package practice.kafka

import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.kstream.Suppressed
import java.time.Duration

object KTableExample extends App{
  
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._
  
  val prop = new Properties()
    prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-app")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)
   
    val builder = new StreamsBuilder()
    val kt0= builder.table[String, String]("stock-val")
    
    kt0.toStream.print(Printed.toSysOut().withLabel("KT0"))
    
    //filtering and holding kt0 to kt1 transmission of records for 1 minut and 100000 cache bytes
    val kt1= kt0.filter((k,v) => (k.equals("HDFC") || k.equals("AXIS")) && !v.isEmpty())
    .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(1), Suppressed.BufferConfig.maxBytes(100000).emitEarlyWhenFull()))
    
    kt1.toStream.print(Printed.toSysOut().withLabel("KT1"))
    
    val kstream = new KafkaStreams(builder.build, prop)
    kstream.start()
    
    sys.addShutdownHook(new Thread(new Runnable {@Override def run(){kstream.cleanUp()}}))
  
    
}