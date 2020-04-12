package practice.top3News
import org.apache.kafka.streams.TopologyTestDriver

import org.junit.jupiter.api._
import java.util.Properties
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.test.ConsumerRecordFactory
import practice.custom.serde.CustomDeserializer
import practice.custom.serde.CustomSerializer
import org.apache.kafka.common.serialization.Serdes
import practice.pojo.ClicksByNewsType
import practice.pojo.AdClick
import practice.pojo.Top3NewsTypes
import practice.pojo.AdInventories
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions._

class AppTopologyTest {
  var topologyTestDriver: TopologyTestDriver = _

  @BeforeAll
  def setupAll() = {

    val prop = new Properties()
    prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "active-session-count-app")
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    prop.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

    val builder = new StreamsBuilder
    AppTopology.withBuilder(builder)

    topologyTestDriver = new TopologyTestDriver(builder.build, prop)
  }

  @Test
  @DisplayName("Test ad click flow from source to final topic")
  def adClickFlowTest() = {

    val adClickSe = new CustomSerializer[AdClick]
    val adClickDes = new CustomDeserializer[AdClick]

    val adInventSe = new CustomSerializer[AdInventories]
    val adInventDes = new CustomDeserializer[AdInventories]

    val clicksByNewsTypesSe = new CustomSerializer[ClicksByNewsType]
    val clicksByNewsTypesDes = new CustomDeserializer[ClicksByNewsType]

    val top3NewsTypesSe = new CustomSerializer[Top3NewsTypes]
    val top3NewsTypesDes = new CustomDeserializer[Top3NewsTypes]

    var adClickFactory = new ConsumerRecordFactory(new StringSerializer, adClickSe)
    
    val adClick = new AdClick()
    adClick.setInventoryID("1001")
    
    topologyTestDriver.pipeInput(adClickFactory.create("adClickTopic", "1001" , adClick))
    
    var producerRecord:ProducerRecord[String, Top3NewsTypes] =  topologyTestDriver.readOutput("adClickTopic", new StringDeserializer, top3NewsTypesDes)
    
        //not correct, just for syntax
    assertEquals("aa", producerRecord.value().getTop3Sorted())
        
    
    
  }

  @AfterAll
  def closeAll = {
    topologyTestDriver.close()
  }

}