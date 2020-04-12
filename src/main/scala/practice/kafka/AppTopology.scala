package practice.top3News

import org.apache.kafka.streams.scala.StreamsBuilder
import practice.pojo.AdClick
import practice.pojo.AdInventories
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.kstream.KTable
import practice.pojo.ClicksByNewsType
import org.apache.kafka.streams.kstream.Printed
import practice.pojo.Top3NewsTypes
import practice.custom.serde.CustomDeserializer
import practice.custom.serde.CustomSerializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serde

object AppTopology {

  def withBuilder(builder: StreamsBuilder) {

    import org.apache.kafka.streams.scala.ImplicitConversions._

    val adClickSe = new CustomSerializer[AdClick]
    val adClickDes = new CustomDeserializer[AdClick]

    val adInventSe = new CustomSerializer[AdInventories]
    val adInventDes = new CustomDeserializer[AdInventories]

    val clicksByNewsTypesSe = new CustomSerializer[ClicksByNewsType]
    val clicksByNewsTypesDes = new CustomDeserializer[ClicksByNewsType]

    val top3NewsTypesSe = new CustomSerializer[Top3NewsTypes]
    val top3NewsTypesDes = new CustomDeserializer[Top3NewsTypes]

    implicit val adClickSerde = Serdes.serdeFrom(adClickSe, adClickDes)
    implicit val adInventSerde = Serdes.serdeFrom(adInventSe, adInventDes)
    implicit val clicksByNewsTypeSerde = Serdes.serdeFrom(clicksByNewsTypesSe, clicksByNewsTypesDes)
    implicit val top3NewsTypesSerde = Serdes.serdeFrom(top3NewsTypesSe, top3NewsTypesDes)

    implicit val stringSerde = Serdes.String()
    implicit val longSerde = Serdes.Long().asInstanceOf[Serde[scala.Long]]

    val adClicks: KStream[String, AdClick] =
      builder.stream[String, AdClick]("ad-clicks")

    val adInvent: GlobalKTable[String, AdInventories] =
      builder.globalTable[String, AdInventories]("ad-inventories")

    val newsTypeWiseCount: KTable[String, Long] = adClicks.join(adInvent)((k, v) ⇒ k, (v1, v2) ⇒ v2)
      .groupBy((k, v) ⇒ v.getNewsType).count()

    //grouping by randon same key so that all records are in one partition
    val top3: KTable[String, Top3NewsTypes] =newsTypeWiseCount.groupBy((k, v) ⇒ {
      val clicks = new ClicksByNewsType
      clicks.setNewsType(k)
      clicks.setClicks(v)
      ("abc", clicks)
    }).aggregate(new Top3NewsTypes)(
      (k, newVal, aggVal) ⇒
        {
          aggVal.add(newVal)
          aggVal
        },
      (k, newVal, aggVal) ⇒
        {
          aggVal.add(newVal)
          aggVal
        })
        
        top3.toStream.to("top3-ad-topic")

  }

}