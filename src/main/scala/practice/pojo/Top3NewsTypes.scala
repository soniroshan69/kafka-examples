package practice.pojo

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.annotation.JsonProperty
import scala.collection.mutable.TreeSet

@JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonPropertyOrder({
    Array("top3Sorted")
  })
  class Top3NewsTypes {
    val mapper = new ObjectMapper()
    var top3Sorted = new TreeSet[ClicksByNewsType]()(new Ordering[ClicksByNewsType] {
      @Override
      def compare(o1: ClicksByNewsType, o2: ClicksByNewsType): Int = {
        val result = o2.getClicks.compareTo(o1.getClicks)
        if (result != 0) {
          result
        } else {
          o1.getNewsType.compareTo(o2.getNewsType)
        }
      }
    })
    
    def add(newVal: ClicksByNewsType)={
      top3Sorted+=(newVal)
      if(top3Sorted.size>3){
        remove(top3Sorted.last)
      }
    }
    
    def remove(oldVal: ClicksByNewsType)={
      top3Sorted.remove(oldVal)
    }
    
    @JsonProperty("top3Sorted")
    def getTop3Sorted():String={
      mapper.writeValueAsString(top3Sorted)
    }
    
    @JsonProperty("top3Sorted")
    def setTop3Sorted(top3string: String)={
      val top3 =mapper.readValue[Array[ClicksByNewsType]](top3string, classOf[Array[ClicksByNewsType]])
      top3.foreach(add(_))
    }
  }