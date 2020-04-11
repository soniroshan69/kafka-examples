package practice.custom.serde

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serialize Jackson JsonNode tree model objects to UTF-8 JSON. Using the tree model allows handling arbitrarily
 * structured data without corresponding Java classes. This serializer also supports Connect schemas.
 */
 class CustomSerializer[T] extends Serializer[T] {
    val objectMapper = new ObjectMapper();


    @Override
    def serialize(topic: String, data: T):Array[Byte]= {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch  {
          case e:Exception ⇒ throw new SerializationException("Error serializing JSON message", e);
        }
    }
}
