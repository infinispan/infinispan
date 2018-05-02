package org.infinispan.rest.search;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * @since 9.2
 */
public class HitSerializer extends JsonSerializer<Object> {

   private final ObjectMapper objectMapper = new ObjectMapper();

   public HitSerializer() {
      objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
   }

   @Override
   public void serialize(Object value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      String rawJson = value instanceof String ? value.toString() : new String((byte[]) value, UTF_8);
      gen.writeObject(objectMapper.readValue(rawJson, Object.class));
   }

}
