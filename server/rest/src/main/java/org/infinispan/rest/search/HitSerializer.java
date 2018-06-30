package org.infinispan.rest.search;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

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
