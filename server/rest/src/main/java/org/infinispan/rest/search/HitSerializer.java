package org.infinispan.rest.search;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;

/**
 * @since 9.2
 */
public class HitSerializer extends JsonSerializer<String> {

   private final ObjectMapper objectMapper = new ObjectMapper();

   public HitSerializer() {
      objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
   }

   @Override
   public void serialize(String string, JsonGenerator gen, SerializerProvider provider) throws IOException {
      Object json = objectMapper.readValue(string, Object.class);
      gen.writeObject(json);
   }

}
