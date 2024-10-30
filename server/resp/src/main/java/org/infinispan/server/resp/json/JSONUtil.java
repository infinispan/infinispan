package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;

public class JSONUtil {
   public static final Configuration config = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   public static final ObjectMapper objectMapper = new ObjectMapper();

   public static void writeBytes(ObjectOutput output, byte[] b) throws IOException {
      output.writeInt(b.length);
      if (b.length > 0) {
         output.write(b);
      }
   }

   public static byte[] readBytes(ObjectInput input) throws IOException {
      int length = input.readInt();
      if (length < 0) {
         throw new IOException("Length cannot be less than 0");
      }
      byte[] b = new byte[length];
      input.read(b);
      return b;
   }
}
