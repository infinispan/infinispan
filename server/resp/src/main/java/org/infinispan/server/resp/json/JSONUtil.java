package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;

public class JSONUtil {
   // JSONPath config that always return a list as a read result. Useful with jsonpath wildcards
   public static final Configuration configList = Configuration.builder().options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .options(Option.ALWAYS_RETURN_LIST)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider())
         .build();

   // JSONPath config that returns Node as a read result. Useful to check if a node exists.
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

   public static boolean isRoot(byte[] path) {
      return path != null && path.length == 1 && path[0] == '$';
   }

   /**
    * Returns a jsonpath version of path
    *
    * @return path itself if path is already in Jsonpath format
    *         or the jsonpath equivalent in a new byte[]
    */
   public static byte[] toJsonPath(byte[] path) {
      if (!isJsonPath(path)) {
         // For '.' return json root '$'
         if (path.length == 1 && path[0] == '.') {
            return new byte[] { '$' };
         }
         // append $. to the beginning
         // Using "$." so wrong legacy path like "" and " " will fail
         byte[] result = new byte[(path != null ? path.length + 2 : 2)];
         result[0] = '$';
         result[1] = '.';
         System.arraycopy(path, 0, result, 2, path.length);
         return result;
      }
      return path;
   }

   public static boolean isJsonPath(byte[] path) {
      return (path != null && path.length > 0 && path[0] == '$'
            && (path.length < 2 || path[1] == '.' || path[1] == '['));
   }

   public static boolean isJsonPath(String path) {
      return path == null ? false : isJsonPath(path.getBytes(StandardCharsets.UTF_8));
   }
}
