package org.infinispan.server.resp.json;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ParseContext;

public class JSONUtil {
   private static byte JSON_ROOT_BYTE = '$';
   public static byte[] JSON_ROOT = new byte[] { '$' };
   // JSONPath config that returns null instead of failing for missing leaf. Used
   // to set new leaf for undefined
   // path.
   // SET operation requires two different config:
   // - definite path needs DEFAULT_PATH_LEAF_TO_NULL otherwise can't add leaf node
   // - undefinite path doesn't need, because it doesn't add node if missing, it just
   // update existing node
   public static final Configuration configForDefiniteSet = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
         .options(Option.DEFAULT_PATH_LEAF_TO_NULL)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider()).build();

   public static final Configuration configForSet = Configuration.builder().options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider()).build();
   public static ParseContext parserForSet = JsonPath.using(configForSet);

   // GET operation needs ALWAYS_RETURN_LIST to handle multipath results
   public static final Configuration configForGet = Configuration.builder().options(Option.ALWAYS_RETURN_LIST)
         .options(Option.SUPPRESS_EXCEPTIONS).jsonProvider(new InfinispanJacksonJsonNodeProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider()).build();
   public static ParseContext parserForGet = JsonPath.using(configForGet);

   // Modifier operations need ALWAYS_RETURN_LIST and AS_PATH_LIST
   public static final Configuration configForMod = Configuration.builder().options(Option.AS_PATH_LIST)
         .options(Option.SUPPRESS_EXCEPTIONS)
         .jsonProvider(new com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider())
         .mappingProvider(new com.jayway.jsonpath.spi.mapper.JacksonMappingProvider()).build();
   public static ParseContext parserForMod = JsonPath.using(configForMod);

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
      return path != null && path.length == 1 && path[0] == JSON_ROOT_BYTE;
   }

   /**
    * Returns a jsonpath version of path
    *
    * @return path itself if path is already in Jsonpath format or the jsonpath equivalent in a new
    *         byte[]
    */
   public static byte[] toJsonPath(byte[] path) {
      if (!isJsonPath(path)) {
         if (path.length >= 1 && path[0] == '.') {
            if (path.length == 1) {
               // For '.' return json root '$'
               return JSON_ROOT;
            }
            // Just append $ to the beginning
            byte[] result = new byte[(path.length + 1)];
            result[0] = JSON_ROOT_BYTE;
            System.arraycopy(path, 0, result, 1, path.length);
            return result;
         }
         // append $. to the beginning
         // Using "$." so wrong legacy path like "" and " " will fail
         byte[] result = new byte[(path.length + 2)];
         result[0] = JSON_ROOT_BYTE;
         result[1] = '.';
         System.arraycopy(path, 0, result, 2, path.length);
         return result;
      }
      return path;
   }

   public static boolean isJsonPath(byte[] path) {
      return (path != null && path.length > 0 && path[0] == JSON_ROOT_BYTE
            && (path.length < 2 || path[1] == '.' || path[1] == '['));
   }

   public static boolean isJsonPath(String path) {
      return path == null ? false : isJsonPath(path.getBytes(StandardCharsets.UTF_8));
   }

   public static byte[] DEFAULT_PATH = { '.' };
}
