package org.infinispan.query.remote.json;

import java.io.IOException;

import org.infinispan.commons.CacheException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @since 9.4
 */
public abstract class JsonQueryResponse {

   private static final ObjectMapper mapper = new ObjectMapper();

   static {
      mapper.registerSubtypes(ProjectedJsonResult.class, JsonQueryResult.class);
      mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
   }

   public byte[] asBytes() {
      try {
         return mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
      } catch (IOException e) {
         throw new CacheException("Invalid query result");
      }
   }
}
