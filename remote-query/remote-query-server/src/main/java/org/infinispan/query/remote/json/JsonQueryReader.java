package org.infinispan.query.remote.json;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * since 9.4
 */
public final class JsonQueryReader {

   private static final ObjectMapper mapper = new ObjectMapper();

   public static JsonQueryRequest getQueryFromJSON(byte[] input) throws IOException {
      return mapper.readValue(input, JsonQueryRequest.class);
   }

}
