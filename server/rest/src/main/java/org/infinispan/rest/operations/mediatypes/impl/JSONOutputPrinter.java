package org.infinispan.rest.operations.mediatypes.impl;

import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.exceptions.ServerInternalException;
import org.infinispan.rest.operations.mediatypes.Charset;
import org.infinispan.rest.operations.mediatypes.OutputPrinter;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link OutputPrinter} for JSON values.
 *
 * @author Sebastian Łaskawiec
 */
public class JSONOutputPrinter implements OutputPrinter {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);

   private static class JsonMapperHolder {
      public static final ObjectMapper jsonMapper = new ObjectMapper();
   }

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(b -> Escaper.escapeJson(b.toString()))
            .collect(() -> Collectors.joining(",", "keys=[", "]"))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(Object value, Charset charset) throws ServerInternalException {
      try {
         return JsonMapperHolder.jsonMapper.writeValueAsBytes(value);
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
   }
}
