package org.infinispan.rest.server.operations.mediatypes.printers;

import java.io.IOException;
import java.util.stream.Collectors;

import org.codehaus.jackson.map.ObjectMapper;
import org.infinispan.CacheSet;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.server.operations.exceptions.ServerInternalException;
import org.infinispan.rest.server.operations.mediatypes.Charset;
import org.infinispan.rest.server.operations.mediatypes.OutputPrinter;
import org.infinispan.stream.CacheCollectors;
import org.infinispan.util.logging.LogFactory;

public class JSONOutputPrinter implements OutputPrinter {

   protected final static Log logger = LogFactory.getLog(JSONOutputPrinter.class, Log.class);

   private static class JsonMapperHolder {
      public static final ObjectMapper jsonMapper = new ObjectMapper();
   }

   @Override
   public byte[] print(String cacheName, CacheSet<?> keys, Charset charset) {
      return keys.stream()
            .map(b -> Escaper.escapeJson(b.toString()))
            .collect(CacheCollectors.serializableCollector(() -> Collectors.joining(",", "keys=[", "]")))
            .getBytes(charset.getJavaCharset());
   }

   @Override
   public byte[] print(byte[] value, Charset charset) throws ServerInternalException {
      Object objectToBeRendered;
      try {
         objectToBeRendered = DeserializationUtil.toObject(value);
      } catch (IOException | ClassNotFoundException e) {
         //This is either not a serializable object or the server has no clue how
         //to deserialize it
         logger.debug("Could not deserialize object from cache. Falling back to String representation.", e);
         objectToBeRendered = new String(value, charset.getJavaCharset());
      }

      try {
         return JsonMapperHolder.jsonMapper.writeValueAsBytes(objectToBeRendered);
      } catch (Exception e) {
         throw new ServerInternalException(e);
      }
   }


}
