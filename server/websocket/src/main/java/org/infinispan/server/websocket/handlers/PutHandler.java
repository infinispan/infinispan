package org.infinispan.server.websocket.handlers;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.Cache;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.server.websocket.json.JsonObject;
import org.infinispan.server.websocket.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.lang.invoke.MethodHandles;

/**
 * Cache "get" operation handler.
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class PutHandler implements OpHandler {

   private static final Log logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   @Override
   public void handleOp(JsonObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) {
      String key = (String) opPayload.get(OpHandler.KEY);
      String value = (String) opPayload.get(OpHandler.VALUE);
      String mimeType = (String) opPayload.get(OpHandler.MIME);

      if (mimeType.equals("application/json")) {
         // Decode the payload to a JSON string...

         // TODO:  Need some way to populate the JSON object string to an Object graph.
         // Something plugable... allowing JAXB, Smooks etc

         throw logger.complexGraphObjectAreNotYetSupported(value);
      } else {
         // Put the raw value into the cache...
         cache.put(key, value);
      }
   }
}
