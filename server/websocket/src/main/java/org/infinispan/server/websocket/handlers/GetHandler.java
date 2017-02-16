package org.infinispan.server.websocket.handlers;

import org.infinispan.Cache;
import org.infinispan.server.websocket.ChannelUtils;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.server.websocket.json.JsonObject;

import io.netty.channel.ChannelHandlerContext;

/**
 * Cache "get" operation handler.
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class GetHandler implements OpHandler {

   @Override
   public void handleOp(JsonObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) {
      String key = (String) opPayload.get(OpHandler.KEY);
      ChannelUtils.pushCacheValue(key, cache, ctx);
   }
}
