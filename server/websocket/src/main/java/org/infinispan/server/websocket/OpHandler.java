package org.infinispan.server.websocket;

import org.infinispan.Cache;
import org.infinispan.server.websocket.json.JsonObject;

import io.netty.channel.ChannelHandlerContext;

/**
 * Websocket cache operation handler.
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public interface OpHandler {

   String OP_CODE = "opCode";
   String CACHE_NAME = "cacheName";
   String KEY = "key";
   String VALUE = "value";
   String MIME = "mime";
   String ERROR = "error";

   /**
    * Handle a websocket channel operation.
    *
    * @param opPayload Operation payload.
    * @param cache The target cache.
    * @param ctx The Netty websocket channel handler.
    */
   void handleOp(JsonObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx);
}
