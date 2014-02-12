package org.infinispan.server.websocket.handlers;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.Cache;
import org.infinispan.server.websocket.OpHandler;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Cache "remove" operation handler.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class RemoveHandler implements OpHandler {

	@Override
   public void handleOp(JSONObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) throws JSONException {
		String key = (String) opPayload.get(OpHandler.KEY);
		
		cache.remove(key);
	}
}
