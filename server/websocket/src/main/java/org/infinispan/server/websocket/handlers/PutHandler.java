package org.infinispan.server.websocket.handlers;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.Cache;
import org.infinispan.server.websocket.OpHandler;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Cache "get" operation handler.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class PutHandler implements OpHandler {

	@Override
   public void handleOp(JSONObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) throws JSONException {
		String key = (String) opPayload.get(OpHandler.KEY);
		String value = (String) opPayload.get(OpHandler.VALUE);
		String mimeType = (String) opPayload.get(OpHandler.MIME);

		if(mimeType.equals("application/json")) {
			// Decode the payload to a JSON string...
			
			// TODO:  Need some way to populate the JSON object string to an Object graph.
			// Something plugable... allowing JAXB, Smooks etc
			
			throw new UnsupportedOperationException("Complex object graphs not yet supported!! Cannot cache value:\n" + value);
		} else {
			// Put the raw value into the cache...
			cache.put(key, value);
		}
	}
}
