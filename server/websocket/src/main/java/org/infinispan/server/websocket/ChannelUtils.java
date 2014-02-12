package org.infinispan.server.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.infinispan.Cache;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Channel Utilities.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class ChannelUtils {

	/**
	 * Push a cache entry value out onto the websocket channel (to the browser).
	 * @param key The cache entry key whose value is to be pushed to the browser.
	 * @param cache The cache containing the key.
	 * @param ctx The channel context associated with the browser websocket channel..
	 * @throws JSONException Error generating JSON string.
	 */
	public static void pushCacheValue(String key, Cache<Object, Object> cache, ChannelHandlerContext ctx) throws JSONException {
		Object value = cache.get(key);
		
		JSONObject responseObject = toJSON(key, value, cache.getName());
		
		// Write the JSON response out onto the channel...
		ctx.channel().writeAndFlush(new TextWebSocketFrame(responseObject.toString()));
	}

	/**
	 * Cache key, value and cache-name to JSON string.
	 * @param key The cache key.
	 * @param value The cache value.
	 * @param cacheName The cache name.
	 * @return JSON Object representing a cache entry payload for transmission to the browser channel.
	 * @throws JSONException Error generating JSON string.
	 */
	public static JSONObject toJSON(String key, Object value, String cacheName) throws JSONException {
		JSONObject jsonObject = new JSONObject();
	
		jsonObject.put(OpHandler.CACHE_NAME, cacheName);
		jsonObject.put(OpHandler.KEY, key);
		
		if(value != null) {
			// Encode the cache value as JSON...
			JSONObject valueObject = new JSONObject(value);
			if(valueObject.get("bytes") == null) {
				jsonObject.put(OpHandler.VALUE, valueObject.toString());
				jsonObject.put(OpHandler.MIME, "application/json");
			} else {
				jsonObject.put(OpHandler.VALUE, value);
				jsonObject.put(OpHandler.MIME, "text/plain");
			}
		} else {
			jsonObject.put(OpHandler.VALUE, JSONObject.NULL);
		}
		
		return jsonObject;
	}
}
