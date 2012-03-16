/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.websocket;

import org.infinispan.Cache;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.websocket.DefaultWebSocketFrame;
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
		ctx.getChannel().write(new DefaultWebSocketFrame(responseObject.toString()));
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
