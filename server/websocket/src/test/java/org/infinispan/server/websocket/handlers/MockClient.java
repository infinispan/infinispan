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
package org.infinispan.server.websocket.handlers;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.websocket.MockChannel;
import org.infinispan.websocket.MockChannelHandlerContext;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class MockClient {
	
	private String cacheName;
	private CacheContainer cacheContainer;
	private Cache<Object, Object> cache;

	private OpHandler putHandler = new PutHandler(); 
	private OpHandler getHandler = new GetHandler(); 
	private OpHandler removeHandler = new RemoveHandler(); 
	private OpHandler notifyHandler = new NotifyHandler();
	private MockChannelHandlerContext ctx;
	
	public MockClient(String cacheName, MockChannel mockChannel) {
		this.cacheName = cacheName;
		this.ctx = new MockChannelHandlerContext(mockChannel);
		
		cacheContainer = TestCacheManagerFactory.createCacheManager();
		cache = cacheContainer.getCache(cacheName);
	}
	
	public void put(String key, String value) {
		callHandler(putHandler, toPut(key, value, "text/plain"));
	}
	
	public void put(String key, JSONObject value) {
		callHandler(putHandler, toPut(key, value.toString(), "application/json"));
	}

	public void get(String key) {
		callHandler(getHandler, toGet(key));
	}

	public void remove(String key) {
		callHandler(removeHandler, toRemove(key));
	}

	public void notify(String key) {
		callHandler(notifyHandler, toNotify(key));
	}

	public void unnotify(String key) {
		callHandler(notifyHandler, toUnnotify(key));
	}
	
	public Cache<Object, Object> getCache() {
		return cache;
	}

	private void callHandler(OpHandler handler, JSONObject jsonObj) {
		try {
			handler.handleOp(jsonObj, cache, ctx);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
	}
	
	private JSONObject toPut(String key, String value, String mimeType) {
		JSONObject jsonObj = new JSONObject();
		
		try {
			jsonObj.put(OpHandler.OP_CODE, "put");
			jsonObj.put(OpHandler.CACHE_NAME, cacheName);
			jsonObj.put(OpHandler.KEY, key);
			jsonObj.put(OpHandler.VALUE, value);
			jsonObj.put(OpHandler.MIME, mimeType);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
		
		return jsonObj;
	}
	
	private JSONObject toGet(String key) {
		JSONObject jsonObj = new JSONObject();
		
		try {
			jsonObj.put(OpHandler.OP_CODE, "get");
			jsonObj.put(OpHandler.CACHE_NAME, cacheName);
			jsonObj.put(OpHandler.KEY, key);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
		
		return jsonObj;
	}
	
	private JSONObject toRemove(String key) {
		JSONObject jsonObj = new JSONObject();
		
		try {
			jsonObj.put(OpHandler.OP_CODE, "remove");
			jsonObj.put(OpHandler.CACHE_NAME, cacheName);
			jsonObj.put(OpHandler.KEY, key);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
		
		return jsonObj;
	}
	
	private JSONObject toNotify(String key) {
		JSONObject jsonObj = new JSONObject();
		
		try {
			jsonObj.put(OpHandler.OP_CODE, "notify");
			jsonObj.put(OpHandler.CACHE_NAME, cacheName);
			jsonObj.put(OpHandler.KEY, key);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
		
		return jsonObj;
	}
	
	private JSONObject toUnnotify(String key) {
		JSONObject jsonObj = new JSONObject();
		
		try {
			jsonObj.put(OpHandler.OP_CODE, "unnotify");
			jsonObj.put(OpHandler.CACHE_NAME, cacheName);
			jsonObj.put(OpHandler.KEY, key);
		} catch (JSONException e) {
			throw new RuntimeException("JSON Exception", e);
		}
		
		return jsonObj;
	}

   public void stop() {
      cacheContainer.stop();
   }

}
