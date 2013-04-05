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
import org.infinispan.server.websocket.CacheListener;
import org.infinispan.server.websocket.CacheListener.ChannelNotifyParams;
import org.infinispan.server.websocket.ChannelUtils;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.util.CollectionFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Map;

/**
 * Handler for the "notify" and "unnotify" operations.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class NotifyHandler implements OpHandler {
	
	private Map<Cache, CacheListener> listeners = CollectionFactory.makeConcurrentMap();

	@Override
   public void handleOp(JSONObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) throws JSONException {
		String opCode = (String) opPayload.get(OpHandler.OP_CODE);
		String key = (String) opPayload.opt(OpHandler.KEY);
		String[] onEvents = (String[]) opPayload.opt("onEvents");
		CacheListener listener = listeners.get(cache);
		
		if(key == null) {
			// If key not specified... notify on all...
			key = "*";
		}
		
		if(listener == null) {
			synchronized (this) {
				listener = listeners.get(cache);
				if(listener == null) {
					listener = new CacheListener();
					listeners.put(cache, listener);	
					cache.addListener(listener);
				}
			}
		}
		
		String[] keyTokens = key.split(",");		
		for(String keyToken : keyTokens) {
			ChannelNotifyParams notifyParams = new ChannelNotifyParams(ctx.getChannel(), keyToken, onEvents);		
			
			if(opCode.equals("notify")) {
				listener.addChannel(notifyParams);
				// And push the value to the channel (if it's not wildcard)...
				if(!keyToken.equals("*")) {
					ChannelUtils.pushCacheValue(keyToken, cache, ctx);
				}
			} else if(opCode.equals("unnotify")) {
				listener.removeChannel(notifyParams);
			}
		}		
	}
}
