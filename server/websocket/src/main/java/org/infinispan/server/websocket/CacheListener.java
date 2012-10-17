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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.infinispan.Cache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Cache listener.
 * <p/>
 * Used to notify websocket clients of cache entry updates.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
@Listener
public class CacheListener {
	
	private List<ChannelNotifyParams> channels = new CopyOnWriteArrayList<ChannelNotifyParams>();

	@CacheEntryCreated
	public void cacheEntryCreated(CacheEntryCreatedEvent<Object, Object> event) {
		notifyChannels(event, event.getType());
	}

	@CacheEntryModified
	public void cacheEntryModified(CacheEntryModifiedEvent<Object, Object> event) {
		notifyChannels(event, event.getType());
	}

	@CacheEntryRemoved
	public void cacheEntryRemoved(CacheEntryRemovedEvent<Object, Object> event) {
		notifyChannels(event, event.getType());
	}
	
	private void notifyChannels(CacheEntryEvent<Object, Object> event, Event.Type eventType) {
		if(event.isPre()) {
			return;
		}
		
		JSONObject jsonObject;
		
		try {
			Cache<Object, Object> cache = event.getCache();
			Object key = event.getKey();
			Object value;
			
			switch(eventType) {
			case CACHE_ENTRY_CREATED:
				// TODO: Add optimization ... don't get from cache if non of the channels are interested in creates...
				value = cache.get(key);
				jsonObject = ChannelUtils.toJSON(key.toString(), value, cache.getName());
				break;
			case CACHE_ENTRY_MODIFIED:
				value = ((CacheEntryModifiedEvent<Object, Object>)event).getValue();
				jsonObject = ChannelUtils.toJSON(key.toString(), value, cache.getName());
				break;
			case CACHE_ENTRY_REMOVED:
				jsonObject = ChannelUtils.toJSON(key.toString(), null, cache.getName());
				break;
			default:
				return;	
			}
			
			jsonObject.put("eventType", eventType.toString());
		} catch (JSONException e) {
			return;
		}

		String jsonString = jsonObject.toString();
		for(ChannelNotifyParams channel : channels) {
			if(channel.channel.isOpen() && channel.onEvents.contains(eventType)) {
				if(channel.key != null) {
					if(event.getKey().equals(channel.key) || channel.key.equals("*")) {
						channel.channel.write(new TextWebSocketFrame(jsonString));
					}
				} else {					
					channel.channel.write(new TextWebSocketFrame(jsonString));
				}
			}
		}
	}
	
	public void addChannel(ChannelNotifyParams channel) {
		if(!channels.contains(channel)) {
			channels.add(channel);
			channel.channel.getCloseFuture().addListener(new ChannelCloseFutureListener());
		}
	}
	
	public void removeChannel(ChannelNotifyParams channel) {
		channels.remove(channel);
	}
	
	public static class ChannelNotifyParams {
		
		private static final String[] DEFAULT_EVENTS = {Event.Type.CACHE_ENTRY_MODIFIED.toString(), Event.Type.CACHE_ENTRY_REMOVED.toString()};
		
		private Channel channel;
		private String key;
		private List<Event.Type> onEvents = new ArrayList<Event.Type>();		
		
		public ChannelNotifyParams(Channel channel, String key, String[] onEvents) {
			if(channel == null) {
				throw new IllegalArgumentException("null 'channel' arg in constructor call.");
			}
			String[] onEventsSpec = onEvents;
			
			this.channel = channel;
			this.key = key;
			
			if(onEventsSpec ==  null) {
				onEventsSpec = DEFAULT_EVENTS;
			}
			for(String eventType : onEventsSpec) {
				try {
					this.onEvents.add(Event.Type.valueOf(eventType));
				} catch(RuntimeException e) {
					// Ignore for now
				}
			}
			
			if(onEvents == null && key.equals("*")) {
				this.onEvents.add(Event.Type.CACHE_ENTRY_CREATED);
			}			
		}

		@Override
		public boolean equals(Object obj) {
			if(obj instanceof ChannelNotifyParams) {
				ChannelNotifyParams channelNotifyParams = (ChannelNotifyParams) obj;
				if(channelNotifyParams.channel == channel) {
					if(key == null) {
						return (channelNotifyParams.key == null);
					} else {
						return key.equals(channelNotifyParams.key);
					}
				}
			}
			
			return false;
		}

		@Override
		public int hashCode() {
			if(key != null) {				
				return super.hashCode() + channel.hashCode() + key.hashCode();
			} else {				
				return super.hashCode() + channel.hashCode();
			}
		}
	}
	
	private class ChannelCloseFutureListener implements ChannelFutureListener {

		@Override
      public void operationComplete(ChannelFuture channelCloseFuture) throws Exception {
			for(ChannelNotifyParams channel : channels) {
				if(channelCloseFuture.getChannel() ==  channel.channel) {
					removeChannel(channel);
				}
			}
		}		
	}
}
