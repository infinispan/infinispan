package org.infinispan.server.websocket.handlers;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.Cache;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.server.websocket.CacheListener;
import org.infinispan.server.websocket.CacheListener.ChannelNotifyParams;
import org.infinispan.server.websocket.ChannelUtils;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.server.websocket.json.JsonObject;

import java.util.Map;

/**
 * Handler for the "notify" and "unnotify" operations.
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class NotifyHandler implements OpHandler {

	private Map<Cache, CacheListener> listeners = CollectionFactory.makeConcurrentMap();

	@Override
   public void handleOp(JsonObject opPayload, Cache<Object, Object> cache, ChannelHandlerContext ctx) {
		String opCode = (String) opPayload.get(OpHandler.OP_CODE);
		String key = (String) opPayload.get(OpHandler.KEY);
		String[] onEvents = (String[]) opPayload.get("onEvents");
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
			ChannelNotifyParams notifyParams = new ChannelNotifyParams(ctx.channel(), keyToken, onEvents);
			
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
