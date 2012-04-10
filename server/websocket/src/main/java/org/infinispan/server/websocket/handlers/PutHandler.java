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
import org.infinispan.server.websocket.OpHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
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
