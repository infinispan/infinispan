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

import org.infinispan.server.websocket.OpHandler;
import org.infinispan.websocket.MockChannel;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.Test;
import org.testng.Assert;

/**
 * 
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
@Test (testName = "websocket.handlers.OpHandlerTest", groups = "unit")
public class OpHandlerTest {
	
	public void test() throws JSONException {
		MockChannel mockChannel = new MockChannel();
		MockClient firstCacheClient = new MockClient("firstCache", mockChannel);
		JSONObject jsonPayload;
 
		// Put...
		firstCacheClient.put("a", "aVal");
		firstCacheClient.put("b", "bVal");
		
		// Get...
		firstCacheClient.get("a");
		jsonPayload = mockChannel.getJSONPayload();
		Assert.assertEquals("firstCache", jsonPayload.get(OpHandler.CACHE_NAME));
		Assert.assertEquals("a", jsonPayload.get(OpHandler.KEY));
		Assert.assertEquals("aVal", jsonPayload.get(OpHandler.VALUE));
		Assert.assertEquals("text/plain", jsonPayload.get(OpHandler.MIME));		
		firstCacheClient.get("b");
		jsonPayload = mockChannel.getJSONPayload();
		Assert.assertEquals("firstCache", jsonPayload.get(OpHandler.CACHE_NAME));
		Assert.assertEquals("b", jsonPayload.get(OpHandler.KEY));
		Assert.assertEquals("bVal", jsonPayload.get(OpHandler.VALUE));
		Assert.assertEquals("text/plain", jsonPayload.get(OpHandler.MIME));
		firstCacheClient.get("x"); // not in cache
		jsonPayload = mockChannel.getJSONPayload();
		Assert.assertEquals("firstCache", jsonPayload.get(OpHandler.CACHE_NAME));
		Assert.assertEquals("x", jsonPayload.get(OpHandler.KEY));
		Assert.assertEquals(null, jsonPayload.get(OpHandler.VALUE));
		
		// Notify...
		firstCacheClient.notify("a");
		// Call to notify immediately pushes the value and then pushes it again later on modify...
		jsonPayload = mockChannel.getJSONPayload(1000);
		Assert.assertEquals("aVal", jsonPayload.get(OpHandler.VALUE));
		// Modify the value should result in a push notification...
		firstCacheClient.getCache().put("a", "aNewValue");
		jsonPayload = mockChannel.getJSONPayload();
		Assert.assertEquals("aNewValue", jsonPayload.get(OpHandler.VALUE));
		// Modify something we're not listening to... nothing should happen...
		firstCacheClient.getCache().put("b", "bNewValue");
		try {
			mockChannel.getJSONPayload(500);
			Assert.fail("Expected timeout");
		} catch (RuntimeException e) {
			Assert.assertEquals("Timed out waiting for data to be pushed onto the channel.", e.getMessage());
		}		
		
		// Remove...
		firstCacheClient.remove("a");
		firstCacheClient.get("a");
		jsonPayload = mockChannel.getJSONPayload();
		Assert.assertEquals("firstCache", jsonPayload.get(OpHandler.CACHE_NAME));
		Assert.assertEquals("a", jsonPayload.get(OpHandler.KEY));
		Assert.assertEquals(null, jsonPayload.get(OpHandler.VALUE));
	}	
}
