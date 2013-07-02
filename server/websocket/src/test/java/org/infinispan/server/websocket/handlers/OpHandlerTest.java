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
      try {
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
      } finally {
         firstCacheClient.stop();
      }
   }
}
