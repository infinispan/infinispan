package org.infinispan.assertions;

import org.infinispan.server.websocket.OpHandler;
import org.infinispan.server.websocket.json.JsonObject;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Custom assertion for easier testing OpHandlers
 *
 * @author Sebastian Laskawiec
 */
public class JsonPayloadAssertion {

   private JsonObject assertedNode;

   public JsonPayloadAssertion(JsonObject assertedNode) {
      this.assertedNode = assertedNode;
   }

   public static JsonPayloadAssertion assertThat(JsonObject assertedNode) {
      return new JsonPayloadAssertion(assertedNode);
   }

   public JsonPayloadAssertion hasCacheName(String cacheName) {
      assertEquals(cacheName, assertedNode.get(OpHandler.CACHE_NAME));
      return this;
   }

   public JsonPayloadAssertion hasKey(String key) {
      assertEquals(key, assertedNode.get(OpHandler.KEY));
      return this;
   }

   public JsonPayloadAssertion hasValue(Object value) {
      assertEquals(value, assertedNode.get(OpHandler.VALUE));
      return this;
   }

   public JsonPayloadAssertion hasMimeType(String mimeType) {
      assertEquals(mimeType, assertedNode.get(OpHandler.MIME));
      return this;
   }

   public JsonPayloadAssertion hasFields(String key, String value) {
      assertTrue(assertedNode.containsKey(key));
      assertEquals(value, assertedNode.get(key));
      return this;
   }
}
