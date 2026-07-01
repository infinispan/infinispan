package org.infinispan.server.functional.rest;

import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertResponse;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * Tests that REST GET with the {@code flags: PEEK} header works correctly,
 * returning entry values without resetting the max-idle timer.
 *
 * @author William Burns
 * @since 16.3
 */
public class RestPeekOperations {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   private static final Map<String, String> PEEK_HEADER = Map.of("flags", "PEEK");

   @Test
   public void testGetWithPeekReturnsValue() {
      RestClient client = SERVERS.rest().create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      assertStatus(NO_CONTENT, cache.post("k1", "v1"));
      assertResponse(OK, cache.get("k1", PEEK_HEADER), r -> assertEquals("v1", r.body()));
   }

   @Test
   public void testGetWithPeekNonExistentKey() {
      RestClient client = SERVERS.rest().create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      assertStatus(NOT_FOUND, cache.get("no-such-key", PEEK_HEADER));
   }

   @Test
   public void testGetWithPeekDoesNotResetMaxIdle() throws InterruptedException {
      RestClient client = SERVERS.rest().create();
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      assertStatus(NO_CONTENT, cache.post("idle-key", "value", -1, 2));

      // GET with PEEK repeatedly — should NOT reset the 2-second max-idle
      for (int i = 0; i < 5; i++) {
         assertResponse(OK, cache.get("idle-key", PEEK_HEADER), r -> assertEquals("value", r.body()));
         Thread.sleep(500);
      }

      // Entry should have expired because PEEK did not reset the max-idle
      assertStatus(NOT_FOUND, cache.get("idle-key"));
   }
}
