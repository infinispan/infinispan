package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.jupiter.InfinispanServer;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link RemoteCache#peek(Object)} works correctly over the HotRod protocol,
 * returning entry metadata without resetting the max-idle timer.
 *
 * @author William Burns
 * @since 16.3
 */
public class HotRodPeekOperations {

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   @Test
   public void testPeekReturnsMetadata() {
      org.infinispan.configuration.cache.ConfigurationBuilder serverCB = new org.infinispan.configuration.cache.ConfigurationBuilder();
      serverCB.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withServerConfiguration(serverCB)
            .create();

      cache.put("key", "value", 10, TimeUnit.MINUTES, 5, TimeUnit.MINUTES);

      MetadataValue<String> metadata = cache.peek("key");
      assertNotNull(metadata);
      assertEquals("value", metadata.getValue());
      assertEquals(TimeUnit.MINUTES.toSeconds(10), metadata.getLifespan());
      assertEquals(TimeUnit.MINUTES.toSeconds(5), metadata.getMaxIdle());
   }

   @Test
   public void testPeekNonExistentKey() {
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withCacheMode(CacheMode.DIST_SYNC)
            .create();

      assertNull(cache.peek("no-such-key"));
   }

   @Test
   public void testPeekDoesNotResetMaxIdle() {
      org.infinispan.configuration.cache.ConfigurationBuilder serverCB = new org.infinispan.configuration.cache.ConfigurationBuilder();
      serverCB.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withServerConfiguration(serverCB)
            .create();

      cache.put("idle-key", "value", -1, TimeUnit.SECONDS, 2, TimeUnit.SECONDS);

      // Peek repeatedly — should NOT reset the idle timer
      for (int i = 0; i < 5; i++) {
         MetadataValue<String> metadata = cache.peek("idle-key");
         assertNotNull(metadata, "Entry should exist during peek loop iteration " + i);
         try {
            Thread.sleep(500);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }

      // Entry should have expired because peek did not reset the 2-second max-idle
      assertNull(cache.get("idle-key"), "Entry should have expired since peek does not reset max-idle");
   }
}
