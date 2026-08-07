package org.infinispan.eviction.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests that {@link DataContainer#peek(Object)} does not protect entries from eviction.
 * Peek does not update eviction access tracking, so peeked entries should be evicted normally
 * based on their original access order.
 *
 * @author William Burns
 * @since 16.3
 */
@Test(groups = "functional", testName = "eviction.impl.PeekEvictionTest")
public class PeekEvictionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.memory().maxCount(4);
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testPeekDoesNotProtectFromEviction() {
      cache.put("k1", "v1");
      cache.put("k2", "v2");
      cache.put("k3", "v3");
      cache.put("k4", "v4");

      InternalDataContainer<Object, Object> dc = TestingUtil.extractComponent(cache, InternalDataContainer.class);

      // Peek k1 many times — this must NOT update eviction access order
      for (int i = 0; i < 10; i++) {
         InternalCacheEntry<Object, Object> entry = dc.peek("k1");
         assertNotNull(entry);
         assertEquals("v1", entry.getValue());
      }

      // Access k2, k3, k4 via regular get to make them recently used
      cache.get("k2");
      cache.get("k3");
      cache.get("k4");

      // Insert new entries to trigger eviction — k1 should be evicted since peek didn't refresh it
      cache.put("k5", "v5");
      cache.put("k6", "v6");

      assertNull(dc.peek("k1"), "k1 should have been evicted since peek does not update access order");
      assertNotNull(dc.peek("k3"));
      assertNotNull(dc.peek("k4"));
   }
}
