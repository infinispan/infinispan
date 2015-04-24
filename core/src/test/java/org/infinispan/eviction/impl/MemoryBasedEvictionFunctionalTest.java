package org.infinispan.eviction.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Random;

import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "eviction.MemoryBasedEvictionFunctionalTest")
public class MemoryBasedEvictionFunctionalTest extends SingleCacheManagerTest {

   private static final long CACHE_SIZE = 2000;

   protected MemoryBasedEvictionFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.eviction().maxEntries(CACHE_SIZE)
            .strategy(EvictionStrategy.LRU).memoryBasedApproximation(true);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      cache = cm.getCache();
      return cm;
   }

   public void testSimpleEvictionMaxEntries() throws Exception {
      int keyValueByteSize = 100;
      long numberInserted = CACHE_SIZE / 2 / keyValueByteSize;
      Random random = new Random();
      // Note that there is overhead for the map itself, so we will not get exactly the same amount
      // More than likely there will be a few hundred byte overhead
      for (long i = 0; i < numberInserted; i++) {
         byte[] key = new byte[keyValueByteSize];
         byte[] value = new byte[keyValueByteSize];
         random.nextBytes(key);
         random.nextBytes(value);
         cache.put(key, value);
      }
      assertTrue(cache.getAdvancedCache().getDataContainer().size() < numberInserted);
   }
}
