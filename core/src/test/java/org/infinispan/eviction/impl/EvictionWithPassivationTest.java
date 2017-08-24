package org.infinispan.eviction.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Collections;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionThreadPolicy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.EvictionWithPassivationTest")
public class EvictionWithPassivationTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "testCache";

   public EvictionWithPassivationTest() {
      // Cleanup needs to be after method, else LIRS can cause failures due to it not caching values due to hot
      // size being equal to full container size
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private final int EVICTION_MAX_ENTRIES = 2;

   private ConfigurationBuilder buildCfg() {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
            .persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class).purgeOnStartup(true)
            .invocationBatching().enable();
      cfg.memory().size(EVICTION_MAX_ENTRIES);
      return cfg;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(true));
      cacheManager.defineConfiguration(CACHE_NAME, buildCfg().build());
      return cacheManager;
   }

   public void testBasicStore() {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);
      testCache.clear();
      testCache.put("X", "4567");
      testCache.put("Y", "4568");
      testCache.put("Z", "4569");

      assertEquals("4567", testCache.get("X"));
      assertEquals("4568", testCache.get("Y"));
      assertEquals("4569", testCache.get("Z"));

      for (int i = 0; i < 10; i++) {
         testCache.getAdvancedCache().startBatch();
         String k = "A" + i;
         testCache.put(k, k);
         k = "B" + i;
         testCache.put(k, k);
         testCache.getAdvancedCache().endBatch(true);
      }

      for (int i = 0; i < 10; i++) {
         String k = "A" + i;
         assertEquals(k, testCache.get(k));
         k = "B" + i;
         assertEquals(k, testCache.get(k));
      }
   }

   public void testActivationInBatchRolledBack() {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }


   public void testActivationWithAnotherConcurrentRequest() throws Exception {
      final Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "Y";
      final String value = "4568";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      assertEquals(value, testCache.get(key));

      // Another thread should be able to see the data as well!
      Future<String> future = testCache.getAsync(key);

      assertEquals(value, future.get(10, TimeUnit.SECONDS));

      assertEquals(value, testCache.get(key));

      testCache.endBatch(true);

      // Lastly try the retrieval after batch was committed
      assertEquals(value, testCache.get(key));
   }

   public void testActivationPendingTransactionDoesNotAffectOthers() throws Throwable {
      final String previousValue = "prev-value";
      final Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      testCache.clear();

      final String key = "Y";
      final String value;

      if (previousValue != null) {
         testCache.put(key, previousValue);
         value = previousValue + "4568";
      } else {
         value = "4568";
      }

      // evict so it is in the loader but not in data container
      testCache.evict(key);

      testCache.startBatch();

      try {
         if (previousValue != null) {
            assertEquals(previousValue, testCache.put(key, value));
         } else {
            assertNull(testCache.put(key, value));
         }

         // In tx we should see new value
         assertEquals(value, testCache.get(key));

         // The spawned thread shouldn't see the new value yet, should see the old one still
         Future<String> future = fork(() -> testCache.get(key));

         if (previousValue != null) {
            assertEquals(previousValue, future.get(10000, TimeUnit.SECONDS));
         } else {
            assertNull(future.get(10, TimeUnit.SECONDS));
         }
      } catch (Throwable e) {
         testCache.endBatch(false);
         throw e;
      }

      testCache.endBatch(true);

      assertEquals(value, testCache.get(key));
   }

   public void testActivationPutAllInBatchRolledBack() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache(CACHE_NAME);

      final String key = "X";
      final String value = "4567";

      testCache.clear();
      testCache.put(key, value);

      testCache.evict(key);

      // Now make sure the act of activation for the entry is not tied to the transaction
      testCache.startBatch();
      testCache.putAll(Collections.singletonMap(key, value + "-putall"));
      testCache.endBatch(false);

      // The data should still be present even if a rollback occurred
      assertEquals(value, testCache.get(key));
   }
}
