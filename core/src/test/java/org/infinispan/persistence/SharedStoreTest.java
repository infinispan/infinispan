package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test (testName = "persistence.SharedStoreTest", groups = "functional")
@CleanupAfterMethod
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC})
public class SharedStoreTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
         .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
               .storeName(SharedStoreTest.class.getName())
               .purgeOnStartup(false).shared(true)
         .clustering()
            .cacheMode(cacheMode)
         .build();
      createCluster(cfg, 3);
      // don't create the caches here, we want them to join the cluster one by one
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      List<DummyInMemoryStore> cachestores = TestingUtil.cachestores(caches());
      super.clearContent();
      // Because the store is shared, the stats are not cleared between methods
      // In particular, the clear between methods is added to the statistics
      clearStoreStats(cachestores);
   }

   private void clearStoreStats(List<DummyInMemoryStore> cachestores) {
      cachestores.forEach(DummyInMemoryStore::clearStats);
   }

   public void testUnnecessaryWrites() throws PersistenceException {
      // The first cache is created here.
      cache(0).put("key", "value");

      // the second and third cache are only started here
      // so state transfer will copy the key to the other caches
      // however is should not write it to the cache store again
      for (Cache<Object, Object> c: caches()) {
         assertEquals("value", c.get("key"));
      }

      List<DummyInMemoryStore> cacheStores = TestingUtil.cachestores(caches());
      for (DummyInMemoryStore dimcs: cacheStores) {
         assertTrue(dimcs.contains("key"));
         assertEquals(0, dimcs.stats().get("clear").intValue());
         assertEquals(0,  dimcs.stats().get("clear").intValue());
         assertEquals(1,  dimcs.stats().get("write").intValue());
      }

      cache(0).remove("key");

      for (Cache<Object, Object> c: caches()) {
         assertNull(c.get("key"));
      }

      for (DummyInMemoryStore dimcs: cacheStores) {
         assert !dimcs.contains("key");
         assertEquals("Entry should have been removed from the cache store just once", Integer.valueOf(1), dimcs.stats().get("delete"));
      }
   }

   public void testSkipSharedCacheStoreFlagUsage() throws PersistenceException {
      cache(0).getAdvancedCache().withFlags(Flag.SKIP_SHARED_CACHE_STORE).put("key", "value");
      assert cache(0).get("key").equals("value");

      List<DummyInMemoryStore> cacheStores = TestingUtil.cachestores(caches());
      for (DummyInMemoryStore dimcs: cacheStores) {
         assert !dimcs.contains("key");
         assert dimcs.stats().get("write") == 0 : "Cache store should NOT contain any entry. Put was with SKIP_SHARED_CACHE_STORE flag.";
      }
   }

   public void testSize() {
      // Force all the caches up
      List<Cache<String, String>> caches = caches();
      Cache<String, String> cache0 = caches.get(0);
      cache0.put("key", "value");
      clearStoreStats(TestingUtil.cachestores(caches()));

      assertEquals(1, cache0.size());

      // Stats are shared between nodes - so it shouldn't matter which, but there should only be 1 size invocation
      // and no other invocations
      assertStoreDistinctInvocationAmount(cache0, 1);
      assertStoreStatInvocationEquals(cache0, "size", 1);
   }

   private void assertStoreStatInvocationEquals(Cache<?, ?> cache, String invocationName, int invocationCount) {
      DummyInMemoryStore dims = TestingUtil.getFirstStore(cache);
      assertEquals(invocationCount, dims.stats().get(invocationName).intValue());
   }

   private void assertStoreDistinctInvocationAmount(Cache<?, ?> cache, int distinctInvocations) {
      DummyInMemoryStore dims = TestingUtil.getFirstStore(cache);
      assertEquals(distinctInvocations, dims.stats().values().stream().filter(i -> i > 0).count());
   }
}
