package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test (testName = "persistence.SharedStoreTest", groups = "functional")
@CleanupAfterMethod
@InCacheMode({CacheMode.REPL_SYNC, CacheMode.SCATTERED_SYNC})
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
      // Because the store is shared, the stats are not cleared between methods
      // In particular, the clear between methods is added to the statistics
      List<CacheLoader<Object, Object>> cachestores = TestingUtil.cachestores(caches());
      super.clearContent();
      cachestores.forEach(store -> ((DummyInMemoryStore) store).clearStats());
   }

   public void testUnnecessaryWrites() throws PersistenceException {
      // The first cache is created here.
      cache(0).put("key", "value");

      // the second and third cache are only started here
      // so state transfer will copy the key to the other caches
      // however is should not write it to the cache store again
      for (Cache<Object, Object> c: caches())
         assert "value".equals(c.get("key"));

      List<CacheLoader<Object, Object>> cacheStores = TestingUtil.cachestores(caches());
      for (CacheLoader cs: cacheStores) {
         assert cs.contains("key");
         DummyInMemoryStore dimcs = (DummyInMemoryStore) cs;
         assert dimcs.stats().get("clear") == 0: "Cache store should not be cleared, purgeOnStartup is false";
         assert dimcs.stats().get("write") == 1: "Cache store should have been written to just once, but was written to " + dimcs.stats().get("write") + " times";
      }

      cache(0).remove("key");

      for (Cache<Object, Object> c: caches())
         assert c.get("key") == null;

      for (CacheLoader cs: cacheStores) {
         DummyInMemoryStore dimcs = (DummyInMemoryStore) cs;
         if (cacheMode.isScattered()) {
            // scattered cache leaves tombstones
            MarshalledEntry entry = cs.load("key");
            assert entry == null || entry.getValue() == null;
            assertEquals("Entry should have been replaced by tombstone", Integer.valueOf(2), dimcs.stats().get("write"));
         } else {
            assert !cs.contains("key");
            assertEquals("Entry should have been removed from the cache store just once", Integer.valueOf(1), dimcs.stats().get("delete"));
         }
      }
   }

   public void testSkipSharedCacheStoreFlagUsage() throws PersistenceException {
      cache(0).getAdvancedCache().withFlags(Flag.SKIP_SHARED_CACHE_STORE).put("key", "value");
      assert cache(0).get("key").equals("value");

      List<CacheLoader<Object, Object>> cachestores = TestingUtil.cachestores(caches());
      for (CacheLoader cs : cachestores) {
         assert !cs.contains("key");
         DummyInMemoryStore dimcs = (DummyInMemoryStore) cs;
         assert dimcs.stats().get("write") == 0 : "Cache store should NOT contain any entry. Put was with SKIP_SHARED_CACHE_STORE flag.";
      }
   }

}
