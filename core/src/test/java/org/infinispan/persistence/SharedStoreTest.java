package org.infinispan.persistence;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.util.List;

@Test (testName = "persistence.SharedStoreTest", groups = "functional")
@CleanupAfterMethod
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
            .cacheMode(CacheMode.REPL_SYNC)
         .build();
      createCluster(cfg, 3);
      // don't create the caches here, we want them to join the cluster one by one
   }

   public void testUnnecessaryWrites() throws PersistenceException {
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
         assert !cs.contains("key");
         DummyInMemoryStore dimcs = (DummyInMemoryStore) cs;
         assert dimcs.stats().get("delete") == 1 : "Entry should have been removed from the cache store just once, but was removed " + dimcs.stats().get("remove") + " times";
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
