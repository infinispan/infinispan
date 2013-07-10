package org.infinispan.loaders;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.loaders.spi.CacheStore;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import java.util.List;

@Test (testName = "loaders.SharedCacheStoreTest", groups = "functional")
@CleanupAfterMethod
public class SharedCacheStoreTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg
         .loaders()
            .shared(true)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class)
               .storeName(SharedCacheStoreTest.class.getName())
               .purgeOnStartup(false)
         .clustering()
            .cacheMode(CacheMode.REPL_SYNC)
         .build();
      createCluster(cfg, 3);
      // don't create the caches here, we want them to join the cluster one by one
   }

   public void testUnnecessaryWrites() throws CacheLoaderException {
      cache(0).put("key", "value");

      // the second and third cache are only started here
      // so state transfer will copy the key to the other caches
      // however is should not write it to the cache store again
      for (Cache<Object, Object> c: caches())
         assert "value".equals(c.get("key"));

      List<CacheStore> cachestores = TestingUtil.cachestores(caches());
      for (CacheStore cs: cachestores) {
         assert cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("clear") == 0: "Cache store should not be cleared, purgeOnStartup is false";
         assert dimcs.stats().get("store") == 1: "Cache store should have been written to just once, but was written to " + dimcs.stats().get("store") + " times";
      }

      cache(0).remove("key");

      for (Cache<Object, Object> c: caches())
         assert c.get("key") == null;

      for (CacheStore cs: cachestores) {
         assert !cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("remove") == 1 : "Entry should have been removed from the cache store just once, but was removed " + dimcs.stats().get("remove") + " times";
      }
   }

   public void testSkipSharedCacheStoreFlagUsage() throws CacheLoaderException {
      cache(0).getAdvancedCache().withFlags(Flag.SKIP_SHARED_CACHE_STORE).put("key", "value");
      assert cache(0).get("key").equals("value");

      List<CacheStore> cachestores = TestingUtil.cachestores(caches());
      for (CacheStore cs : cachestores) {
         assert !cs.containsKey("key");
         DummyInMemoryCacheStore dimcs = (DummyInMemoryCacheStore) cs;
         assert dimcs.stats().get("store") == 0 : "Cache store should NOT contain any entry. Put was with SKIP_SHARED_CACHE_STORE flag.";
      }
   }

}
