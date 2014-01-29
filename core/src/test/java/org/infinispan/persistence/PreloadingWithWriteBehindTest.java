package org.infinispan.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.loaders.decorators.AsyncStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "persistence.PreloadingWithWriteBehindTest")
public class PreloadingWithWriteBehindTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dccc.loaders().preload(true)
            .addStore(DummyInMemoryCacheStoreConfigurationBuilder.class).storeName("PreloadingWithWriteBehindTest")
               .async().enabled(true);

      return TestCacheManagerFactory.createCacheManager(dccc);
   }

   public void testPreload() {
      cache.put("k1","v1");
      cache.put("k2","v2");
      cache.put("k3","v3");
      getDummyLoader().clearStats();
      cache.stop();
      cache.start();
      assertEquals(3, cache.size());
      Integer loads = getDummyLoader().stats().get("load");
      assertEquals((Integer)0, loads);
   }

   private DummyInMemoryCacheStore getDummyLoader() {
      AsyncStore asyncStore = (AsyncStore) TestingUtil.getCacheLoader(cache);
      return (DummyInMemoryCacheStore) AsyncStore.undelegateCacheLoader(asyncStore);
   }
}
