package org.infinispan.persistence;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.async.AdvancedAsyncCacheLoader;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "persistence.PreloadingWithWriteBehindTest")
public class PreloadingWithWriteBehindTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.LOCAL);
      DummyInMemoryStoreConfigurationBuilder discb = dccc.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      dccc.transaction().cacheStopTimeout(50, TimeUnit.SECONDS);
      discb
            .async().enabled(true)
            .preload(true)
            .storeName("PreloadingWithWriteBehindTest");
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

   private DummyInMemoryStore getDummyLoader() {
      return (DummyInMemoryStore) ((AdvancedAsyncCacheLoader)TestingUtil.getCacheLoader(cache)).undelegate();
   }
}
