package org.infinispan.atomic;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.LocalDeltaAwarePassivationTest")
@CleanupAfterMethod
public class LocalDeltaAwarePassivationTest extends LocalDeltaAwareEvictionTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configBuilder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      configBuilder.eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .loaders().passivation(true).addStore().cacheStore(new DummyInMemoryCacheStore());

      addClusterEnabledCacheManager(configBuilder);
   }

   protected void assertNumberOfEntries(int cacheIndex) throws Exception {
      CacheStore cacheStore = TestingUtil.extractComponent(cache(cacheIndex), CacheLoaderManager.class).getCacheStore();
      assertEquals(1, cacheStore.loadAllKeys(null).size()); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
