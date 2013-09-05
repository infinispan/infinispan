package org.infinispan.atomic;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
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
            .persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class);

      addClusterEnabledCacheManager(configBuilder);
   }

   @Override
   protected void assertNumberOfEntries(int cacheIndex) throws Exception {
      AdvancedCacheLoader cacheStore = (AdvancedCacheLoader) TestingUtil.getCacheLoader(cache(cacheIndex));
      assertEquals(1, PersistenceUtil.count(cacheStore, null)); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
