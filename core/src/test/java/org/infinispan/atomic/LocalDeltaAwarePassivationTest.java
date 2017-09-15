package org.infinispan.atomic;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
      configBuilder.memory().size(1)
            .persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class);
      configBuilder.clustering().hash().groups().enabled();
      addClusterEnabledCacheManager(configBuilder);
   }

   @Override
   protected void assertNumberOfEntries(int cacheIndex, DeltaAwareAccessor daa) throws Exception {
      AdvancedCacheLoader cacheStore = (AdvancedCacheLoader) TestingUtil.getCacheLoader(cache(cacheIndex));
      assertEquals(daa.isFineGrained() ? 5 : 1, PersistenceUtil.count(cacheStore, null)); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
