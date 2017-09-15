package org.infinispan.atomic;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "atomic.ReplDeltaAwarePassivationTest")
@CleanupAfterMethod
public class ReplDeltaAwarePassivationTest extends ReplDeltaAwareEvictionTest {

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new JBossStandaloneJTAManagerLookup())
            .memory().size(1)
            .clustering().hash().groups().enabled()
            .persistence().passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      builder.persistence().clearStores()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      waitForClusterToForm();
   }

   @Override
   protected void assertNumberOfEntries(int cacheIndex, DeltaAwareAccessor daa) throws Exception {
      AdvancedCacheLoader cacheStore = (AdvancedCacheLoader) TestingUtil.getCacheLoader(cache(cacheIndex));
      assertEquals(daa.isFineGrained() ? 5 : 1, PersistenceUtil.count(cacheStore, null)); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
