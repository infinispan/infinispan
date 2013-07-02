package org.infinispan.atomic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.junit.Assert;
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
            .eviction().maxEntries(1).strategy(EvictionStrategy.LRU)
            .loaders().passivation(true)
            .addStore().cacheStore(new DummyInMemoryCacheStore())
            .fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      builder.loaders().clearCacheLoaders()
            .addStore().cacheStore(new DummyInMemoryCacheStore()).fetchPersistentState(false);

      addClusterEnabledCacheManager(builder);

      waitForClusterToForm();
   }

   protected void assertNumberOfEntries(int cacheIndex) throws Exception {
      CacheStore cacheStore = TestingUtil.extractComponent(cache(cacheIndex), CacheLoaderManager.class).getCacheStore();
      Assert.assertEquals(1, cacheStore.loadAllKeys(null).size()); // one entry in store

      DataContainer dataContainer = cache(cacheIndex).getAdvancedCache().getDataContainer();
      Assert.assertEquals(1, dataContainer.size());        // only one entry in memory (the other one was evicted)
   }
}
