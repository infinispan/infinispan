package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests distributed caches with shared cache stores under transactional
 * environments.
 *
 * @author Thomas Fromm
 * @since 5.1
 */
@Test(groups = "functional", testName = "distribution.PessimisticDistSyncTxStoreSharedTest")
public class PessimisticDistSyncTxStoreSharedTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder getCB(){
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .sync().replTimeout(60000)
            .stateTransfer().timeout(180000).fetchInMemoryState(true)
            .hash().numOwners(1);

      // transactions

      cb.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC)
            .syncCommitPhase(true)
            .syncRollbackPhase(true);

      // cb.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      cb.persistence().passivation(false);
      // Make it really shared by adding the test's name as store name
      cb.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class).preload(true).shared(true)
               .storeName(getClass().getSimpleName()).async()
            .disable();
      return cb;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getCB(), 1);
      waitForClusterToForm();
   }

   @Test
   public void testInvalidPut() throws Exception {
      Cache cache = cacheManagers.get(0).getCache("P006");

      // add 1st 4 elements
      for(int i = 0; i < 4; i++){
         cache.put(cacheManagers.get(0).getAddress().toString()+"-"+i, "42");
      }

      // lets check if all elements arrived
      AdvancedCacheLoader cs1 = (AdvancedCacheLoader) TestingUtil.getCacheLoader(cache);
      Set<Object> keys = PersistenceUtil.toKeySet(cs1, null);

      Assert.assertEquals(keys.size(), 4);

      // now start 2nd node
      addClusterEnabledCacheManager(getCB());
      waitForClusterToForm("P006");

      cache = cacheManagers.get(1).getCache("P006");

      // add next 4 elements
      for(int i = 0; i < 4; i++){
         cache.put(cacheManagers.get(1).getAddress().toString()+"-"+i, "42");
      }

      Set mergedKeys = new HashSet();
      // add keys from all cache stores
      AdvancedCacheLoader cs2 = (AdvancedCacheLoader) TestingUtil.getCacheLoader(cache);
      log.debugf("Load from cache store via cache 1");
      mergedKeys.addAll(PersistenceUtil.toKeySet(cs1, null));
      log.debugf("Load from cache store via cache 2");
      mergedKeys.addAll(PersistenceUtil.toKeySet(cs2, null));

      Assert.assertEquals(mergedKeys.size(), 8);

   }

}
