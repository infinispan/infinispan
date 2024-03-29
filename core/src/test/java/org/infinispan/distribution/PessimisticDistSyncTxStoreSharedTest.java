package org.infinispan.distribution;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests distributed caches with shared cache stores under transactional
 * environments.
 *
 * @author Thomas Fromm
 * @since 5.1
 */
@Test(groups = "functional", testName = "distribution.PessimisticDistSyncTxStoreSharedTest")
public class PessimisticDistSyncTxStoreSharedTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder getCB() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .remoteTimeout(60000)
            .stateTransfer().timeout(180000).fetchInMemoryState(true)
            .hash().numOwners(1);

      // transactions

      cb.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.PESSIMISTIC);

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
      defineConfigurationOnAllManagers("P006", getCB());
      waitForClusterToForm();
   }

   @Test
   public void testInvalidPut() {
      Cache<String, String> cache = cacheManagers.get(0).getCache("P006");
      IntSet allSegments = IntSets.immutableRangeSet(cache.getCacheConfiguration().clustering().hash().numSegments());

      // add 1st 4 elements
      for (int i = 0; i < 4; i++) {
         cache.put(cacheManagers.get(0).getAddress().toString() + "-" + i, "42");
      }

      // lets check if all elements arrived
      DummyInMemoryStore<String, String> cs1 = TestingUtil.getFirstStore(cache);
      Set<String> keys = PersistenceUtil.toKeySet(cs1, allSegments, null);

      Assert.assertEquals(keys.size(), 4);

      // now start 2nd node
      addClusterEnabledCacheManager(getCB()).defineConfiguration("P006", getCB().build());
      waitForClusterToForm("P006");

      cache = cacheManagers.get(1).getCache("P006");

      // add next 4 elements
      for (int i = 0; i < 4; i++) {
         cache.put(cacheManagers.get(1).getAddress().toString() + "-" + i, "42");
      }

      // add keys from all cache stores
      DummyInMemoryStore<String, String> cs2 = TestingUtil.getFirstStore(cache);
      log.debugf("Load from cache store via cache 1");
      Set<String> mergedKeys = new HashSet<>(PersistenceUtil.toKeySet(cs1, allSegments, null));
      log.debugf("Load from cache store via cache 2");
      mergedKeys.addAll(PersistenceUtil.toKeySet(cs2, allSegments, null));
      Assert.assertEquals(mergedKeys.size(), 8);
   }
}
