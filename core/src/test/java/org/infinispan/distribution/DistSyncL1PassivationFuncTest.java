package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Test(groups = "functional", testName = "distribution.DistSyncL1PassivationFuncTest")
public class DistSyncL1PassivationFuncTest extends BaseDistFunctionalTest {

   protected int MAX_ENTRIES = 4;

   protected AdvancedCacheLoader ownerCacheStore;
   protected AdvancedCacheLoader nonOwnerCacheStore;

   public DistSyncL1PassivationFuncTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      numOwners = 1;
      INIT_CLUSTER_SIZE = 2;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      ownerCacheStore = TestingUtil.extractComponent(cache(0, cacheName), PersistenceManager.class).getStores(DummyInMemoryStore.class).iterator().next();
      nonOwnerCacheStore = TestingUtil.extractComponent(cache(1, cacheName), PersistenceManager.class).getStores(DummyInMemoryStore.class).iterator().next();
   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      builder
            .eviction()
               .maxEntries(MAX_ENTRIES)
            .persistence()
               .passivation(true)
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return builder;
   }

   @Test
   public void testPassivatedL1Entries() {
      final int minPassivated = 2;
      final int insertCount = MAX_ENTRIES + minPassivated;
      List<MagicKey> keys = new ArrayList<MagicKey>(insertCount);
      Cache<MagicKey, Object> ownerCache = cache(0, cacheName);
      Cache<MagicKey, Object> nonOwnerCache = cache(1, cacheName);

      // Need to put 2+ magic keys to make sure we fill up the L1 on the local node
      for (int i = 0; i < insertCount; ++i) {

         // If the put worked then keep the key otherwise we need to generate a new one
         MagicKey key = new MagicKey(ownerCache);
         while (ownerCache.putIfAbsent(key, key) != null) {
            key = new MagicKey(ownerCache);
         }
         keys.add(key);
      }

      assertTrue(ownerCacheStore.size() >= minPassivated);
      assertTrue(MAX_ENTRIES >= ownerCache.getAdvancedCache().getDataContainer().size());

      assertEquals(0, nonOwnerCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).size());
      assertEquals(0, nonOwnerCacheStore.size());

      // Now load those keys in our non owner cache which should store them in L1
      for (MagicKey key : keys) {
         nonOwnerCache.get(key);
      }

      // L1 entries should not be passivated
      assertEquals("Some L1 values were passivated", 0, nonOwnerCacheStore.size());
   }
}
