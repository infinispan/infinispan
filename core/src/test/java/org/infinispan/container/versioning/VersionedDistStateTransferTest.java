package org.infinispan.container.versioning;

import static org.infinispan.transaction.impl.WriteSkewHelper.versionFromEntry;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Transaction;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(testName = "container.versioning.VersionedDistStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class VersionedDistStateTransferTest extends MultipleCacheManagersTest {
   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder.clustering().cacheMode(CacheMode.DIST_SYNC).l1().disable()
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
            .transaction().lockingMode(LockingMode.OPTIMISTIC);

      createCluster(TestDataSCI.INSTANCE, builder, 4);
      waitForClusterToForm();
   }

   public void testStateTransfer() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache3 = cache(3);

      int NUM_KEYS = 20;
      MagicKey[] keys = new MagicKey[NUM_KEYS];
      String[] values = new String[NUM_KEYS];
      for (int i = 0; i < NUM_KEYS; i++) {
         // Put the entries on the cache that will be killed
         keys[i] = new MagicKey("key" + i, cache3);
         values[i] = "v" + i;
         cache0.put(keys[i], values[i]);
      }

      // no state transfer per se, but we check that the initial data is ok
      checkStateTransfer(keys, values);

      Transaction[] txs = new Transaction[NUM_KEYS];
      for (int i = 0; i < NUM_KEYS; i++) {
         int cacheIndex = i % 3;
         tm(cacheIndex).begin(); {
            assertEquals(values[i], cache(cacheIndex).get(keys[i]));
         }
         txs[i] = tm(cacheIndex).suspend();
      }

      log.debugf("Starting joiner");
      addClusterEnabledCacheManager(TestDataSCI.INSTANCE, builder);
      Cache<Object, Object> cache4 = cache(4);

      log.debugf("Joiner started, checking transferred data");
      checkStateTransfer(keys, values);

      log.debugf("Stopping cache %s", cache3);
      manager(3).stop();
      // Eliminate the dead cache from the caches collection, cache4 now becomes cache(3)
      cacheManagers.remove(3);
      TestingUtil.waitForNoRebalance(caches());

      log.debugf("Leaver stopped, checking transferred data");
      checkStateTransfer(keys, values);

      // Cause a write skew
      for (int i = 0; i < NUM_KEYS; i++) {
         cache4.put(keys[i], "new " + values[i]);
      }

      for (int i = 0; i < NUM_KEYS; i++) {
         int cacheIndex = i % 3;
         log.tracef("Expecting a write skew failure for key %s on cache %s", keys[i], cache(cacheIndex));
         tm(cacheIndex).resume(txs[i]);
         cache(cacheIndex).put(keys[i], "new new " + values[i]);

         try {
            tm(cacheIndex).commit();
            fail("The write skew check should have failed!");
         } catch (RollbackException expected) {
            // Expected
         }
      }

      for (int cacheIndex = 0; cacheIndex < 4; cacheIndex++) {
         for (int i = 0; i < NUM_KEYS; i++) {
            assertEquals("Wrong value found on cache " + cache(cacheIndex),
                  "new " + values[i], cache(cacheIndex).get(keys[i]));
         }
      }
   }

   private void checkStateTransfer(MagicKey[] keys, String[] values) {
      for (Cache<Object, Object> c: caches()) {
         for (int i = 0; i < keys.length; i++) {
            assertEquals("Wrong value found on cache " + c, values[i], c.get(keys[i]));
            checkVersion(c, keys[i]);
         }
      }
   }

   private void checkVersion(Cache<Object, Object> c, MagicKey key) {
      LocalizedCacheTopology topology = c.getAdvancedCache().getDistributionManager().getCacheTopology();
      if (topology.isReadOwner(key)) {
         InternalCacheEntry<Object, Object> ice = c.getAdvancedCache().getDataContainer().peek(key);
         assertNotNull("Entry not found on owner cache " + c, ice);
         assertNotNull("Version is null on owner cache " + c, versionFromEntry(ice));
      }
   }
}
