package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import static org.jgroups.util.Util.assertNotNull;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(testName = "container.versioning.VersionedDistStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class VersionedDistStateTransferTest extends MultipleCacheManagersTest {
   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .l1()
                  .disable()
            .versioning()
               .enable()
               .scheme(VersioningScheme.SIMPLE)
            .locking()
               .isolationLevel(IsolationLevel.REPEATABLE_READ)
               .writeSkewCheck(true)
            .transaction()
               .lockingMode(LockingMode.OPTIMISTIC)
               .syncCommitPhase(true);

      amendConfig(builder);
      createCluster(builder, 4);
      waitForClusterToForm();
   }

   protected void amendConfig(ConfigurationBuilder builder) {
   }

   public void testStateTransfer() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);
      Cache<Object, Object> cache2 = cache(2);
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
      addClusterEnabledCacheManager(builder);
      Cache<Object, Object> cache4 = cache(4);

      log.debugf("Joiner started, checking transferred data");
      checkStateTransfer(keys, values);

      log.debugf("Stopping cache %s", cache3);
      manager(3).stop();
      // Eliminate the dead cache from the caches collection, cache4 now becomes cache(3)
      cacheManagers.remove(3);
      waitForClusterToForm();

      log.debugf("Leaver stopped, checking transferred data");
      checkStateTransfer(keys, values);

      // Cause a write skew
      for (int i = 0; i < NUM_KEYS; i++) {
         cache4.put(keys[i], "new " + values[i]);
      }

      for (int i = 0; i < NUM_KEYS; i++) {
         int cacheIndex = i % 3;
         log.tracef("Expecting a write skew failure for key %s on cache %s", keys[i], cache(cacheIndex));
         tm(cacheIndex).resume(txs[i]); {
            cache(cacheIndex).put(keys[i], "new new " + values[i]);
         }
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

   private void checkVersion(Cache<Object, Object> c, MagicKey hello) {
      Address address = c.getCacheManager().getAddress();
      ConsistentHash readConsistentHash = c.getAdvancedCache().getDistributionManager().getReadConsistentHash();
      if (readConsistentHash.isKeyLocalToNode(address, hello)) {
         InternalCacheEntry ice = c.getAdvancedCache().getDataContainer().get(hello);
         assertNotNull("Entry not found on owner cache " + c, ice);
         assertNotNull("Version is null on owner cache " + c, ice.getMetadata().version());
      }
   }
}
