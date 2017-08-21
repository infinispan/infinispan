package org.infinispan.lock;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.Callable;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Condition;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * Test for stale remote locks on invalidation mode caches with pessimistic transactions.
 * See https://issues.jboss.org/browse/ISPN-2549.
 *
 * @author anistor@redhat.com
 * @since 5.3
 */
@Test(groups = "functional", testName = "lock.InvalidationModePessimisticLockReleaseTest")
public class InvalidationModePessimisticLockReleaseTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.INVALIDATION_SYNC, true, true);
      builder.transaction().useSynchronization(false)
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .useLockStriping(false);

      createCluster(builder, 2);
      waitForClusterToForm();
   }

   public void testStaleRemoteLocks() throws Exception {
      // put two keys on node 1
      TestingUtil.withTx(tm(1), new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            cache(1).put(1, "val_1");
            cache(1).put(2, "val_2");
            return null;
         }
      });

      assertValue(1, 1, "val_1");
      assertValue(1, 2, "val_2");

      // assert that no locks remain on node 1
      assertFalse(checkLocked(1, 1));
      assertFalse(checkLocked(1, 2));

      // assert that no locks remain on node 0 (need to wait a bit for tx completion notifications to be processed async)
      Eventually.eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return !checkLocked(0, 1) && !checkLocked(0, 2);
         }
      });

      // try to modify a key. this should not fail due to residual locks.
      cache(0).put(1, "new_val_1");

      // assert expected values on each node
      assertValue(0, 1, "new_val_1");
      assertValue(1, 1, null);
      assertValue(1, 2, "val_2");
   }

   private void assertValue(int nodeIndex, Object key, Object expectedValue) {
      assertEquals(expectedValue, cache(nodeIndex).get(key));
   }
}
