package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

/**
 * Test the failures after lock acquired for Optimistic transactional caches.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "lock.OptimisticTxFailureAfterLockingTest")
@CleanupAfterMethod
public class OptimisticTxFailureAfterLockingTest extends MultipleCacheManagersTest {

   /**
    * ISPN-3556
    */
   public void testInOwner() throws Exception {
      //primary owner is cache(0) and the failure is executed in cache(0)
      doTest(0, 0);
   }

   /**
    * ISPN-3556
    */
   public void testInNonOwner() throws Exception {
      //primary owner is cache(1) and the failure is executed in cache(0)
      doTest(1, 0);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true);
      builder.transaction()
            .lockingMode(LockingMode.OPTIMISTIC);
      builder.clustering().hash()
            .numOwners(2);
      builder.versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE);
      createClusteredCaches(3, builder);
   }

   private void doTest(int primaryOwnerIndex, int execIndex) throws Exception {
      final Object key = new MagicKey(cache(primaryOwnerIndex), cache(2));

      cache(primaryOwnerIndex).put(key, "v1");

      tm(execIndex).begin();
      AssertJUnit.assertEquals("v1", cache(execIndex).get(key));
      final Transaction transaction = tm(execIndex).suspend();

      cache(primaryOwnerIndex).put(key, "v2");

      tm(execIndex).resume(transaction);
      AssertJUnit.assertEquals("v1", cache(execIndex).put(key, "v3"));
      try {
         tm(execIndex).commit();
         AssertJUnit.fail("Exception expected!");
      } catch (RollbackException e) {
         //expected
      }

      assertNoTransactions();
      assertNotLocked(cache(primaryOwnerIndex), key);
   }
}
