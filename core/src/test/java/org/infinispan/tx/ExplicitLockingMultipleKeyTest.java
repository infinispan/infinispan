package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.ExplicitLockingMultipleKeyTest")
public class ExplicitLockingMultipleKeyTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder c = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(c, 2);
      waitForClusterToForm();
   }

   public void testAcquireRemoteLocks1() throws Exception {
      runTest(0, 1);
   }

   public void testAcquireRemoteLocks2() throws Exception {
      runTest(0, 0);
   }

   public void testAcquireRemoteLocks3() throws Exception {
      runTest(1, 1);
   }

   public void testAcquireRemoteLocks4() throws Exception {
      runTest(1, 0);
   }

   private void runTest(int lockOwner, int txOriginator) throws Exception {
      Object k0_1 = getKeyForCache(lockOwner);
      Object k0_2 = getKeyForCache(lockOwner);

      tm(txOriginator).begin();
      advancedCache(txOriginator).lock(k0_1, k0_2);
      assertKeyLockedCorrectly(k0_1);
      assertKeyLockedCorrectly(k0_2);
      tm(txOriginator).commit();
      assertNotLocked(k0_1);
      assertNotLocked(k0_2);
   }
}
