package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.LockReleaseWithNoWriteTest")
public class LockReleaseWithNoWriteTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(false);
      createCluster(dcc, 2);
      waitForClusterToForm();
   }

   public void testLocksReleased1() throws Exception {
      runtTest(1, 0);
   }
   public void testLocksReleased2() throws Exception {
      runtTest(1, 1);
   }
   public void testLocksReleased3() throws Exception {
      runtTest(0, 0);
   }
   public void testLocksReleased4() throws Exception {
      runtTest(0, 1);
   }

   private void runtTest(int lockOwner, int txOwner) throws NotSupportedException, SystemException, RollbackException, HeuristicMixedException, HeuristicRollbackException {
      Object key = getKeyForCache(lockOwner);
      tm(txOwner).begin();
      advancedCache(txOwner).lock(key);
      tm(txOwner).commit();

      assertNotLocked(key);
   }
}
