/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.replication;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests for lock API
 * <p/>
 * Introduce lock() API methods https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author <a href="mailto:vblagoje@redhat.com">Vladimir Blagojevic (vblagoje@redhat.com)</a>
 */
@Test(groups = "functional", testName = "replication.SyncReplLockingTest")
public class SyncReplLockingTest extends MultipleCacheManagersTest {
   String k = "key", v = "value";

   protected void createCacheManagers() throws Throwable {
      Configuration replSync = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC, true);
      replSync.setLockAcquisitionTimeout(500);
      createClusteredCaches(2, "replSync", replSync);
   }

   public void testLocksReleasedWithoutExplicitUnlock() throws Exception {
      locksReleasedWithoutExplicitUnlockHelper(false, false);
      locksReleasedWithoutExplicitUnlockHelper(true, false);
      locksReleasedWithoutExplicitUnlockHelper(false, true);
      locksReleasedWithoutExplicitUnlockHelper(true, true);
   }

   public void testConcurrentNonTxLocking() throws Exception {
      concurrentLockingHelper(false, false);
      concurrentLockingHelper(true, false);
   }

   public void testConcurrentTxLocking() throws Exception {
      concurrentLockingHelper(false, true);
      concurrentLockingHelper(true, true);
   }


   public void testLocksReleasedWithNoMods() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.getAdvancedCache().lock(k);

      //do a dummy read
      cache1.get(k);
      mgr.commit();

      assertNoLocks(cache1);
      assertNoLocks(cache2);

      assert cache1.isEmpty();
      assert cache2.isEmpty();
      cache1.clear();
      cache2.clear();
   }
   
   public void testReplaceNonExistentKey() throws Exception {
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);
     
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.getAdvancedCache().lock(k);

      //do a replace on empty key
      //https://jira.jboss.org/browse/ISPN-514
      Object old = cache1.replace(k, "blah");
      assertNull("Should be null", cache1.get(k));
      
      boolean replaced = cache1.replace(k, "Vladimir","Blagojevic");
      assert !replaced;
      
      assertNull("Should be null", cache1.get(k));
      mgr.commit();

      assertNoLocks(cache1);
      assertNoLocks(cache2);

      assert cache1.isEmpty();
      assert cache2.isEmpty();
      cache1.clear();
      cache2.clear();
   }
   
   private void concurrentLockingHelper(final boolean sameNode, final boolean useTx)
         throws Exception {
      final Cache cache1 = cache(0, "replSync");
      final Cache cache2 = cache(1, "replSync");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));
      final CountDownLatch latch = new CountDownLatch(1);

      Thread t = new Thread() {
         @Override
         public void run() {
            log.info("Concurrent " + (useTx ? "tx" : "non-tx") + " write started "
                  + (sameNode ? "on same node..." : "on a different node..."));
            TransactionManager mgr = null;
            try {
               if (useTx) {
                  mgr = TestingUtil.getTransactionManager(sameNode ? cache1 : cache2);
                  mgr.begin();
               }
               if (sameNode) {
                  cache1.put(k, "JBC");
               } else {
                  cache2.put(k, "JBC");
               }
            } catch (Exception e) {
               if (useTx) {
                  try {
                     mgr.commit();
                  } catch (Exception e1) {
                  }
               }
               latch.countDown();
            }
         }
      };

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      // lock node and start other thread whose write should now block
      cache1.getAdvancedCache().lock(k);
      t.start();

      // wait till the put in thread t times out
      assert latch.await(1, TimeUnit.SECONDS) : "Concurrent put didn't time out!";

      cache1.put(k, name);
      mgr.commit();

      t.join();

      cache2.remove(k);
      assert cache1.isEmpty();
      assert cache2.isEmpty();
      cache1.clear();
      cache2.clear();
   }

   private void locksReleasedWithoutExplicitUnlockHelper(boolean lockPriorToPut, boolean useCommit)
         throws Exception {
      
      Cache cache1 = cache(0, "replSync");
      Cache cache2 = cache(1, "replSync");
      
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertNull("Should be null", cache1.get(k));
      assertNull("Should be null", cache2.get(k));

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();
      if (lockPriorToPut)
         cache1.getAdvancedCache().lock(k);
      cache1.put(k, name);
      if (!lockPriorToPut)
         cache1.getAdvancedCache().lock(k);

      if (useCommit)
         mgr.commit();
      else
         mgr.rollback();

      if (useCommit) {
         assertEquals(name, cache1.get(k));
         assertEquals("Should have replicated", name, cache2.get(k));
      } else {
         assertEquals(null, cache1.get(k));
         assertEquals("Should not have replicated", null, cache2.get(k));
      }

      cache2.remove(k);
      assert cache1.isEmpty();
      assert cache2.isEmpty();
      cache1.clear();
      cache2.clear();
   }

   @SuppressWarnings("unchecked")
   protected void assertNoLocks(Cache cache) {
      LockManager lm = TestingUtil.extractLockManager(cache);
      for (Object key : cache.keySet()) assert !lm.isLocked(key);
   }
}
