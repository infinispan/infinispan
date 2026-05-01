package org.infinispan.replication;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.TimeoutException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * Tests for lock API
  * Introduce lock() API methods https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 */
@Test(groups = "functional", testName = "replication.SyncLockingTest")
@InCacheMode({ CacheMode.DIST_SYNC, CacheMode.REPL_SYNC })
public class SyncLockingTest extends MultipleCacheManagersTest {
   private String k = "key", v = "value";

   public SyncLockingTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }


   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(cacheMode, true);
      cfg.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.PESSIMISTIC)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createClusteredCaches(2, "testcache", cfg);
      waitForClusterToForm("testcache");
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
      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertThat(cache1.get(k)).isNull();
      assertThat(cache2.get(k)).isNull();

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.getAdvancedCache().lock(k);

      //do a dummy read
      cache1.get(k);
      mgr.commit();

      assertEventuallyNotLocked(cache1, "testcache");
      assertEventuallyNotLocked(cache2, "testcache");

      assertThat(cache1).isEmpty();
      assertThat(cache2).isEmpty();
      cache1.clear();
      cache2.clear();
   }

   public void testReplaceNonExistentKey() throws Exception {
      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      cache1.getAdvancedCache().lock(k);

      // do a replace on empty key
      // https://jira.jboss.org/browse/ISPN-514
      Object old = cache1.replace(k, "blah");
      assertThat(cache1.get(k)).isNull();

      boolean replaced = cache1.replace(k, "Vladimir", "Blagojevic");
      assertThat(replaced).isFalse();

      assertThat(cache1.get(k)).isNull();
      mgr.commit();

      assertEventuallyNotLocked(cache1, "testcache");
      assertEventuallyNotLocked(cache2, "testcache");

      assertThat(cache1).isEmpty();
      assertThat(cache2).isEmpty();
      cache1.clear();
      cache2.clear();
   }

   private void concurrentLockingHelper(final boolean sameNode, final boolean useTx) throws Exception {
      log.debugf("sameNode=%s, useTx=%s", sameNode, useTx);
      final Cache cache1 = cache(0, "testcache");
      final Cache cache2 = cache(1, "testcache");
      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertThat(cache1.get(k)).isNull();
      assertThat(cache2.get(k)).isNull();
      final CountDownLatch latch = new CountDownLatch(1);

      Thread t = getTestThreadFactory("Worker").newThread(new Runnable() {
         @Override
         public void run() {
            log.info("Concurrent " + (useTx ? "tx" : "non-tx") + " write started "
                  + (sameNode ? "on same node..." : "on a different node..."));
            EmbeddedTransactionManager mgr = null;
            try {
               if (useTx) {
                  mgr = (EmbeddedTransactionManager) TestingUtil.getTransactionManager(sameNode ? cache1 : cache2);
                  mgr.begin();
               }
               if (sameNode) {
                  cache1.put(k, "JBC");
               } else {
                  cache2.put(k, "JBC");
               }
               if (useTx) {
                  if (!mgr.getTransaction().runPrepare()) { //couldn't prepare
                     latch.countDown();
                     mgr.rollback();
                  }
               }
            } catch (Exception e) {
               Throwable cause = e;
               while (cause instanceof RemoteException) {
                  cause = cause.getCause();
               }
               assertThat(cause).isInstanceOf(TimeoutException.class);
               if (useTx) {
                  try {
                     mgr.rollback();
                  } catch (Exception ignore) {
                  }
               }
               latch.countDown();
            }
         }
      });

      String name = "Infinispan";
      TransactionManager mgr = TestingUtil.getTransactionManager(cache1);
      mgr.begin();

      log.trace("Here is where the fun starts...Here is where the fun starts...");

      // lock node and start other thread whose write should now block
      cache1.getAdvancedCache().lock(k);
      t.start();

      // wait till the put in thread t times out
      assertThat(latch.await(10, TimeUnit.SECONDS)).as("Concurrent put didn't time out!").isTrue();

      cache1.put(k, name);
      mgr.commit();

      assertNotLocked("testcache", k);
      t.join();


      cache2.remove(k);
      assertThat(cache1).isEmpty();
      assertThat(cache2).isEmpty();
      cache1.clear();
      cache2.clear();
   }

   private void locksReleasedWithoutExplicitUnlockHelper(boolean lockPriorToPut, boolean useCommit)
         throws Exception {
      log.debugf("lockPriorToPut=%s, useCommit=%s", lockPriorToPut, useCommit);

      Cache cache1 = cache(0, "testcache");
      Cache cache2 = cache(1, "testcache");

      assertClusterSize("Should only be 2  caches in the cluster!!!", 2);

      assertThat(cache1.get(k)).isNull();
      assertThat(cache2.get(k)).isNull();

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
         assertThat(cache1.get(k)).isEqualTo(name);
         assertThat(cache2.get(k)).as("Should have replicated").isEqualTo(name);
      } else {
         assertThat(cache1.get(k)).isNull();
         assertThat(cache2.get(k)).as("Should not have replicated").isNull();
      }

      cache2.remove(k);
      assertThat(cache1).isEmpty();
      assertThat(cache2).isEmpty();
      cache1.clear();
      cache2.clear();
   }

}
