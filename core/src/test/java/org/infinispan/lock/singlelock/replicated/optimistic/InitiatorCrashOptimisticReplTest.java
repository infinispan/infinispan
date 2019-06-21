package org.infinispan.lock.singlelock.replicated.optimistic;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "unstable", testName = "lock.singlelock.replicated.optimistic.InitiatorCrashOptimisticReplTest", description = "See ISPN-2161 -- original group: functional")
@CleanupAfterMethod
public class InitiatorCrashOptimisticReplTest extends AbstractCrashTest {

   public InitiatorCrashOptimisticReplTest() {
      super(CacheMode.REPL_SYNC, LockingMode.OPTIMISTIC, false);
   }

   public InitiatorCrashOptimisticReplTest(CacheMode mode, LockingMode locking, boolean useSync) {
      super(mode, locking, useSync);
   }

   public void testInitiatorNodeCrashesBeforeCommit() throws Exception {

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      txControlInterceptor.prepareProgress.countDown();
      extractInterceptorChain(advancedCache(1)).addInterceptor(txControlInterceptor, 1);

      Future<Void> future = beginAndCommitTx("k", 1);
      txControlInterceptor.commitReceived.await();

      assertLocked(cache(0), "k");
      assertEventuallyNotLocked(cache(1), "k");
      assertEventuallyNotLocked(cache(2), "k");

      checkTxCount(0, 0, 1);
      checkTxCount(1, 1, 0);
      checkTxCount(2, 0, 1);

      killMember(1);

      assertNotLocked("k");
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      future.get(30, TimeUnit.SECONDS);
   }

   public void testInitiatorCrashesBeforeReleasingLock() throws Exception {
      final CountDownLatch releaseLocksLatch = new CountDownLatch(1);

      skipTxCompletion(advancedCache(1), releaseLocksLatch);

      Future<Void> future = beginAndCommitTx("k", 1);
      releaseLocksLatch.await();

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 1);

      assertLocked(cache(0), "k");
      assertEventuallyNotLocked(cache(1), "k");
      assertEventuallyNotLocked(cache(2), "k");

      killMember(1);

      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      assertNotLocked("k");
      assert cache(0).get("k").equals("v");
      assert cache(1).get("k").equals("v");
      future.get(30, TimeUnit.SECONDS);
   }

   public void testInitiatorNodeCrashesBeforePrepare() throws Exception {

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      extractInterceptorChain(advancedCache(1)).addInterceptor(txControlInterceptor, 1);

      //prepare is sent, but is not precessed on other nodes because of the txControlInterceptor.preparedReceived
      Future<Void> future = beginAndPrepareTx("k", 1);

      txControlInterceptor.preparedReceived.await();
      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 1, 0);
      assert checkTxCount(2, 0, 1);

      killMember(1);

      assert caches().size() == 2;
      txControlInterceptor.prepareProgress.countDown();

      assertNotLocked("k");
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      future.get(30, TimeUnit.SECONDS);
   }
}
