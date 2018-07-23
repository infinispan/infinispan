package org.infinispan.lock.singlelock.replicated.pessimistic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.lock.singlelock.replicated.optimistic.InitiatorCrashOptimisticReplTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "unstable", testName = "lock.singlelock.replicated.pessimistic.InitiatorCrashPessimisticReplTest", description = "See ISPN-2161 -- original group: functional")
@CleanupAfterMethod
public class InitiatorCrashPessimisticReplTest extends InitiatorCrashOptimisticReplTest {

   public InitiatorCrashPessimisticReplTest() {
      super(CacheMode.REPL_SYNC, LockingMode.PESSIMISTIC, false);
   }

   public void testInitiatorNodeCrashesBeforeCommit() throws Exception {
      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      txControlInterceptor.prepareProgress.countDown();
      advancedCache(1).getAsyncInterceptorChain().addInterceptor(txControlInterceptor, 1);

      MagicKey key = new MagicKey("k", cache(0));
      Future<Void> future = beginAndCommitTx(key, 1);
      txControlInterceptor.preparedReceived.await();

      assertLocked(cache(0), key);
      assertEventuallyNotLocked(cache(1), key);
      assertEventuallyNotLocked(cache(2), key);

      checkTxCount(0, 0, 1);
      checkTxCount(1, 1, 0);
      checkTxCount(2, 0, 1);

      killMember(1);

      assertNotLocked(key);
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      future.get(30, TimeUnit.SECONDS);
   }

   public void testInitiatorCrashesBeforeReleasingLock() throws Exception {
      final CountDownLatch releaseLocksLatch = new CountDownLatch(1);

      skipTxCompletion(advancedCache(1), releaseLocksLatch);

      MagicKey key = new MagicKey("k", cache(0));
      Future<Void> future = beginAndCommitTx(key, 1);
      releaseLocksLatch.await();

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 1);

      assertLocked(cache(0), key);
      assertEventuallyNotLocked(cache(1), key);
      assertEventuallyNotLocked(cache(2), key);

      killMember(1);

      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      assertNotLocked(key);
      assert cache(0).get(key).equals("v");
      assert cache(1).get(key).equals("v");
      future.get(30, TimeUnit.SECONDS);
   }

   public void testInitiatorNodeCrashesBeforePrepare() throws Exception {
      MagicKey key = new MagicKey("a", cache(0));
      cache(0).put(key, "b");
      assert cache(0).get(key).equals("b");
      assert cache(1).get(key).equals("b");
      assert cache(2).get(key).equals("b");

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      advancedCache(1).getAsyncInterceptorChain().addInterceptor(txControlInterceptor, 1);

      //prepare is sent, but is not precessed on other nodes because of the txControlInterceptor.preparedReceived
      Future<Void> future = beginAndPrepareTx("k", 1);

      eventually(() -> checkTxCount(0, 0, 1) &&  checkTxCount(1, 1, 0) && checkTxCount(2, 0, 1));

      killMember(1);

      assert caches().size() == 2;
      txControlInterceptor.prepareProgress.countDown();

      assertNotLocked("k");
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      future.get(30, TimeUnit.SECONDS);
   }
}
