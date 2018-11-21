package org.infinispan.lock.singlelock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractInitiatorCrashTest extends AbstractCrashTest {

   public AbstractInitiatorCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      super(cacheMode, lockingMode, useSynchronization);
   }

   public void testInitiatorCrashesBeforeReleasingLock() throws Exception {
      final CountDownLatch releaseLocksLatch = new CountDownLatch(1);

      skipTxCompletion(advancedCache(1), releaseLocksLatch);

      Object k = getKeyForCache(2);
      Future<Void> future = beginAndCommitTx(k, 1);
      releaseLocksLatch.await(30, TimeUnit.SECONDS);

      assert checkTxCount(0, 0, 1);
      eventuallyEquals(0, () -> getLocalTxCount(1));
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 1);

      assertEventuallyNotLocked(cache(0), k);
      assertEventuallyNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      killMember(1);

      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      assertNotLocked(k);
      future.get(30, TimeUnit.SECONDS);
   }

   public void testInitiatorNodeCrashesBeforeCommit() throws Exception {

      Object k = getKeyForCache(2);

      tm(1).begin();
      cache(1).put(k,"v");
      final EmbeddedTransaction transaction = (EmbeddedTransaction) tm(1).getTransaction();
      transaction.runPrepare();
      tm(1).suspend();

      assertEventuallyNotLocked(cache(0), k);
      assertEventuallyNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      checkTxCount(0, 0, 1);
      checkTxCount(1, 1, 0);
      checkTxCount(2, 0, 1);

      killMember(1);

      assertNotLocked(k);
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
   }
}
