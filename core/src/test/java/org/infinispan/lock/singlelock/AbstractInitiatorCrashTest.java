package org.infinispan.lock.singlelock;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

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

      prepareCache(releaseLocksLatch);

      Object k = getKeyForCache(2);
      beginAndCommitTx(k, 1);
      releaseLocksLatch.await();

      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 0, 0);
      assert checkTxCount(2, 0, 1);

      assertNotLocked(cache(0), k);
      assertNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      killMember(1);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
      assertNotLocked(k);
   }

   public void testInitiatorNodeCrashesBeforeCommit() throws Exception {

      Object k = getKeyForCache(2);
      
      tm(1).begin();
      cache(1).put(k,"v");
      final DummyTransaction transaction = (DummyTransaction) tm(1).getTransaction();
      transaction.runPrepare();
      tm(1).suspend();

      assertNotLocked(cache(0), k);
      assertNotLocked(cache(1), k);
      assertLocked(cache(2), k);

      checkTxCount(0, 0, 1);
      checkTxCount(1, 1, 0);
      checkTxCount(2, 0, 1);

      killMember(1);

      assertNotLocked(k);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }
}
