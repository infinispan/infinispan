package org.infinispan.lock.singlelock.optimistic;

import org.infinispan.config.Configuration;
import org.infinispan.lock.singlelock.AbstractInitiatorCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.InitiatorCrasherBeforePrepareOptimisticTest")
@CleanupAfterMethod
public class InitiatorCrashOptimisticTest extends AbstractInitiatorCrashTest {
   public InitiatorCrashOptimisticTest() {
      super(Configuration.CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC, false);
   }


   public void testInitiatorNodeCrashesBeforePrepare() throws Exception {

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      advancedCache(1).addInterceptor(txControlInterceptor, 1);
      Object k = getKeyForCache(2);

      //prepare is sent, but is not precessed on other nodes because of the txControlInterceptor.preparedReceived
      beginAndPrepareTx(k, 1);

      txControlInterceptor.preparedReceived.await();
      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 1, 0);
      assert checkTxCount(2, 0, 1);

      killMember(1);
      cacheManagers.remove(1);

      assert caches().size() == 2;
      txControlInterceptor.prepareProgress.countDown();

      assertNotLocked(k);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0);
         }
      });
   }
}
