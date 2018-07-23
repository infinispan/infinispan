package org.infinispan.lock.singlelock.optimistic;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractInitiatorCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.optimistic.InitiatorCrashOptimisticTest")
@CleanupAfterMethod
public class InitiatorCrashOptimisticTest extends AbstractInitiatorCrashTest {

   public InitiatorCrashOptimisticTest() {
      super(CacheMode.DIST_SYNC, LockingMode.OPTIMISTIC, false);
   }

   public void testInitiatorNodeCrashesBeforePrepare() throws Exception {

      TxControlInterceptor txControlInterceptor = new TxControlInterceptor();
      advancedCache(1).getAsyncInterceptorChain().addInterceptor(txControlInterceptor, 1);
      Object k = getKeyForCache(2);

      //prepare is sent, but is not precessed on other nodes because of the txControlInterceptor.preparedReceived
      Future<Void> future = beginAndPrepareTx(k, 1);

      txControlInterceptor.preparedReceived.await();
      assert checkTxCount(0, 0, 1);
      assert checkTxCount(1, 1, 0);
      assert checkTxCount(2, 0, 1);

      killMember(1);

      assert caches().size() == 2;
      txControlInterceptor.prepareProgress.countDown();

      assertNotLocked(k);
      eventually(() -> checkTxCount(0, 0, 0) && checkTxCount(1, 0, 0));
      future.get(30, TimeUnit.SECONDS);
   }
}
