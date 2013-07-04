package org.infinispan.lock.singlelock.optimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractInitiatorCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
      advancedCache(1).addInterceptor(txControlInterceptor, 1);
      Object k = getKeyForCache(2);

      //prepare is sent, but is not precessed on other nodes because of the txControlInterceptor.preparedReceived
      beginAndPrepareTx(k, 1);

      txControlInterceptor.preparedReceived.await();
      assertTrue("Wrong tx count for " + cache(0), checkTxCount(0, 0, 1));
      assertTrue("Wrong tx count for " + cache(1), checkTxCount(1, 1, 0));
      assertTrue("Wrong tx count for " + cache(2), checkTxCount(2, 0, 1));

      killMember(1);

      assertEquals("Wrong number of caches", 2, caches().size());
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
