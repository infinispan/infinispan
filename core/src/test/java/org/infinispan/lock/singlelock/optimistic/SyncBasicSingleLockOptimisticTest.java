package org.infinispan.lock.singlelock.optimistic;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.optimistic.SyncBasicSingleLockOptimisticTest")
public class SyncBasicSingleLockOptimisticTest extends BasicSingleLockOptimisticTest {

   public SyncBasicSingleLockOptimisticTest() {
      useSynchronization = true;
   }

   @Override
   public void testSecondTxCannotPrepare() throws Exception {
   }
}
