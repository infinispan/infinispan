package org.infinispan.lock.singlelock.pessimistic;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.pessimistic.SyncBasicSingleLockPessimisticTest")
public class SyncBasicSingleLockPessimisticTest extends BasicSingleLockPessimisticTest {
   public SyncBasicSingleLockPessimisticTest() {
      useSynchronization = true;
   }
}
