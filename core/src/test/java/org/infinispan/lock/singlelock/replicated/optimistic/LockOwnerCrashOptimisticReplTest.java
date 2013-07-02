package org.infinispan.lock.singlelock.replicated.optimistic;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.lock.singlelock.AbstractLockOwnerCrashTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.singlelock.replicated.optimistic.LockOwnerCrashOptimisticReplTest")
@CleanupAfterMethod
public class LockOwnerCrashOptimisticReplTest extends AbstractLockOwnerCrashTest {

   public LockOwnerCrashOptimisticReplTest() {
      super(CacheMode.REPL_SYNC, LockingMode.OPTIMISTIC, false);
   }

   @Override
   protected Object getKeyForCache(int nodeIndex) {
      return "k" + nodeIndex;
   }
}
