package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.containers.LockContainer;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Helper class to assert lock status in MVCC
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
public class LockAssert {
   public static void assertLocked(Object key, LockManager lockManager, InvocationContextContainer icc) {
      assertTrue("" + key + " not locked!", lockManager.isLocked(key));
      InvocationContext context = icc.getInvocationContext(true);
      if (context != null) {
         assertTrue("" + key + " lock not recorded!", context.hasLockedKey(key));
      }
   }

   public static void assertNotLocked(Object key, InvocationContextContainer icc) {
      // can't rely on the negative test since other entries may share the same lock with lock striping.
      InvocationContext context = icc.getInvocationContext(true);
      if (context != null) {
         assertFalse("" + key + " lock recorded!", context.hasLockedKey(key));
      }
   }

   public static void assertNoLocks(LockManager lockManager) {
      LockContainer lc = (LockContainer) TestingUtil.extractField(lockManager, "lockContainer");
      assertEquals("Stale locks exist! NumLocksHeld is " + lc.getNumLocksHeld() + " and lock info is " + lockManager.printLockInfo(),
            0, lc.getNumLocksHeld());
   }

   public static void assertNoLocks(Cache cache) {
      LockManager lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      assertNoLocks(lockManager);
   }
}
