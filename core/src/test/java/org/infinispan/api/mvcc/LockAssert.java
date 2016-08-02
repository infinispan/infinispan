package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.impl.LockContainer;
import org.infinispan.util.concurrent.locks.LockManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Helper class to assert lock status in MVCC
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
public class LockAssert {
   public static void assertLocked(Object key, LockManager lockManager) {
      assertTrue("" + key + " not locked!", lockManager.isLocked(key));
   }

   public static void assertNotLocked(Object key, LockManager lockManager) {
      assertFalse("" + key + " not locked!", lockManager.isLocked(key));
   }

   public static void assertNoLocks(LockManager lockManager) {
      LockContainer lc = TestingUtil.extractField(lockManager, "lockContainer");
      assertEquals("Stale locks exist! NumLocksHeld is " + lc.getNumLocksHeld() + " and lock info is " + lockManager.printLockInfo(),
            0, lc.getNumLocksHeld());
   }

   public static void assertNoLocks(Cache cache) {
      LockManager lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      assertNoLocks(lockManager);
   }
}
