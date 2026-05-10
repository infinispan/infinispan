package org.infinispan.api.mvcc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.impl.LockContainer;

/**
 * Helper class to assert lock status in MVCC
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
public class LockAssert {
   public static void assertLocked(Object key, LockManager lockManager) {
      assertTrue(lockManager.isLocked(key), key + " not locked!");
   }

   public static void assertNotLocked(Object key, LockManager lockManager) {
      assertFalse(lockManager.isLocked(key), key + " not locked!");
   }

   public static void assertNoLocks(LockManager lockManager) {
      LockContainer lc = TestingUtil.extractField(lockManager, "lockContainer");
      assertEquals(0, lc.getNumLocksHeld(), "Stale locks exist! NumLocksHeld is " + lc.getNumLocksHeld() + " and lock info is " + lockManager.printLockInfo());
   }

   public static void assertNoLocks(Cache cache) {
      LockManager lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      assertNoLocks(lockManager);
   }
}
