package org.infinispan.api.mvcc;

import org.infinispan.Cache;
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.lock.LockManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.containers.LockContainer;

/**
 * Helper class to assert lock status in MVCC
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 */
public class LockAssert {
   public static void assertLocked(Object key, LockManager lockManager, InvocationContextContainer icc) {
      assert lockManager.isLocked(key) : key + " not locked!";
//      assert icc.get().getKeysLocked().contains(key) : "Lock not recorded for " + key;
   }

   public static void assertNotLocked(Object key, InvocationContextContainer icc) {
      // can't rely on the negative test since other entries may share the same lock with lock striping.
      assert !icc.get().hasLockedKey(key) : key + " lock recorded!";
   }

   public static void assertNoLocks(LockManager lockManager, InvocationContextContainer icc) {
      LockContainer lc = (LockContainer) TestingUtil.extractField(lockManager, "lockContainer");
      assert lc.getNumLocksHeld() == 0 : "Stale locks exist! NumLocksHeld is " + lc.getNumLocksHeld() + " and lock info is " + lockManager.printLockInfo();
      assert !icc.get().isContainsModifications() : "Stale (?) locks recorded!";
   }

   public static void assertNoLocks(Cache cache) {
      LockManager lockManager = TestingUtil.extractComponentRegistry(cache).getComponent(LockManager.class);
      InvocationContextContainer icc = TestingUtil.extractComponentRegistry(cache).getComponent(InvocationContextContainer.class);

      assertNoLocks(lockManager, icc);
   }
}
