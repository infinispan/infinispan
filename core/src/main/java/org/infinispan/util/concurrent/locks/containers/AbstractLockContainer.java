package org.infinispan.util.concurrent.locks.containers;

import org.infinispan.util.logging.Log;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractLockContainer<L extends Lock> implements LockContainer<L> {

   /**
    * Releases a lock and swallows any IllegalMonitorStateExceptions - so it is safe to call this method even if the
    * lock is not locked, or not locked by the current thread.
    *
    * @param toRelease lock to release
    */
   protected void safeRelease(L toRelease, Object lockOwner) {
      if (toRelease != null) {
         try {
            unlock(toRelease, lockOwner);
         } catch (IllegalMonitorStateException imse) {
            // Perhaps the caller hadn't acquired the lock after all.
         }
      }
   }

   protected abstract void unlock(L toRelease, Object ctx);

   protected abstract boolean tryLock(L lock, long timeout, TimeUnit unit, Object lockOwner) throws InterruptedException;

   protected abstract void lock(L lock, Object lockOwner);

   protected abstract Log getLog();
}
