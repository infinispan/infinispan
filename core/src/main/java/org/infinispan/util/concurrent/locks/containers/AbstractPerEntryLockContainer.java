package org.infinispan.util.concurrent.locks.containers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * An abstract lock container that creates and maintains a new lock per entry
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class AbstractPerEntryLockContainer implements LockContainer {

   protected final ConcurrentMap<Object, Lock> locks;

   protected AbstractPerEntryLockContainer(int concurrencyLevel) {
      locks = new ConcurrentHashMap<Object, Lock>(16, .75f, concurrencyLevel);
   }

   protected abstract Lock newLock();

   public final Lock getLock(Object key) {
      // this is an optimisation.  It is not foolproof as we may still be creating new locks unnecessarily (thrown away
      // when we do a putIfAbsent) but it minimises the chances somewhat, for the cost of an extra CHM get.
      Lock lock = locks.get(key);
      if (lock == null) lock = newLock();
      Lock existingLock = locks.putIfAbsent(key, lock);
      if (existingLock != null) lock = existingLock;
      return lock;
   }

   public int getNumLocksHeld() {
      return locks.size();
   }

   public int size() {
      return locks.size();
   }

   public Lock acquireLock(Object key, long timeout, TimeUnit unit) throws InterruptedException {
      while (true) {
         Lock lock = getLock(key);
         if (lock.tryLock(timeout, unit)) {
            // lock acquired.  Now check if it is the *correct* lock!
            Lock existingLock = locks.putIfAbsent(key, lock);
            if (existingLock != null && existingLock != lock) {
               // we have the wrong lock!  Unlock and retry.
               lock.unlock();
            } else {
               // we got the right lock.
               return lock;
            }
         } else {
            // we couldn't acquire the lock within the timeout period
            return null;
         }
      }
   }

   public void releaseLock(Object key) {
      Lock l = locks.remove(key);
      if (l != null) l.unlock();
   }
}
