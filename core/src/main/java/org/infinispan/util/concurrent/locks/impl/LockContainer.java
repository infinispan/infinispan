package org.infinispan.util.concurrent.locks.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.util.concurrent.locks.DeadlockChecker;
import org.infinispan.util.concurrent.locks.ExtendedLockPromise;

/**
 * A container for locks
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 4.0
 */
public interface LockContainer {

   /**
    * @param key the key to lock.
    * @return the lock for a specific object to be acquired. If the lock does not exists, it is created.
    */
   ExtendedLockPromise acquire(Object key, Object lockOwner, long time, TimeUnit timeUnit);

   /**
    * @param key the key to lock.
    * @return the lock for a specific object. If the lock does not exists, it return {@code null}.
    */
   InfinispanLock getLock(Object key);

   void release(Object key, Object lockOwner);

   /**
    * @return number of locks held
    */
   int getNumLocksHeld();

   /**
    * @param key the key to test.
    * @return {@code true} if the key is locked, {@code false} otherwise.
    */
   boolean isLocked(Object key);

   /**
    * @return the size of the shared lock pool
    */
   int size();

   /**
    * It forces a deadlock checks in all existing locks.
    */
   void deadlockCheck(DeadlockChecker deadlockChecker);
}
