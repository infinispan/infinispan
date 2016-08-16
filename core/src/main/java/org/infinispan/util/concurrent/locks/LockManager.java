package org.infinispan.util.concurrent.locks;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.context.InvocationContext;
import org.infinispan.util.concurrent.locks.impl.InfinispanLock;

/**
 * An interface to deal with all aspects of acquiring and releasing locks for cache entries.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 4.0
 */
public interface LockManager {

   /**
    * Attempts to lock the {@code key} if the lock isn't already held by the {@code lockOwner}.
    * <p>
    * This method is non-blocking and return immediately a {@link LockPromise}. The {@link LockPromise} can (and should)
    * be used by the invoker to check when the lock is really acquired by invoking {@link LockPromise#lock()}.
    *
    * @param key       key to lock.
    * @param lockOwner the owner of the lock.
    * @param time      the maximum time to wait for the lock
    * @param unit      the time unit of the {@code time} argument
    * @return the {@link KeyAwareLockPromise} associated to this keys.
    */
   KeyAwareLockPromise lock(Object key, Object lockOwner, long time, TimeUnit unit);

   /**
    * Same as {@link #lock(Object, Object, long, TimeUnit)} but for multiple keys.
    * <p>
    * It ensures no deadlocks if the method is invoked by different lock owners for the same set (or subset) of keys.
    *
    * @param keys      keys to lock.
    * @param lockOwner the owner of the lock.
    * @param time      the maximum time to wait for the lock
    * @param unit      the time unit of the {@code time} argument
    * @return the {@link KeyAwareLockPromise} associated to this keys.
    */
   KeyAwareLockPromise lockAll(Collection<?> keys, Object lockOwner, long time, TimeUnit unit);

   /**
    * Releases the lock for the {@code key} if the {@code lockOwner} is the lock owner.
    *
    * @param key       key to unlock.
    * @param lockOwner the owner of the lock.
    */
   void unlock(Object key, Object lockOwner);

   /**
    * Same as {@link #unlock(Object, Object)} but for multiple keys.
    *
    * @param keys      keys to unlock.
    * @param lockOwner the owner of the lock.
    */
   void unlockAll(Collection<?> keys, Object lockOwner);

   /**
    * Same as {@code unlockAll(context.getLockedKeys(), context.getKeyLockOwner();}.
    *
    * @param context the context with the locked keys and the lock owner.
    */
   void unlockAll(InvocationContext context);

   /**
    * Tests if the {@code lockOwner} owns a lock on the {@code key}.
    *
    * @param key       key to test.
    * @param lockOwner the owner of the lock.
    * @return {@code true} if the owner does own the lock on the key, {@code false} otherwise.
    */
   boolean ownsLock(Object key, Object lockOwner);

   /**
    * Tests if the {@code key} is locked.
    *
    * @param key key to test.
    * @return {@code true} if the key is locked, {@code false} otherwise.
    */
   boolean isLocked(Object key);

   /**
    * Retrieves the owner of the lock for the {@code key}.
    *
    * @return the owner of the lock, or {@code null} if not locked.
    */
   Object getOwner(Object key);

   /**
    * Prints lock information for all locks.
    *
    * @return the lock information
    */
   String printLockInfo();

   /**
    * @return the number of locks held.
    */
   int getNumberOfLocksHeld();

   InfinispanLock getLock(Object key);
}
