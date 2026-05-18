package org.infinispan.util.concurrent.locks;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
    * <p>
    * Note this method <b>will</b> unlock a lock where the key is the lockOwner
    * </p>
    *
    * @param key       key to unlock.
    * @param lockOwner the owner of the lock.
    * @return any post-unlock listener suppliers that were registered for the lock owner.
    */
   Collection<Supplier<CompletionStage<Void>>> unlock(Object key, Object lockOwner);

   /**
    * Same as {@link #unlock(Object, Object)} but for multiple keys.
    * <p>
    * Note this method will <b>not</b> unlock a lock where the key is the lockOwner
    * </p>
    * @param keys      keys to unlock.
    * @param lockOwner the owner of the lock.
    * @return any post-unlock listener suppliers that were registered for the unlocked keys.
    */
   Collection<Supplier<CompletionStage<Void>>> unlockAll(Collection<?> keys, Object lockOwner);

   /**
    * Same as {@code unlockAll(context.getLockedKeys(), context.getKeyLockOwner();}.
    * <p>
    * Note this method will <b>not</b> unlock a lock where the key is the lockOwner
    * </p>
    * @param context the context with the locked keys and the lock owner.
    * @return any post-unlock listener suppliers that were registered for the unlocked keys.
    */
   Collection<Supplier<CompletionStage<Void>>> unlockAll(InvocationContext context);

   /**
    * Registers a listener that will be returned when the current lock owner releases its locks via
    * {@link #unlock(Object, Object)}, {@link #unlockAll(Collection, Object)}, or {@link #unlockAll(InvocationContext)}.
    * <p>
    * The key must currently be locked. If it is not, an {@link IllegalStateException} is thrown.
    * The listener is associated with the current lock owner and will only be returned when that
    * owner's locks are released.
    * </p>
    * <p>
    * Note: registered listeners are not invoked when the operation that acquired the lock completes
    * with an exception. In that case, the listeners are still drained from the map but are discarded
    * by the interceptor chain.
    * </p>
    *
    * @param key      the key that must be currently locked.
    * @param listener a supplier that produces a {@link CompletionStage} to be awaited after unlock.
    * @throws IllegalStateException if the key is not currently locked.
    */
   void addPostUnlockListener(Object key, Supplier<CompletionStage<Void>> listener);

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
