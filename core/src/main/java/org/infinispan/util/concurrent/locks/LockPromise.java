package org.infinispan.util.concurrent.locks;

import org.infinispan.interceptors.InvocationStage;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * A promise returned by {@link org.infinispan.util.concurrent.locks.impl.InfinispanLock}.
 * <p>
 * This promise does not means that the lock is acquired. The lock is acquired when the {@link #lock()}  method is
 * invoked. It contains the basic method to check it state (when it is available or not) and it allows adding listeners
 * to it.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface LockPromise {

   /**
    * It tests if the lock is available.
    * <p>
    * The lock is consider available when it is successfully acquired or the timeout is expired. In any case, when it
    * returns {@code true}, the {@link #lock()} will never block.
    *
    * @return {@code true} if the lock is available (or the timeout is expired), {@code false} otherwise.
    */
   boolean isAvailable();

   /**
    * It locks the key (or keys) associated to this promise.
    * <p>
    * This method will block until the lock is available or the timeout is expired.
    *
    * @throws InterruptedException if the current thread is interrupted while acquiring the lock
    * @throws TimeoutException     if we are unable to acquire the lock after a specified timeout.
    */
   void lock() throws InterruptedException, TimeoutException;

   /**
    * Adds a {@link LockListener} to be invoked when the lock is available.
    * <p>
    * The {@code acquired} parameter indicates that the lock is acquired (when it is {@code true}) or it timed out (when
    * it is {@code false}).
    *
    * @param listener the {@link LockListener} to invoke.
    */
   void addListener(LockListener listener);

   /**
    * @return an {@link InvocationStage} for this lock.
    */
   InvocationStage toInvocationStage();
}
