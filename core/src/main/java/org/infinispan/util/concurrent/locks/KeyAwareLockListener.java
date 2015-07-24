package org.infinispan.util.concurrent.locks;

/**
 * A listener for {@link KeyAwareLockPromise}.
 * <p>
 * This event contains the key that the lock owner is try to acquire.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface KeyAwareLockListener {

   /**
    * Invoked when the lock is available.
    *
    * @param key   the key associated to this lock.
    * @param state the lock state. Possible values are {@link LockState#ACQUIRED}, {@link LockState#TIMED_OUT} or
    *              {@link LockState#DEADLOCKED}.
    */
   void onEvent(Object key, LockState state);
}
