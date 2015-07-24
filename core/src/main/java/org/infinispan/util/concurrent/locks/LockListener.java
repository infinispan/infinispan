package org.infinispan.util.concurrent.locks;

/**
 * The listener for {@link LockPromise}.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface LockListener {

   /**
    * Invoked when the lock is available.
    *
    * @param state the lock state. Possible values are {@link LockState#ACQUIRED}, {@link LockState#TIMED_OUT} or
    *              {@link LockState#DEADLOCKED}.
    */
   void onEvent(LockState state);
}
