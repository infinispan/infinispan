package org.infinispan.util.concurrent.locks;

/**
 * The {@link org.infinispan.util.concurrent.locks.impl.InfinispanLock} possible states.
 * <p>
 * Used in listener to notify when the state changes.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public enum LockState {
   /**
    * The lock owner is in the queue waiting for the lock to be available.
    */
   WAITING,
   /**
    * The lock owner left the queue and it is available to acquire the lock.
    */
   ACQUIRED,
   /**
    * The time out occurred while the lock owner waits on the queue.
    */
   TIMED_OUT,
   /**
    * The deadlock occurred with another possible lock owner and it should abort.
    */
   DEADLOCKED,
   /**
    * The lock owner released the lock.
    */
   RELEASED
}
