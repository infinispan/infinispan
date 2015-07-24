package org.infinispan.util.concurrent.locks;

/**
 * An interface to implement the deadlock algorithm.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface DeadlockChecker {

   /**
    * It checks for deadlock.
    * <p>
    * It accepts two arguments: the {@code pendingOwner} is a lock owner that tries to acquire the lock and the {@code
    * currentOwner} is the current lock owner. If a deadlock is detected and the {@code pendingOwner} must rollback, it
    * must return {@code true}. If no deadlock is found or the {@code currentOwner} must rollback, it must return {@code
    * false}.
    * <p>
    * This method may be invoked multiples times and in multiple threads. Thread safe is advised.
    *
    * @param pendingOwner a lock owner that tries to acquire the lock.
    * @param currentOwner the current lock owner.
    * @return {@code true} if a deadlock is detected and the {@code pendingOwner} must rollback.
    */
   boolean deadlockDetected(Object pendingOwner, Object currentOwner);

}
