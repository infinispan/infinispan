package org.infinispan.util.concurrent.locks;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.util.concurrent.locks.deadlock.DisabledDeadlockDetection;

/**
 * Interface for deadlock detection algorithms.
 *
 * <p>
 * Provides the method to start the algorithm and handle the objects to identify cycles.
 * </p>
 */
public interface DeadlockDetection {

   /**
    * Asynchronously start the deadlock detection algorithm.
    *
    * @param initiator: The object waiting to acquire the lock.
    * @param holder: The current resource owner.
    */
   void initializeDeadlockDetection(Object initiator, Object holder);

   /**
    * Verifies for deadlock cycles.
    * <p>
    * The method receive the object waiting for the resource, the current owner, and the list of keys the current owner
    * is working with.
    * </p>
    *
    * @param initiator: The object waiting to acquire the lock.
    * @param holder: The current resource owner.
    * @param keys: The keys the holder is working.
    * @return A completable future to finish after handling the operation.
    */
   CompletionStage<Void> verifyDeadlockCycle(Object initiator, Object holder, Collection<?> keys);

   /**
    * Whether the deadlock detection is enabled.
    *
    * @return <code>true</code> if enabled, <code>false</code>, otherwise.
    */
   default boolean isEnabled() {
      return false;
   }

   /**
    * Creates an instance of the disabled deadlock detection.
    *
    * @return A deadlock detection which performs no operations.
    */
   static DeadlockDetection disabled() {
      return DisabledDeadlockDetection.getInstance();
   }
}
