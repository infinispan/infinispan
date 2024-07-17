package org.infinispan.util.concurrent.locks.deadlock;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.DeadlockDetection;

/**
 * An empty instance of a deadlock detection algorithm.
 *
 * <p>
 * This instance is utilized when the deadlock detection is disabled.
 * </p>
 *
 * @author José Bolina
 */
public final class DisabledDeadlockDetection implements DeadlockDetection {
   private static final DeadlockDetection INSTANCE = new DisabledDeadlockDetection();

   private DisabledDeadlockDetection() { }

   public static DeadlockDetection getInstance() {
      return INSTANCE;
   }

   @Override
   public void initializeDeadlockDetection(Object initiator, Object holder) { }

   @Override
   public CompletionStage<Void> verifyDeadlockCycle(Object initiator, Object holder, Collection<?> keys) {
      return CompletableFutures.completedNull();
   }
}
