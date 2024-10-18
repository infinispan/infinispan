package org.infinispan.util.concurrent.locks.deadlock;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;

/**
 * Listener for view changes.
 * <p>
 * This listener triggers the deadlock detection algorithm after a network partition heals. This procedure is necessary
 * to detect deadlock cycles spanning partitions when nodes can communicate again. Therefore, this listener initializes
 * the deadlock algorithm by probing from the local transactions pending remote locks.
 * </p>
 *
 * <p>
 * Deadlock cycles are identified by message exchange during operation in the cluster without network partitions. Cycles
 * within a cycle partition are identified even during degraded operation. However, it is impossible to detect cycles during
 * partitions when the cycle is not self-contained since the nodes can't communicate.
 * </p>
 *
 * @author José Bolina
 */
@Listener(sync = false)
final class DeadlockDetectionViewListener {

   private final DistributedDeadlockDetection deadlockDetection;

   DeadlockDetectionViewListener(DistributedDeadlockDetection deadlockDetection) {
      this.deadlockDetection = deadlockDetection;
   }

   @Merged
   @SuppressWarnings("unused")
   public CompletionStage<Void> handleViewChange(MergeEvent event) {
      if (!event.isMergeView()) return CompletableFutures.completedNull();
      return deadlockDetection.probeAllLocalTransactions();
   }
}
