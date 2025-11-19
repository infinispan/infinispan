package org.infinispan.query.backend;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.query.core.impl.Log;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * SegmentListener will detect segments that were lost for this node due to topology changes so they can be removed
 * from the local indexes.
 *
 * @since 11.0
 */
@Listener(observation = Listener.Observation.POST)
@SuppressWarnings("unused")
final class SegmentListener {

   private static final Log log = Log.getLog(SegmentListener.class);

   private final Consumer<IntSet> segmentDeleted;
   private final Address address;
   private final BlockingManager blockingManager;

   SegmentListener(Address address, Consumer<IntSet> segmentsDeleted, BlockingManager blockingManager) {
      segmentDeleted = segmentsDeleted;
      this.address = address;
      this.blockingManager = blockingManager;
   }

   @TopologyChanged
   public CompletionStage<Void> topologyChanged(TopologyChangedEvent<?, ?> event) {
      if (event.isPre()) {
         return CompletableFutures.completedNull();
      }
      var newWriteCh = event.getWriteConsistentHashAtEnd();

      if (!newWriteCh.getMembers().contains(address)) {
         return CompletableFutures.completedNull();
      }

      var removedSegments = IntSets.mutableCopyFrom(event.getWriteConsistentHashAtStart().getSegmentsForOwner(address));
      removedSegments.removeAll(IntSets.from(newWriteCh.getSegmentsForOwner(address)));

      if (removedSegments.isEmpty()) {
         return CompletableFutures.completedNull();
      }

      return blockingManager.runBlocking(() -> segmentDeleted.accept(removedSegments), this)
            .exceptionally(throwable -> {
               // catch exception to allow the cache topology to be installed
               log.failedToPurgeIndexForSegments(throwable, removedSegments);
               return null;
            });
   }
}
