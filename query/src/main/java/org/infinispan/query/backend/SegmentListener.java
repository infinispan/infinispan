package org.infinispan.query.backend;

import java.util.function.Consumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * SegmentListener will detect segments that were lost for this node due to topology changes so they can be removed
 * from the local indexes.
 *
 * @since 11.0
 */
@Listener(observation = Listener.Observation.POST)
@SuppressWarnings("unused")
final class SegmentListener {

   private final AdvancedCache<?, ?> cache;
   private final Consumer<IntSet> segmentDeleted;
   private final Address address;

   SegmentListener(AdvancedCache<?, ?> cache, Consumer<IntSet> segmentsDeleted) {
      this.cache = cache;
      this.segmentDeleted = segmentsDeleted;
      this.address = cache.getRpcManager().getAddress();
   }

   @TopologyChanged
   public void topologyChanged(TopologyChangedEvent<?, ?> event) {
      if (event.isPre()) return;
      ConsistentHash newWriteCh = event.getWriteConsistentHashAtEnd();
      LocalizedCacheTopology cacheTopology = cache.getDistributionManager().getCacheTopology();

      boolean isMember = cacheTopology.getMembers().contains(address);

      if (!isMember) return;

      int numSegments = newWriteCh.getNumSegments();

      IntSet removedSegments = IntSets.mutableEmptySet(numSegments);
      IntSet newSegments = IntSets.from(newWriteCh.getSegmentsForOwner(address));

      for (int i = 0; i < numSegments; ++i) {
         if (!newSegments.contains(i)) {
            removedSegments.add(i);
         }
      }

      segmentDeleted.accept(removedSegments);
   }
}
