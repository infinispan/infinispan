package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.commons.marshall.Marshaller;

/**
 * @author gustavonalle
 * @since 8.0
 */
final class KeyTrackerFactory {

   private KeyTrackerFactory() {
   }

   public static KeyTracker create(Marshaller marshaller, ConsistentHash hash, int topologyId, Set<Integer> segments) {
      if (topologyId == -1) return new NoOpSegmentKeyTracker();
      if (hash == null) return new ReplKeyTracker();
      return new SegmentKeyTracker(marshaller, (SegmentConsistentHash) hash, segments);
   }

}
