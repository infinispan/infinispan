package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;

/**
 * @author gustavonalle
 * @since 8.0
 */
final class KeyTrackerFactory {

   private KeyTrackerFactory() {
   }

   public static KeyTracker create(DataFormat dataFormat, ConsistentHash hash, int topologyId, Set<Integer> segments) {
      if (topologyId == -1) return new NoOpSegmentKeyTracker();
      if (hash == null) return new ReplKeyTracker();
      return new SegmentKeyTracker(dataFormat, (SegmentConsistentHash) hash, segments);
   }

}
