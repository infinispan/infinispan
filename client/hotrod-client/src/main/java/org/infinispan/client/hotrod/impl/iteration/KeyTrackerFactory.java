package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;

/**
 * @author gustavonalle
 * @since 8.0
 */
public final class KeyTrackerFactory {

   private KeyTrackerFactory() {
   }

   public static KeyTracker create(ConsistentHash hash, int topologyId) {
      if (topologyId == -1) return new NoOpSegmentKeyTracker();
      if (hash == null) return new ReplKeyTracker();
      return new SegmentKeyTracker((SegmentConsistentHash) hash);
   }

}
