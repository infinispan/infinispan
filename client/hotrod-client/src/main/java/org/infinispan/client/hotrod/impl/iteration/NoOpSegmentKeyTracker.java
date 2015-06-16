package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class NoOpSegmentKeyTracker implements KeyTracker {

   @Override
   public boolean track(byte[] key) {
      return true;
   }

   @Override
   public void segmentsFinished(byte[] finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return null;
   }

}
