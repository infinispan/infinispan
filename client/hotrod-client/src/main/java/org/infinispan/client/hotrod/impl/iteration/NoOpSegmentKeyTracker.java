package org.infinispan.client.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.IntSet;

/**
 * @author gustavonalle
 * @since 8.0
 */
class NoOpSegmentKeyTracker implements KeyTracker {

   @Override
   public boolean track(byte[] key, short status, ClassAllowList allowList) {
      return true;
   }

   @Override
   public void segmentsFinished(IntSet finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return null;
   }

}
