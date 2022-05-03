package org.infinispan.hotrod.impl.iteration;

import java.util.Set;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.IntSet;

/**
 * @since 14.0
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
