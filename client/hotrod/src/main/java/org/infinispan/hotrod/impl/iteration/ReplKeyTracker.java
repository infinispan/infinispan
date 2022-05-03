package org.infinispan.hotrod.impl.iteration;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.IntSet;

/**
 * Tracks all keys seen during iteration. Depends on ISPN-5451 to be done more efficiently, by discarding segments as
 * soon as they are completed iterating.
 *
 * @since 14.0
 */
class ReplKeyTracker implements KeyTracker {

   private Set<WrappedByteArray> keys = new HashSet<>();

   @Override
   public boolean track(byte[] key, short status, ClassAllowList allowList) {
      return keys.add(new WrappedByteArray(key));
   }

   @Override
   public void segmentsFinished(IntSet finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return null;
   }
}
