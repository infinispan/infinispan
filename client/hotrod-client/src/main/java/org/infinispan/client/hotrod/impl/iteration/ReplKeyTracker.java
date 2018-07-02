package org.infinispan.client.hotrod.impl.iteration;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * Tracks all keys seen during iteration. Depends on ISPN-5451 to be done more efficiently, by discarding segments as
 * soon as they are completed iterating.
 *
 * @author gustavonalle
 * @since 8.0
 */
class ReplKeyTracker implements KeyTracker {

   private Set<WrappedByteArray> keys = new HashSet<>();

   @Override
   public boolean track(byte[] key, short status, ClassWhiteList whitelist) {
      return keys.add(new WrappedByteArray(key));
   }

   @Override
   public void segmentsFinished(byte[] finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return null;
   }
}
