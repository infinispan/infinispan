package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.CollectionFactory;

import java.util.List;
import java.util.Set;

/**
 * Tracks all keys seen during iteration. Depends on ISPN-5451 to be done more efficiently, by discarding segments as
 * soon as they are completed iterating.
 *
 * @author gustavonalle
 * @since 8.0
 */
class ReplKeyTracker implements KeyTracker {

   private Set<byte[]> keys = CollectionFactory.makeSet(ByteArrayEquivalence.INSTANCE);

   @Override
   public boolean track(byte[] key, short status, List<String> whitelist) {
      return keys.add(key);
   }

   @Override
   public void segmentsFinished(byte[] finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return null;
   }
}
