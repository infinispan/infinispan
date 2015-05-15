package org.infinispan.client.hotrod.impl.iteration;


import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.nio.ByteBuffer.wrap;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.CollectionFactory;

/**
 * Tracks all keys seen during iteration. Depends on ISPN-5451 to be done more efficiently, by
 * discarding segments as soon as they are completed iterating.
 *
 * @author gustavonalle
 * @since 8.0
 */
public class ReplKeyTracker implements KeyTracker {

   private Set<byte[]> keys =  CollectionFactory.makeSet(ByteArrayEquivalence.INSTANCE);

   @Override
   public boolean track(byte[] key) {
      return keys.add(key);
   }

   @Override
   public void segmentsFinished(byte[] finishedSegments) {
   }

   @Override
   public Set<Integer> missedSegments() {
      return Collections.emptySet();
   }
}
