package org.infinispan.client.hotrod.impl.iteration;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class SegmentKeyTracker implements KeyTracker {

   private static final Log log = LogFactory.getLog(SegmentKeyTracker.class);

   private final AtomicReferenceArray<Set<byte[]>> keysPerSegment;
   private final SegmentConsistentHash segmentConsistentHash;

   public SegmentKeyTracker(SegmentConsistentHash segmentConsistentHash) {
      int numSegments = segmentConsistentHash.getNumSegments();
      keysPerSegment = new AtomicReferenceArray<>(numSegments);
      if (log.isDebugEnabled()) log.debugf("Created SegmentKeyTracker with %d segments", numSegments);
      this.segmentConsistentHash = segmentConsistentHash;
      IntStream.range(0, segmentConsistentHash.getNumSegments())
              .forEach(i -> keysPerSegment.set(i, CollectionFactory.makeSet(ByteArrayEquivalence.INSTANCE)));
   }

   public boolean track(byte[] key) {
      int segment = segmentConsistentHash.getSegment(key);
      boolean result = keysPerSegment.get(segment).add(key);
      if (log.isDebugEnabled())
         log.debugf("Tracking key %s belonging to segment %d, seenBefore? = %s", Util.printArray(key), segment, !result);
      return result;
   }

   public Set<Integer> missedSegments() {
      int length = keysPerSegment.length();
      if (length == 0) return null;
      Set<Integer> missed = new HashSet<>(length);
      for (int i = 0; i < keysPerSegment.length(); i++) {
         if (keysPerSegment.get(i) != null) {
            missed.add(i);
         }
      }
      return missed;
   }

   public void segmentsFinished(byte[] finishedSegments) {
      if (finishedSegments != null) {
         BitSet bitSet = BitSet.valueOf(finishedSegments);
         if (log.isDebugEnabled()) log.debugf("Removing completed segments %s", bitSet);
         bitSet.stream().forEach(seg -> keysPerSegment.set(seg, null));
      }
   }
}
