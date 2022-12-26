package org.infinispan.hotrod.impl.iteration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;

/**
 * @since 14.0
 */
class SegmentKeyTracker implements KeyTracker {

   private static final Log log = LogFactory.getLog(SegmentKeyTracker.class);

   private final AtomicReferenceArray<Set<WrappedByteArray>> keysPerSegment;
   private final SegmentConsistentHash segmentConsistentHash;
   private final DataFormat dataFormat;

   public SegmentKeyTracker(DataFormat dataFormat, SegmentConsistentHash segmentConsistentHash, Set<Integer> segments) {
      this.dataFormat = dataFormat;
      int numSegments = segmentConsistentHash.getNumSegments();
      keysPerSegment = new AtomicReferenceArray<>(numSegments);
      if (log.isTraceEnabled())
         log.tracef("Created SegmentKeyTracker with %d segments, filter %s", numSegments, segments);
      this.segmentConsistentHash = segmentConsistentHash;
      IntStream segmentStream = segments == null ?
            IntStream.range(0, segmentConsistentHash.getNumSegments()) : segments.stream().mapToInt(i -> i);
      segmentStream.forEach(i -> keysPerSegment.set(i, new HashSet<>()));
   }

   public boolean track(byte[] key, short status, ClassAllowList allowList) {
      int segment = dataFormat.isObjectStorage() ?
            segmentConsistentHash.getSegment(dataFormat.keyToObj(key, allowList)) :
            segmentConsistentHash.getSegment(key);
      Set<WrappedByteArray> keys = keysPerSegment.get(segment);
      if (keys == null) throw new IllegalStateException("Segment " + segment + " already completed");
      boolean result = keys.add(new WrappedByteArray(key));
      if (log.isTraceEnabled())
         log.trackingSegmentKey(Util.printArray(key), segment, !result);
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

   public void segmentsFinished(IntSet finishedSegments) {
      if (finishedSegments != null) {
         if (log.isTraceEnabled())
            log.tracef("Removing completed segments %s", finishedSegments);
         finishedSegments.forEach((IntConsumer) seg -> keysPerSegment.set(seg, null));
      }
   }
}
