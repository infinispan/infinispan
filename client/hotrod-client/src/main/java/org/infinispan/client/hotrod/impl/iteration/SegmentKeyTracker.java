package org.infinispan.client.hotrod.impl.iteration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.Util;

/**
 * @author gustavonalle
 * @since 8.0
 */
class SegmentKeyTracker implements KeyTracker {

   private static final Log log = LogFactory.getLog(SegmentKeyTracker.class);
   private final boolean trace = log.isTraceEnabled();

   private final AtomicReferenceArray<Set<WrappedByteArray>> keysPerSegment;
   private final SegmentConsistentHash segmentConsistentHash;
   private final DataFormat dataFormat;
   private volatile boolean trackSegments = true;
   private Set<WrappedByteArray> keyOnlyTracker = new HashSet<>();

   public SegmentKeyTracker(DataFormat dataFormat, SegmentConsistentHash segmentConsistentHash, Set<Integer> segments) {
      this.dataFormat = dataFormat;
      int numSegments = segmentConsistentHash.getNumSegments();
      keysPerSegment = new AtomicReferenceArray<>(numSegments);
      if (trace)
         log.tracef("Created SegmentKeyTracker with %d segments, filter %s", numSegments, segments);
      this.segmentConsistentHash = segmentConsistentHash;
      IntStream segmentStream = segments == null ?
            IntStream.range(0, segmentConsistentHash.getNumSegments()) : segments.stream().mapToInt(i -> i);
      segmentStream.forEach(i -> keysPerSegment.set(i, new HashSet<>()));
   }

   private void drainKeys() {
      for (int i = 0; i < keysPerSegment.length(); i++) {
         Set<WrappedByteArray> keys = keysPerSegment.get(i);
         if (keys != null) keyOnlyTracker.addAll(keys);
         keysPerSegment.set(i, null);
      }
   }

   public boolean track(byte[] key, short status, ClassAllowList allowList) {
      if (!trackSegments) return keyOnlyTracker.add(new WrappedByteArray(key));

      int segment = HotRodConstants.isObjectStorage(status) ?
            segmentConsistentHash.getSegment(dataFormat.keyToObj(key, allowList)) :
            segmentConsistentHash.getSegment(key);
      Set<WrappedByteArray> keys = keysPerSegment.get(segment);
      if (keys == null) {
         trackSegments = false;
         this.drainKeys();
         return keyOnlyTracker.add(new WrappedByteArray(key));
      }
      boolean result = keys.add(new WrappedByteArray(key));
      if (trace)
         log.trackingSegmentKey(Util.printArray(key), segment, !result);
      return result;
   }

   public Set<Integer> missedSegments() {
      if (!trackSegments) return null;
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
      if (trackSegments && finishedSegments != null) {
         if (trace)
            log.tracef("Removing completed segments %s", finishedSegments);
         finishedSegments.forEach((IntConsumer) seg -> keysPerSegment.set(seg, null));
      }
   }
}
