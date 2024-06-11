package org.infinispan.client.hotrod.impl.iteration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MediaTypeMarshaller;
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
      int segment;
      // In case is an object store, we marshall the bytes to the object to retrieve the segment.
      if (dataFormat.isObjectStorage()) {
         segment = segmentConsistentHash.getSegment(dataFormat.keyToObj(key, allowList));
      } else {
         // In case we use the direct bytes we have different options.
         // If there is a server encoding, we need to transform the serialized type to an object,
         // and transform the object with the server type.
         // Since the response uses the client request type, the client marshaller transforms into an object but
         // the server marshaller transforms into byte.
         // This is a workaround for ISPN-15312.
         if (dataFormat.server() != null) {
            MediaTypeMarshaller mtm = dataFormat.server();
            segment = segmentConsistentHash.getSegment(mtm.keyToBytes(dataFormat.keyToObj(key, allowList)));
         } else {
            // Otherwise, we can just use the bytes directly.
            segment = segmentConsistentHash.getSegment(key);
         }
      }
      Set<WrappedByteArray> keys = keysPerSegment.get(segment);
      if (keys == null) {
         if (log.isTraceEnabled()) log.tracef("Key %s maps to %d which is not present", Util.toStr(key), segment, segment);
         throw new IllegalStateException("Segment " + segment + " already completed");
      }
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
