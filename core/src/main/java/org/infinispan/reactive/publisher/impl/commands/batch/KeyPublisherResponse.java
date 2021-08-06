package org.infinispan.reactive.publisher.impl.commands.batch;

import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;

/**
 * A Publisher Response that is used when key tracking is enabled. This is used in cases when EXACTLY_ONCE delivery
 * guarantee is needed and a map (that isn't encoder based) or flat map operation is required.
 * <p>
 * The keys array will hold all of the original keys for the mapped/flatmapped values.
 * <p>
 * The extraObjects array will only be required when using flatMap based operation. This is required as some flat map
 * operations may return more than one value. In this case it is possible to overflow the results array (sized based on
 * batch size). However since we are tracking by key we must retain all values that map to a given key in the response.
 */
public class KeyPublisherResponse extends PublisherResponse {
   final Object[] extraObjects;
   final Object[] keys;
   final int keySize;

   public KeyPublisherResponse(Object[] results, IntSet completedSegments, IntSet lostSegments, int size,
         boolean complete, Object[] extraObjects, int extraSize, Object[] keys, int keySize) {
      super(results, completedSegments, lostSegments, size, complete, extraSize);
      this.extraObjects = extraObjects;
      this.keys = keys;
      this.keySize = keySize;
   }

   // NOTE: extraSize is stored in the segmentOffset field since it isn't valid when using key tracking.
   // Normally segmentOffset is used to determine which key/entry(s) mapped to the current processing segment,
   // since we have the keys directly we don't need this field
   public int getExtraSize() {
      return segmentOffset;
   }

   public Object[] getExtraObjects() {
      return extraObjects;
   }

   @Override
   public void forEachSegmentValue(ObjIntConsumer consumer, int segment) {
      for (int i = 0; i < keySize; ++i) {
         consumer.accept(keys[i], segment);
      }
   }

   @Override
   public String toString() {
      return "KeyPublisherResponse{" +
            "size=" + size +
            ", extraSize=" + segmentOffset +
            ", keySize=" + keySize +
            ", completedSegments=" + completedSegments +
            ", lostSegments=" + lostSegments +
            ", complete=" + complete +
            '}';
   }
}
