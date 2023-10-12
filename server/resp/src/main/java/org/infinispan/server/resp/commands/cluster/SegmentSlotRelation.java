package org.infinispan.server.resp.commands.cluster;

import static org.infinispan.commons.logging.Log.CONFIG;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.Util;

public class SegmentSlotRelation {

   // This is the default number of slots a clustered Redis has. Which is not configurable.
   // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
   public static final int SLOT_SIZE = 1 << 14;
   private final int segmentCount;
   private final int width;

   public SegmentSlotRelation(int segmentSize) {
      if (!Util.isPow2(segmentSize))
         throw CONFIG.respCacheSegmentSizePow2(segmentSize);
      this.segmentCount = segmentSize;
      if (segmentSize > SLOT_SIZE) {
         throw new IllegalArgumentException("Number of segments cannot be larger than " + SLOT_SIZE);
      }
      this.width = SLOT_SIZE / segmentSize;
   }

   public int hashToSlot(int hash) {
      return hash % SLOT_SIZE;
   }

   public IntSet segmentToSlots(int segment) {
      assert segment >= 0 && segment < segmentCount : "Not provided a segment value";

      int start = segment * width;
      int end = segment * width + width;
      return IntSets.immutableOffsetIntSet(start, end);
   }

   public int slotToSegment(int slot) {
      assert slot >= 0 && slot < SLOT_SIZE : "Not provided a slot value";

      return slot / width;
   }

   public int slotWidth() {
      return width;
   }
}
