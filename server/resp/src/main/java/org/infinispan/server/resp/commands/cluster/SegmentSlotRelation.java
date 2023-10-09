package org.infinispan.server.resp.commands.cluster;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.distribution.ch.impl.CRC16HashFunctionPartitioner;

public class SegmentSlotRelation {

   // This is the default number of slots a clustered Redis has. Which is not configurable.
   // See: https://redis.io/docs/reference/cluster-spec/#key-distribution-model
   static final int SLOT_SIZE = 1 << 14;
   private final int segmentSize;
   private final int width;

   public SegmentSlotRelation(int segmentSize) {
      if (!CRC16HashFunctionPartitioner.isPow2(segmentSize))
         throw CONFIG.respCacheSegmentSizePow2(segmentSize);
      this.segmentSize = segmentSize;
      this.width = 1 << Integer.numberOfTrailingZeros(SLOT_SIZE) - Integer.numberOfTrailingZeros(segmentSize);
   }

   public int segmentToSingleSlot(int hash, int segment) {
      assert segment >= 0 && segment < segmentSize : "Not provided a segment value";
      return segmentSize * reduce(hash) + segment;
   }

   public Collection<Integer> segmentToSlots(int segment) {
      assert segment >= 0 && segment < segmentSize : "Not provided a segment value";

      List<Integer> slots = new ArrayList<>();
      for (int i = 0; i < width; i++) {
         slots.add(segment + segmentSize * i);
      }
      return slots;
   }

   public int slotToSegment(int slot) {
      assert slot >= 0 && slot < SLOT_SIZE : "Not provided a slot value";

      return slot % segmentSize;
   }

   private int reduce(int hash) {
      return (hash / segmentSize) % width;
   }
}
