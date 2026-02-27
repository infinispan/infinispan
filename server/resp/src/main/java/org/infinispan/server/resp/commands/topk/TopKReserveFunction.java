package org.infinispan.server.resp.commands.topk;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to reserve (create) a Top-K filter using FunctionalMap.
 * Used by TOPK.RESERVE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_RESERVE_FUNCTION)
public final class TopKReserveFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final int k;
   private final int width;
   private final int depth;
   private final double decay;

   public TopKReserveFunction(int k, int width, int depth, double decay) {
      this.k = k;
      this.width = width;
      this.depth = depth;
      this.decay = decay;
   }

   @ProtoFactory
   TopKReserveFunction(int k, int width, int depth, double decay, boolean dummy) {
      this(k, width, depth, decay);
   }

   @ProtoField(number = 1, defaultValue = "10")
   public int getK() {
      return k;
   }

   @ProtoField(number = 2, defaultValue = "8")
   public int getWidth() {
      return width;
   }

   @ProtoField(number = 3, defaultValue = "7")
   public int getDepth() {
      return depth;
   }

   @ProtoField(number = 4, defaultValue = "0.9")
   public double getDecay() {
      return decay;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (view.peek().isPresent()) {
         throw new IllegalStateException("ERR item exists");
      }

      TopK topK = new TopK(k, width, depth, decay);
      view.set(topK);
      return true;
   }
}
