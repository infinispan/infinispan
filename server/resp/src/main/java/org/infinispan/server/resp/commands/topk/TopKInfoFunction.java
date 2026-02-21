package org.infinispan.server.resp.commands.topk;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get information about a Top-K filter using FunctionalMap.
 * Used by TOPK.INFO command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_INFO_FUNCTION)
public final class TopKInfoFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, TopKInfoFunction.TopKInfo> {

   public static final TopKInfoFunction INSTANCE = new TopKInfoFunction();

   private TopKInfoFunction() {
   }

   @Override
   public TopKInfo apply(EntryView.ReadEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException("ERR not found");
      }
      return new TopKInfo(topK.getK(), topK.getWidth(), topK.getDepth(), topK.getDecay());
   }

   public static final class TopKInfo {
      private final int k;
      private final int width;
      private final int depth;
      private final double decay;

      public TopKInfo(int k, int width, int depth, double decay) {
         this.k = k;
         this.width = width;
         this.depth = depth;
         this.decay = decay;
      }

      public Map<String, Object> toMap() {
         Map<String, Object> map = new LinkedHashMap<>();
         map.put("k", (long) k);
         map.put("width", (long) width);
         map.put("depth", (long) depth);
         map.put("decay", decay);
         return map;
      }
   }
}
