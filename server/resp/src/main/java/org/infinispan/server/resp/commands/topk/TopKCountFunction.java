package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get counts of items in a Top-K filter using FunctionalMap.
 * Used by TOPK.COUNT command (deprecated in Redis but still supported).
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_COUNT_FUNCTION)
public final class TopKCountFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Long>> {

   private final List<byte[]> items;

   public TopKCountFunction(List<byte[]> items) {
      this.items = items;
   }

   @ProtoFactory
   TopKCountFunction(List<byte[]> items, boolean dummy) {
      this(items);
   }

   @ProtoField(number = 1)
   public List<byte[]> getItems() {
      return items;
   }

   @Override
   public List<Long> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Long> results = new ArrayList<>();
      for (byte[] item : items) {
         results.add(topK.getCount(item));
      }
      return results;
   }
}
