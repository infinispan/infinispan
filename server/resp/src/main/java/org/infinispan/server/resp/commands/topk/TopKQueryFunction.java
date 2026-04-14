package org.infinispan.server.resp.commands.topk;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to query items in a Top-K filter using FunctionalMap.
 * Used by TOPK.QUERY command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_QUERY_FUNCTION)
public final class TopKQueryFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Long>> {

   private final List<byte[]> items;

   @ProtoFactory
   public TopKQueryFunction(List<byte[]> items) {
      this.items = items;
   }

   @ProtoField(number = 1)
   public List<byte[]> getItems() {
      return items;
   }

   @Override
   public List<Long> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException(ProbabilisticErrors.TOPK_KEY_NOT_FOUND);
      }

      List<Long> results = new ArrayList<>();
      for (byte[] item : items) {
         results.add(topK.query(item) ? 1L : 0L);
      }
      return results;
   }
}
