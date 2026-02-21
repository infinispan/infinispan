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
 * Function to increment item counts in a Top-K filter using FunctionalMap.
 * Used by TOPK.INCRBY command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_INCRBY_FUNCTION)
public final class TopKIncrByFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, List<String>> {

   private final List<byte[]> items;
   private final List<Long> increments;

   public TopKIncrByFunction(List<byte[]> items, List<Long> increments) {
      this.items = items;
      this.increments = increments;
   }

   @ProtoFactory
   TopKIncrByFunction(List<byte[]> items, List<Long> increments, boolean dummy) {
      this(items, increments);
   }

   @ProtoField(number = 1)
   public List<byte[]> getItems() {
      return items;
   }

   @ProtoField(number = 2)
   public List<Long> getIncrements() {
      return increments;
   }

   @Override
   public List<String> apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<String> expelled = new ArrayList<>();
      for (int i = 0; i < items.size(); i++) {
         String exp = topK.incrBy(items.get(i), increments.get(i));
         expelled.add(exp); // may be null
      }
      view.set(topK);
      return expelled;
   }
}
