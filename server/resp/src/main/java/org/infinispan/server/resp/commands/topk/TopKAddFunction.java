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
 * Function to add items to a Top-K filter using FunctionalMap.
 * Used by TOPK.ADD command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_ADD_FUNCTION)
public final class TopKAddFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, List<String>> {

   private final List<byte[]> items;

   public TopKAddFunction(List<byte[]> items) {
      this.items = items;
   }

   @ProtoFactory
   TopKAddFunction(List<byte[]> items, boolean dummy) {
      this(items);
   }

   @ProtoField(number = 1)
   public List<byte[]> getItems() {
      return items;
   }

   @Override
   public List<String> apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<String> expelled = new ArrayList<>();
      for (byte[] item : items) {
         String exp = topK.add(item);
         expelled.add(exp); // may be null
      }
      view.set(topK);
      return expelled;
   }
}
