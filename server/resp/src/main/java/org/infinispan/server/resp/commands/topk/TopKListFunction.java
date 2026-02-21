package org.infinispan.server.resp.commands.topk;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to list items in a Top-K filter using FunctionalMap.
 * Used by TOPK.LIST command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TOPK_LIST_FUNCTION)
public final class TopKListFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Object>> {

   private final boolean withCount;

   public TopKListFunction(boolean withCount) {
      this.withCount = withCount;
   }

   @ProtoFactory
   TopKListFunction(boolean withCount, boolean dummy) {
      this(withCount);
   }

   @ProtoField(number = 1, defaultValue = "false")
   public boolean isWithCount() {
      return withCount;
   }

   @Override
   public List<Object> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TopK topK = (TopK) view.peek().orElse(null);
      if (topK == null) {
         throw new IllegalStateException("ERR not found");
      }

      return topK.list(withCount);
   }
}
