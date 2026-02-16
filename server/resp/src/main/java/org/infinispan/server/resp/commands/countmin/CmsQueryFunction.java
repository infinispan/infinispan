package org.infinispan.server.resp.commands.countmin;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to query item counts in a Count-Min Sketch using FunctionalMap.
 * Used by CMS.QUERY command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CMS_QUERY_FUNCTION)
public final class CmsQueryFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Long>> {

   private final List<byte[]> items;

   public CmsQueryFunction(List<byte[]> items) {
      this.items = items;
   }

   @ProtoFactory
   CmsQueryFunction(List<byte[]> items, boolean dummy) {
      this(items);
   }

   @ProtoField(number = 1)
   public List<byte[]> getItems() {
      return items;
   }

   @Override
   public List<Long> apply(EntryView.ReadEntryView<byte[], Object> view) {
      CountMinSketch sketch = (CountMinSketch) view.peek().orElse(null);
      if (sketch == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Long> results = new ArrayList<>(items.size());
      for (byte[] item : items) {
         results.add(sketch.query(item));
      }
      return results;
   }
}
