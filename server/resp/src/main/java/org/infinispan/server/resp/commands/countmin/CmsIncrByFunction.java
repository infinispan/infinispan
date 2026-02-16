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
 * Function to increment item counts in a Count-Min Sketch using FunctionalMap.
 * Used by CMS.INCRBY command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CMS_INCRBY_FUNCTION)
public final class CmsIncrByFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, List<Long>> {

   private final List<byte[]> items;
   private final List<Long> increments;

   public CmsIncrByFunction(List<byte[]> items, List<Long> increments) {
      this.items = items;
      this.increments = increments;
   }

   @ProtoFactory
   CmsIncrByFunction(List<byte[]> items, List<Long> increments, boolean dummy) {
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
   public List<Long> apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      CountMinSketch sketch = (CountMinSketch) view.peek().orElse(null);
      if (sketch == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Long> results = new ArrayList<>(items.size());
      for (int i = 0; i < items.size(); i++) {
         long count = sketch.incrBy(items.get(i), increments.get(i));
         results.add(count);
      }
      view.set(sketch);
      return results;
   }
}
