package org.infinispan.server.resp.commands.countmin;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to initialize a Count-Min Sketch using FunctionalMap.
 * Used by CMS.INITBYDIM and CMS.INITBYPROB commands.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CMS_INIT_FUNCTION)
public final class CmsInitFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final int width;
   private final int depth;

   public CmsInitFunction(int width, int depth) {
      this.width = width;
      this.depth = depth;
   }

   @ProtoFactory
   CmsInitFunction(int width, int depth, boolean dummy) {
      this(width, depth);
   }

   @ProtoField(number = 1, defaultValue = "2000")
   public int getWidth() {
      return width;
   }

   @ProtoField(number = 2, defaultValue = "7")
   public int getDepth() {
      return depth;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (view.peek().isPresent()) {
         throw new IllegalStateException("ERR item exists");
      }

      CountMinSketch sketch = new CountMinSketch(width, depth);
      view.set(sketch);
      return true;
   }
}
