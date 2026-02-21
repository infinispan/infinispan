package org.infinispan.server.resp.commands.countmin;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get information about a Count-Min Sketch using FunctionalMap.
 * Used by CMS.INFO command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CMS_INFO_FUNCTION)
public final class CmsInfoFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, CmsInfoFunction.CmsInfo> {

   public static final CmsInfoFunction INSTANCE = new CmsInfoFunction();

   private CmsInfoFunction() {
   }

   @Override
   public CmsInfo apply(EntryView.ReadEntryView<byte[], Object> view) {
      CountMinSketch sketch = (CountMinSketch) view.peek().orElse(null);
      if (sketch == null) {
         throw new IllegalStateException("ERR not found");
      }
      return new CmsInfo(sketch.getWidth(), sketch.getDepth(), sketch.getTotalCount());
   }

   public static final class CmsInfo {
      private final int width;
      private final int depth;
      private final long count;

      public CmsInfo(int width, int depth, long count) {
         this.width = width;
         this.depth = depth;
         this.count = count;
      }

      public Map<String, Long> toMap() {
         Map<String, Long> map = new LinkedHashMap<>();
         map.put("width", (long) width);
         map.put("depth", (long) depth);
         map.put("count", count);
         return map;
      }
   }
}
