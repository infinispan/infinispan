package org.infinispan.server.resp.commands.cuckoo;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get information about a Cuckoo filter using FunctionalMap.
 * Used by CF.INFO command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_INFO_FUNCTION)
public final class CuckooFilterInfoFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, CuckooFilterInfoFunction.CuckooFilterInfo> {

   public static final CuckooFilterInfoFunction INSTANCE = new CuckooFilterInfoFunction();

   public CuckooFilterInfoFunction() {
   }

   @Override
   public CuckooFilterInfo apply(EntryView.ReadEntryView<byte[], Object> view) {
      CuckooFilter filter = (CuckooFilter) view.peek().orElse(null);
      if (filter == null) {
         throw new IllegalStateException("ERR not found");
      }

      return new CuckooFilterInfo(
            filter.getSize(),
            filter.getTotalBuckets(),
            filter.getFilterCount(),
            filter.getItemsInserted(),
            filter.getItemsDeleted(),
            filter.getBucketSize(),
            filter.getExpansion(),
            filter.getMaxIterations()
      );
   }

   /**
    * Result of CF.INFO command.
    */
   public record CuckooFilterInfo(
         long size,
         long numBuckets,
         int numFilters,
         long itemsInserted,
         long itemsDeleted,
         int bucketSize,
         int expansionRate,
         int maxIterations
   ) {
      public Map<String, Long> toMap() {
         Map<String, Long> result = new LinkedHashMap<>();
         result.put("Size", size);
         result.put("Number of buckets", numBuckets);
         result.put("Number of filter", (long) numFilters);
         result.put("Number of items inserted", itemsInserted);
         result.put("Number of items deleted", itemsDeleted);
         result.put("Bucket size", (long) bucketSize);
         result.put("Expansion rate", (long) expansionRate);
         result.put("Max iteration", (long) maxIterations);
         return result;
      }
   }
}
