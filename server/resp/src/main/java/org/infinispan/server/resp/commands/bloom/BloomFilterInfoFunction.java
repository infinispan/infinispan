package org.infinispan.server.resp.commands.bloom;

import java.util.LinkedHashMap;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get information about a Bloom filter using FunctionalMap.
 * Used by BF.INFO command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_INFO_FUNCTION)
public final class BloomFilterInfoFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, BloomFilterInfoFunction.BloomFilterInfo> {

   public enum InfoType {
      ALL, CAPACITY, SIZE, FILTERS, ITEMS, EXPANSION
   }

   public static final BloomFilterInfoFunction INSTANCE = new BloomFilterInfoFunction();

   @Override
   public BloomFilterInfo apply(EntryView.ReadEntryView<byte[], Object> view) {
      BloomFilter filter = (BloomFilter) view.peek().orElse(null);
      if (filter == null) {
         throw new IllegalStateException(ProbabilisticErrors.ERR_NOT_FOUND);
      }

      return new BloomFilterInfo(
            filter.getTotalCapacity(),
            filter.getSize(),
            filter.getFilterCount(),
            filter.getItemCount(),
            filter.getExpansion()
      );
   }

   /**
    * Result of BF.INFO command.
    */
   @ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_INFO)
   public record BloomFilterInfo(
         long capacity,
         long size,
         int filters,
         long items,
         int expansion
   ) {
      public Map<String, Long> toMap(InfoType requestedType) {
         Map<String, Long> result = new LinkedHashMap<>();
         switch (requestedType) {
            case ALL:
               result.put("Capacity", capacity);
               result.put("Size", size);
               result.put("Number of filters", (long) filters);
               result.put("Number of items inserted", items);
               result.put("Expansion rate", (long) expansion);
               break;
            case CAPACITY:
               result.put("Capacity", capacity);
               break;
            case SIZE:
               result.put("Size", size);
               break;
            case FILTERS:
               result.put("Number of filters", (long) filters);
               break;
            case ITEMS:
               result.put("Number of items inserted", items);
               break;
            case EXPANSION:
               result.put("Expansion rate", (long) expansion);
               break;
         }
         return result;
      }
   }
}
