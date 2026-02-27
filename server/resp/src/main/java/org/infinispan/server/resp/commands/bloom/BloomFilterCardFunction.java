package org.infinispan.server.resp.commands.bloom;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get the cardinality (number of items) of a Bloom filter using FunctionalMap.
 * Used by BF.CARD command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_CARD_FUNCTION)
public final class BloomFilterCardFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, Long> {

   public static final BloomFilterCardFunction INSTANCE = new BloomFilterCardFunction();

   public BloomFilterCardFunction() {
   }

   @Override
   public Long apply(EntryView.ReadEntryView<byte[], Object> view) {
      BloomFilter filter = (BloomFilter) view.peek().orElse(null);
      if (filter == null) {
         return 0L;
      }
      return filter.getItemCount();
   }
}
