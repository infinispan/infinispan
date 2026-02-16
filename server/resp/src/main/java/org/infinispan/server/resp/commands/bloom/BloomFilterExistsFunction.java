package org.infinispan.server.resp.commands.bloom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to check if items exist in a Bloom filter using FunctionalMap.
 * Used by BF.EXISTS and BF.MEXISTS commands.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_EXISTS_FUNCTION)
public final class BloomFilterExistsFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Integer>> {

   private final List<byte[]> items;

   public BloomFilterExistsFunction(List<byte[]> items) {
      this.items = items;
   }

   @ProtoFactory
   BloomFilterExistsFunction(MarshallableList<byte[]> items) {
      this.items = MarshallableList.unwrap(items);
   }

   @ProtoField(1)
   MarshallableList<byte[]> getItems() {
      return MarshallableList.create(items);
   }

   @Override
   public List<Integer> apply(EntryView.ReadEntryView<byte[], Object> view) {
      BloomFilter filter = (BloomFilter) view.peek().orElse(null);
      if (filter == null) {
         return Collections.nCopies(items.size(), 0);
      }

      List<Integer> results = new ArrayList<>(items.size());
      for (byte[] item : items) {
         results.add(filter.mightContain(item) ? 1 : 0);
      }
      return results;
   }
}
