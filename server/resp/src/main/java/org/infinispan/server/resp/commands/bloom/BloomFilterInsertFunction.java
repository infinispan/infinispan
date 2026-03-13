package org.infinispan.server.resp.commands.bloom;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to insert items into a Bloom filter with creation options using FunctionalMap.
 * Used by BF.INSERT command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_INSERT_FUNCTION)
public final class BloomFilterInsertFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, List<Integer>> {

   private final List<byte[]> items;
   private final long capacity;
   private final double errorRate;
   private final int expansion;
   private final boolean noCreate;
   private final boolean nonScaling;

   public BloomFilterInsertFunction(List<byte[]> items, long capacity, double errorRate,
                                    int expansion, boolean noCreate, boolean nonScaling) {
      this.items = items;
      this.capacity = capacity;
      this.errorRate = errorRate;
      this.expansion = expansion;
      this.noCreate = noCreate;
      this.nonScaling = nonScaling;
   }

   @ProtoFactory
   BloomFilterInsertFunction(MarshallableList<byte[]> items, long capacity, double errorRate,
                             int expansion, boolean noCreate, boolean nonScaling) {
      this.items = MarshallableList.unwrap(items);
      this.capacity = capacity;
      this.errorRate = errorRate;
      this.expansion = expansion;
      this.noCreate = noCreate;
      this.nonScaling = nonScaling;
   }

   @ProtoField(1)
   MarshallableList<byte[]> getItems() {
      return MarshallableList.create(items);
   }

   @ProtoField(number = 2, defaultValue = "100")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 3, defaultValue = "0.01")
   public double getErrorRate() {
      return errorRate;
   }

   @ProtoField(number = 4, defaultValue = "2")
   public int getExpansion() {
      return expansion;
   }

   @ProtoField(number = 5, defaultValue = "false")
   public boolean isNoCreate() {
      return noCreate;
   }

   @ProtoField(number = 6, defaultValue = "false")
   public boolean isNonScaling() {
      return nonScaling;
   }

   @Override
   public List<Integer> apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      BloomFilter filter = (BloomFilter) view.peek().orElse(null);

      if (filter == null) {
         if (noCreate) {
            throw new IllegalStateException("ERR not found");
         }
         filter = new BloomFilter(errorRate, capacity, expansion, nonScaling);
      }

      List<Integer> results = new ArrayList<>(items.size());
      for (byte[] item : items) {
         boolean added = filter.add(item);
         results.add(added ? 1 : 0);
      }

      view.set(filter);
      return results;
   }
}
