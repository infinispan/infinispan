package org.infinispan.server.resp.commands.bloom;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to reserve (create) a Bloom filter with specific parameters using FunctionalMap.
 * Used by BF.RESERVE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_BLOOM_RESERVE_FUNCTION)
public final class BloomFilterReserveFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final double errorRate;
   private final long capacity;
   private final int expansion;
   private final boolean nonScaling;

   public BloomFilterReserveFunction(double errorRate, long capacity, int expansion, boolean nonScaling) {
      this.errorRate = errorRate;
      this.capacity = capacity;
      this.expansion = expansion;
      this.nonScaling = nonScaling;
   }

   @ProtoFactory
   BloomFilterReserveFunction(double errorRate, long capacity, int expansion, boolean nonScaling, boolean dummy) {
      this(errorRate, capacity, expansion, nonScaling);
   }

   @ProtoField(number = 1, defaultValue = "0.01")
   public double getErrorRate() {
      return errorRate;
   }

   @ProtoField(number = 2, defaultValue = "100")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 3, defaultValue = "2")
   public int getExpansion() {
      return expansion;
   }

   @ProtoField(number = 4, defaultValue = "false")
   public boolean isNonScaling() {
      return nonScaling;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (view.peek().isPresent()) {
         throw new IllegalStateException("ERR item exists");
      }

      BloomFilter filter = new BloomFilter(errorRate, capacity, expansion, nonScaling);
      view.set(filter);
      return true;
   }
}
