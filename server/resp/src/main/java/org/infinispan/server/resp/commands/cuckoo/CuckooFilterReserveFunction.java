package org.infinispan.server.resp.commands.cuckoo;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to reserve (create) a Cuckoo filter with specific parameters using FunctionalMap.
 * Used by CF.RESERVE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_RESERVE_FUNCTION)
public final class CuckooFilterReserveFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final long capacity;
   private final int bucketSize;
   private final int maxIterations;
   private final int expansion;

   public CuckooFilterReserveFunction(long capacity, int bucketSize, int maxIterations, int expansion) {
      this.capacity = capacity;
      this.bucketSize = bucketSize;
      this.maxIterations = maxIterations;
      this.expansion = expansion;
   }

   @ProtoFactory
   CuckooFilterReserveFunction(long capacity, int bucketSize, int maxIterations, int expansion, boolean dummy) {
      this(capacity, bucketSize, maxIterations, expansion);
   }

   @ProtoField(number = 1, defaultValue = "1024")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 2, defaultValue = "2")
   public int getBucketSize() {
      return bucketSize;
   }

   @ProtoField(number = 3, defaultValue = "20")
   public int getMaxIterations() {
      return maxIterations;
   }

   @ProtoField(number = 4, defaultValue = "1")
   public int getExpansion() {
      return expansion;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (view.peek().isPresent()) {
         throw new IllegalStateException("ERR item exists");
      }

      CuckooFilter filter = new CuckooFilter(capacity, bucketSize, maxIterations, expansion);
      view.set(filter);
      return true;
   }
}
