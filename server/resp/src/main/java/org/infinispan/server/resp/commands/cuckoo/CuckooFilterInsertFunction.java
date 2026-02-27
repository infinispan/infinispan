package org.infinispan.server.resp.commands.cuckoo;

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
 * Function to insert items into a Cuckoo filter with creation options using FunctionalMap.
 * Used by CF.INSERT and CF.INSERTNX commands.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_INSERT_FUNCTION)
public final class CuckooFilterInsertFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, List<Integer>> {

   private final List<byte[]> items;
   private final long capacity;
   private final boolean noCreate;
   private final boolean onlyIfNotExists;

   public CuckooFilterInsertFunction(List<byte[]> items, long capacity, boolean noCreate, boolean onlyIfNotExists) {
      this.items = items;
      this.capacity = capacity;
      this.noCreate = noCreate;
      this.onlyIfNotExists = onlyIfNotExists;
   }

   @ProtoFactory
   CuckooFilterInsertFunction(MarshallableList<byte[]> items, long capacity, boolean noCreate, boolean onlyIfNotExists) {
      this.items = MarshallableList.unwrap(items);
      this.capacity = capacity;
      this.noCreate = noCreate;
      this.onlyIfNotExists = onlyIfNotExists;
   }

   @ProtoField(1)
   MarshallableList<byte[]> getItems() {
      return MarshallableList.create(items);
   }

   @ProtoField(number = 2, defaultValue = "1024")
   public long getCapacity() {
      return capacity;
   }

   @ProtoField(number = 3, defaultValue = "false")
   public boolean isNoCreate() {
      return noCreate;
   }

   @ProtoField(number = 4, defaultValue = "false")
   public boolean isOnlyIfNotExists() {
      return onlyIfNotExists;
   }

   @Override
   public List<Integer> apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      CuckooFilter filter = (CuckooFilter) view.peek().orElse(null);

      if (filter == null) {
         if (noCreate) {
            throw new IllegalStateException("ERR not found");
         }
         filter = new CuckooFilter(capacity, CuckooFilter.DEFAULT_BUCKET_SIZE,
               CuckooFilter.DEFAULT_MAX_ITERATIONS, CuckooFilter.DEFAULT_EXPANSION);
      }

      List<Integer> results = new ArrayList<>(items.size());
      for (byte[] item : items) {
         boolean added;
         if (onlyIfNotExists) {
            added = filter.addNx(item);
         } else {
            added = filter.add(item);
         }
         // Return 1 for success, 0 for already exists (NX), -1 for filter full
         if (added) {
            results.add(1);
         } else if (onlyIfNotExists) {
            results.add(0);
         } else {
            results.add(-1);
         }
      }

      view.set(filter);
      return results;
   }
}
