package org.infinispan.server.resp.commands.cuckoo;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to delete an item from a Cuckoo filter using FunctionalMap.
 * Used by CF.DEL command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_DEL_FUNCTION)
public final class CuckooFilterDelFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final byte[] item;

   @ProtoFactory
   public CuckooFilterDelFunction(byte[] item) {
      this.item = item;
   }

   @ProtoField(number = 1)
   public byte[] getItem() {
      return item;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      CuckooFilter filter = (CuckooFilter) view.peek().orElse(null);
      if (filter == null) {
         return false;
      }

      boolean deleted = filter.delete(item);
      if (deleted) {
         view.set(filter);
      }
      return deleted;
   }
}
