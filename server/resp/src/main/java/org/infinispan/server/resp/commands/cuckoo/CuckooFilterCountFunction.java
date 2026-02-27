package org.infinispan.server.resp.commands.cuckoo;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to count occurrences of an item in a Cuckoo filter using FunctionalMap.
 * Used by CF.COUNT command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CUCKOO_COUNT_FUNCTION)
public final class CuckooFilterCountFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, Long> {

   private final byte[] item;

   public CuckooFilterCountFunction(byte[] item) {
      this.item = item;
   }

   @ProtoFactory
   CuckooFilterCountFunction(byte[] item, boolean dummy) {
      this.item = item;
   }

   @ProtoField(number = 1)
   public byte[] getItem() {
      return item;
   }

   @Override
   public Long apply(EntryView.ReadEntryView<byte[], Object> view) {
      CuckooFilter filter = (CuckooFilter) view.peek().orElse(null);
      if (filter == null) {
         return 0L;
      }
      return filter.count(item);
   }
}
