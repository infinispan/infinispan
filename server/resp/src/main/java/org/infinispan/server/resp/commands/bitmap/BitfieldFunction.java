package org.infinispan.server.resp.commands.bitmap;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

@ProtoTypeId(ProtoStreamTypeIds.RESP_BITFIELD_FUNCTION)
public final class BitfieldFunction implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], byte[]>, List<Long>> {

   private final List<BitfieldOperation> operations;

   public BitfieldFunction(List<BitfieldOperation> operations) {
      this.operations = operations;
   }

   @ProtoFactory
   BitfieldFunction(MarshallableList<BitfieldOperation> operations) {
      this.operations = MarshallableList.unwrap(operations);
   }

   @ProtoField(1)
   MarshallableList<BitfieldOperation> getValues() {
      return MarshallableList.create(operations);
   }

   @Override
   public List<Long> apply(EntryView.ReadWriteEntryView<byte[], byte[]> entryView) {
      if (operations.isEmpty()) {
         return Collections.emptyList();
      } else {
         Optional<byte[]> existing = entryView.peek();
         AtomicReference<byte[]> valueRef = new AtomicReference<>(existing.orElseGet(() -> new byte[0]));
         List<Long> results = BitfieldOperation.apply(operations, valueRef);
         entryView.set(valueRef.get());
         return results;
      }
   }
}
