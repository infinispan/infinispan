package org.infinispan.server.resp.commands.tdigest;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to add values to a T-Digest using FunctionalMap.
 * Used by TDIGEST.ADD command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_ADD_FUNCTION)
public final class TDigestAddFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final List<Double> values;

   public TDigestAddFunction(List<Double> values) {
      this.values = values;
   }

   @ProtoFactory
   TDigestAddFunction(List<Double> values, boolean dummy) {
      this(values);
   }

   @ProtoField(number = 1)
   public List<Double> getValues() {
      return values;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }

      for (Double value : values) {
         tdigest.add(value);
      }
      view.set(tdigest);
      return true;
   }
}
