package org.infinispan.server.resp.commands.tdigest;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get minimum value from a T-Digest using FunctionalMap.
 * Used by TDIGEST.MIN command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_MIN_FUNCTION)
public final class TDigestMinFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, Double> {

   public static final TDigestMinFunction INSTANCE = new TDigestMinFunction();

   private TDigestMinFunction() {
   }

   @Override
   public Double apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }
      return tdigest.min();
   }
}
