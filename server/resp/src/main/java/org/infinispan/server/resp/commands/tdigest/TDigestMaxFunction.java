package org.infinispan.server.resp.commands.tdigest;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get maximum value from a T-Digest using FunctionalMap.
 * Used by TDIGEST.MAX command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_MAX_FUNCTION)
public final class TDigestMaxFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, Double> {

   public static final TDigestMaxFunction INSTANCE = new TDigestMaxFunction();

   private TDigestMaxFunction() {
   }

   @Override
   public Double apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }
      return tdigest.max();
   }
}
