package org.infinispan.server.resp.commands.tdigest;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to reset a T-Digest using FunctionalMap.
 * Used by TDIGEST.RESET command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_RESET_FUNCTION)
public final class TDigestResetFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   public static final TDigestResetFunction INSTANCE = new TDigestResetFunction();

   private TDigestResetFunction() {
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }

      tdigest.reset();
      view.set(tdigest);
      return true;
   }
}
