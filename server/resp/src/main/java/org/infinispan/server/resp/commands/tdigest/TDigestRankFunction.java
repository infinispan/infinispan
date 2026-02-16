package org.infinispan.server.resp.commands.tdigest;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get rank values from a T-Digest using FunctionalMap.
 * Used by TDIGEST.RANK command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_RANK_FUNCTION)
public final class TDigestRankFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Long>> {

   private final List<Double> values;

   public TDigestRankFunction(List<Double> values) {
      this.values = values;
   }

   @ProtoFactory
   TDigestRankFunction(List<Double> values, boolean dummy) {
      this(values);
   }

   @ProtoField(number = 1)
   public List<Double> getValues() {
      return values;
   }

   @Override
   public List<Long> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Long> results = new ArrayList<>();
      for (Double value : values) {
         results.add(tdigest.rank(value));
      }
      return results;
   }
}
