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
 * Function to get quantile values from a T-Digest using FunctionalMap.
 * Used by TDIGEST.QUANTILE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_QUANTILE_FUNCTION)
public final class TDigestQuantileFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Double>> {

   private final List<Double> fractions;

   public TDigestQuantileFunction(List<Double> fractions) {
      this.fractions = fractions;
   }

   @ProtoFactory
   TDigestQuantileFunction(List<Double> fractions, boolean dummy) {
      this(fractions);
   }

   @ProtoField(number = 1)
   public List<Double> getFractions() {
      return fractions;
   }

   @Override
   public List<Double> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Double> results = new ArrayList<>();
      for (Double fraction : fractions) {
         results.add(tdigest.quantile(fraction));
      }
      return results;
   }
}
