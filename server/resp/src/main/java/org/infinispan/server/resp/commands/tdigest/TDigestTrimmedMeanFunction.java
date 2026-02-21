package org.infinispan.server.resp.commands.tdigest;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to get trimmed mean from a T-Digest using FunctionalMap.
 * Used by TDIGEST.TRIMMED_MEAN command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_TRIMMED_MEAN_FUNCTION)
public final class TDigestTrimmedMeanFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, Double> {

   private final double lowFraction;
   private final double highFraction;

   public TDigestTrimmedMeanFunction(double lowFraction, double highFraction) {
      this.lowFraction = lowFraction;
      this.highFraction = highFraction;
   }

   @ProtoFactory
   TDigestTrimmedMeanFunction(double lowFraction, double highFraction, boolean dummy) {
      this(lowFraction, highFraction);
   }

   @ProtoField(number = 1, defaultValue = "0")
   public double getLowFraction() {
      return lowFraction;
   }

   @ProtoField(number = 2, defaultValue = "1")
   public double getHighFraction() {
      return highFraction;
   }

   @Override
   public Double apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }
      return tdigest.trimmedMean(lowFraction, highFraction);
   }
}
