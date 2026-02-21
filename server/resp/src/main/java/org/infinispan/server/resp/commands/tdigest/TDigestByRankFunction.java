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
 * Function to get values by rank from a T-Digest using FunctionalMap.
 * Used by TDIGEST.BYRANK command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_BYRANK_FUNCTION)
public final class TDigestByRankFunction
      implements SerializableFunction<EntryView.ReadEntryView<byte[], Object>, List<Double>> {

   private final List<Long> ranks;

   public TDigestByRankFunction(List<Long> ranks) {
      this.ranks = ranks;
   }

   @ProtoFactory
   TDigestByRankFunction(List<Long> ranks, boolean dummy) {
      this(ranks);
   }

   @ProtoField(number = 1)
   public List<Long> getRanks() {
      return ranks;
   }

   @Override
   public List<Double> apply(EntryView.ReadEntryView<byte[], Object> view) {
      TDigest tdigest = (TDigest) view.peek().orElse(null);
      if (tdigest == null) {
         throw new IllegalStateException("ERR not found");
      }

      List<Double> results = new ArrayList<>();
      for (Long rank : ranks) {
         results.add(tdigest.byRank(rank));
      }
      return results;
   }
}
