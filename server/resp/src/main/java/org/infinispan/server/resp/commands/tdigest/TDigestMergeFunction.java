package org.infinispan.server.resp.commands.tdigest;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to merge multiple T-Digests into one using FunctionalMap.
 * Used by TDIGEST.MERGE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_TDIGEST_MERGE_FUNCTION)
public final class TDigestMergeFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final List<TDigest> sources;
   private final int compression;

   public TDigestMergeFunction(List<TDigest> sources, int compression) {
      this.sources = sources;
      this.compression = compression;
   }

   @ProtoFactory
   TDigestMergeFunction(List<TDigest> sources, int compression, boolean dummy) {
      this(sources, compression);
   }

   @ProtoField(number = 1)
   public List<TDigest> getSources() {
      return sources;
   }

   @ProtoField(number = 2, defaultValue = "100")
   public int getCompression() {
      return compression;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (sources.isEmpty()) {
         throw new IllegalArgumentException("ERR no source keys provided");
      }

      // Get or create destination
      TDigest dest = (TDigest) view.peek().orElse(null);
      if (dest == null) {
         dest = new TDigest(compression);
      }

      // Merge all sources
      for (TDigest source : sources) {
         dest.merge(source);
      }

      view.set(dest);
      return true;
   }
}
