package org.infinispan.server.resp.commands.countmin;

import java.util.List;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.util.function.SerializableFunction;

/**
 * Function to merge multiple Count-Min Sketches into one using FunctionalMap.
 * Used by CMS.MERGE command.
 *
 * @since 16.2
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_CMS_MERGE_FUNCTION)
public final class CmsMergeFunction
      implements SerializableFunction<EntryView.ReadWriteEntryView<byte[], Object>, Boolean> {

   private final List<CountMinSketch> sources;
   private final List<Double> weights;

   @ProtoFactory
   public CmsMergeFunction(List<CountMinSketch> sources, List<Double> weights) {
      this.sources = sources;
      this.weights = weights;
   }

   @ProtoField(number = 1)
   public List<CountMinSketch> getSources() {
      return sources;
   }

   @ProtoField(number = 2)
   public List<Double> getWeights() {
      return weights;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<byte[], Object> view) {
      if (sources.isEmpty()) {
         throw new IllegalArgumentException(ProbabilisticErrors.CMS_KEY_NOT_FOUND);
      }

      CountMinSketch first = sources.get(0);
      int width = first.getWidth();
      int depth = first.getDepth();

      // Validate all sources have same dimensions
      for (CountMinSketch src : sources) {
         if (src.getWidth() != width || src.getDepth() != depth) {
            throw new IllegalArgumentException(ProbabilisticErrors.CMS_WIDTH_DEPTH_MISMATCH);
         }
      }

      // Get destination sketch — must already exist
      CountMinSketch dest = (CountMinSketch) view.peek().orElse(null);
      if (dest == null) {
         throw new IllegalStateException(ProbabilisticErrors.CMS_KEY_NOT_FOUND);
      }
      if (dest.getWidth() != width || dest.getDepth() != depth) {
         throw new IllegalArgumentException(ProbabilisticErrors.CMS_WIDTH_DEPTH_MISMATCH);
      }

      // Merge all sources
      for (int i = 0; i < sources.size(); i++) {
         double weight = (weights != null && i < weights.size()) ? weights.get(i) : 1.0;
         dest.merge(sources.get(i), weight);
      }

      view.set(dest);
      return true;
   }
}
