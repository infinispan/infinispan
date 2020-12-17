package org.infinispan.query.core.stats;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A snapshot of {@link QueryStatistics}.
 *
 * @see QueryStatistics#computeSnapshot()
 * @since 12.0
 */
public interface QueryStatisticsSnapshot extends QueryStatistics, JsonSerialization {

   @Override
   default CompletionStage<QueryStatisticsSnapshot> computeSnapshot() {
      return CompletableFuture.completedFuture(this);
   }

   /**
    * Merge with another {@link QueryStatisticsSnapshot}
    *
    * @return self.
    */
   QueryStatisticsSnapshot merge(QueryStatisticsSnapshot other);

}
