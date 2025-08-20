package org.infinispan.query.core.stats;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A snapshot of {@link SearchStatistics}.
 *
 * @since 12.0
 */
public interface SearchStatisticsSnapshot extends SearchStatistics, JsonSerialization {

   @Override
   QueryStatisticsSnapshot getQueryStatistics();

   @Override
   IndexStatisticsSnapshot getIndexStatistics();

   @Override
   default CompletionStage<SearchStatisticsSnapshot> computeSnapshot() {
      return CompletableFuture.completedFuture(this);
   }

   /**
    * Merge with another {@link SearchStatisticsSnapshot}
    *
    * @return self.
    */
   SearchStatisticsSnapshot merge(SearchStatisticsSnapshot other);
}
