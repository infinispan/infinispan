package org.infinispan.query.core.stats;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;
import org.infinispan.query.impl.ComponentRegistryUtils;

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


   /**
    * Returns aggregated search statistics for all nodes in the cluster.
    */
   static CompletionStage<SearchStatisticsSnapshot> of(Cache<?, ?> cache) {
      return ComponentRegistryUtils.getSearchStatsRetriever(cache).getDistributedSearchStatistics();
   }
}
