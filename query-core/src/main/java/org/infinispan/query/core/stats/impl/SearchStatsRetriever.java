package org.infinispan.query.core.stats.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.Util;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;

/**
 * Retrieves {@link SearchStatistics} for a cache.
 *
 * @since 12.0
 */
@Scope(Scopes.NAMED_CACHE)
public class SearchStatsRetriever {
   @Inject LocalQueryStatistics localQueryStatistics;
   @Inject IndexStatistics localIndexStatistics;
   @Inject Cache<?, ?> cache;

   public SearchStatistics getSearchStatistics() {
      return new SearchStatisticsImpl(localQueryStatistics, localIndexStatistics);
   }

   SearchStatistics getSnapshot() {
      return new SearchStatisticsImpl(localQueryStatistics.getSnapshot(), localIndexStatistics.getSnapshot());
   }

   private QueryStatistics mergeQueryStats(Collection<QueryStatistics> clusterStats) {
      return clusterStats.stream().reduce(new LocalQueryStatistics(), QueryStatistics::merge);
   }

   private IndexStatistics mergeIndexStats(Collection<IndexStatistics> indexStatistics) {
      return indexStatistics.stream().reduce(new IndexStatisticSnapshot(new HashMap<>()), IndexStatistics::merge);
   }

   public CompletionStage<SearchStatistics> getDistributedSearchStatistics() {
      StatsTask statsTask = new StatsTask(cache.getName());
      ClusterExecutor clusterExecutor = SecurityActions.getClusterExecutor(cache);
      Collection<QueryStatistics> queryStats = new ConcurrentLinkedQueue<>();
      Collection<IndexStatistics> indexStats = new ConcurrentLinkedQueue<>();
      return clusterExecutor.submitConsumer(statsTask, (address, searchStats, throwable) -> {
         if (throwable != null) {
            Throwable rootCause = Util.getRootCause(throwable);
            throw new CacheException("Error obtaining statistics from node", rootCause);
         }
         queryStats.add(searchStats.getQueryStatistics());
         indexStats.add(searchStats.getIndexStatistics());
      }).thenApply(v -> new SearchStatisticsImpl(mergeQueryStats(queryStats), mergeIndexStats(indexStats)));
   }
}
