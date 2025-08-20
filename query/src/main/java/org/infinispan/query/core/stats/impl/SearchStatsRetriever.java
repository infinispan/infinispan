package org.infinispan.query.core.stats.impl;

import java.util.Collection;
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
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;
import org.infinispan.security.actions.SecurityActions;

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

   public CompletionStage<SearchStatisticsSnapshot> getDistributedSearchStatistics() {
      StatsTask statsTask = new StatsTask(cache.getName());
      ClusterExecutor clusterExecutor = SecurityActions.getClusterExecutor(cache);
      Collection<SearchStatisticsSnapshot> stats = new ConcurrentLinkedQueue<>();
      return clusterExecutor.submitConsumer(statsTask, (address, searchStats, throwable) -> {
         if (throwable != null) {
            Throwable rootCause = Util.getRootCause(throwable);
            throw new CacheException("Error obtaining statistics from node", rootCause);
         }
         stats.add(searchStats);
      }).thenApply(v -> stats.stream().reduce(new SearchStatisticsSnapshotImpl(), SearchStatisticsSnapshot::merge));
   }
}
