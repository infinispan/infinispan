package org.infinispan.query.core.stats.impl;

import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;

import java.util.concurrent.CompletionStage;

/**
 * Query and Index statistics for a Cache.
 *
 * since 12.0
 */
public final class SearchStatisticsImpl implements SearchStatistics {
   private QueryStatistics queryStatistics;
   private IndexStatistics indexStatistics;

   public SearchStatisticsImpl(QueryStatistics queryStatistics, IndexStatistics indexStatistics) {
      this.queryStatistics = queryStatistics;
      this.indexStatistics = indexStatistics;
   }

   @Override
   public QueryStatistics getQueryStatistics() {
      return queryStatistics;
   }

   @Override
   public IndexStatistics getIndexStatistics() {
      return indexStatistics;
   }

   @Override
   public CompletionStage<SearchStatisticsSnapshot> computeSnapshot() {
      return queryStatistics.computeSnapshot()
              .thenCombine(indexStatistics.computeSnapshot(), SearchStatisticsSnapshotImpl::new);
   }
}
