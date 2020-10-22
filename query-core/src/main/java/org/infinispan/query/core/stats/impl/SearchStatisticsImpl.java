package org.infinispan.query.core.stats.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;

/**
 * Query and Index statistics for a Cache.
 *
 * since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SEARCH_STATISTICS)
public final class SearchStatisticsImpl implements SearchStatistics {
   private QueryStatistics queryStatistics;
   private IndexStatistics indexStatistics;

   public SearchStatisticsImpl() {
   }

   @ProtoFactory
   public SearchStatisticsImpl(LocalQueryStatistics queryStatistics, IndexStatisticSnapshot indexStatistics) {
      this.queryStatistics = queryStatistics;
      this.indexStatistics = indexStatistics;
   }

   public SearchStatisticsImpl(QueryStatistics queryStatistics, IndexStatistics indexStatistics) {
      this.queryStatistics = queryStatistics;
      this.indexStatistics = indexStatistics;
   }

   @Override
   @ProtoField(number = 1, javaType = LocalQueryStatistics.class)
   public QueryStatistics getQueryStatistics() {
      return queryStatistics;
   }

   @Override
   @ProtoField(number = 2, javaType = IndexStatisticSnapshot.class)
   public IndexStatistics getIndexStatistics() {
      return indexStatistics;
   }
}
