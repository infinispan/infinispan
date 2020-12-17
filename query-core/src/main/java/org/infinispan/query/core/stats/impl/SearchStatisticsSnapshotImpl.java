package org.infinispan.query.core.stats.impl;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.IndexStatisticsSnapshot;
import org.infinispan.query.core.stats.QueryStatisticsSnapshot;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;

/**
 * Query and Index statistics for a Cache.
 * <p>
 * since 12.0
 */
@ProtoTypeId(ProtoStreamTypeIds.SEARCH_STATISTICS)
public final class SearchStatisticsSnapshotImpl implements SearchStatisticsSnapshot {
   private final QueryStatisticsSnapshot queryStatistics;
   private final IndexStatisticsSnapshot indexStatistics;

   public SearchStatisticsSnapshotImpl() {
      this(new LocalQueryStatistics(), new IndexStatisticsSnapshotImpl());
   }

   @ProtoFactory
   public SearchStatisticsSnapshotImpl(LocalQueryStatistics queryStatistics, IndexStatisticsSnapshotImpl indexStatistics) {
      this.queryStatistics = queryStatistics;
      this.indexStatistics = indexStatistics;
   }

   public SearchStatisticsSnapshotImpl(QueryStatisticsSnapshot queryStatistics, IndexStatisticsSnapshot indexStatistics) {
      this.queryStatistics = queryStatistics;
      this.indexStatistics = indexStatistics;
   }

   @Override
   @ProtoField(number = 1, javaType = LocalQueryStatistics.class)
   public QueryStatisticsSnapshot getQueryStatistics() {
      return queryStatistics;
   }

   @Override
   @ProtoField(number = 2, javaType = IndexStatisticsSnapshotImpl.class)
   public IndexStatisticsSnapshot getIndexStatistics() {
      return indexStatistics;
   }

   @Override
   public SearchStatisticsSnapshot merge(SearchStatisticsSnapshot other) {
      queryStatistics.merge(other.getQueryStatistics());
      indexStatistics.merge(other.getIndexStatistics());
      return this;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("query", Json.make(queryStatistics))
            .set("index", Json.make(indexStatistics));
   }
}
