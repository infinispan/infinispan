package org.infinispan.query.core.stats.impl;

import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.concurrent.CompletionStages;

@ProtoTypeId(ProtoStreamTypeIds.STATS_TASK)
public class StatsTask implements Function<EmbeddedCacheManager, SearchStatisticsSnapshot> {

   @ProtoField(number = 1)
   String cacheName;

   @ProtoFactory
   public StatsTask(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public SearchStatisticsSnapshot apply(EmbeddedCacheManager cacheManager) {
      Cache<?, ?> cache = SecurityActions.getCache(cacheManager, cacheName);
      SearchStatsRetriever searchStatsRetriever = SecurityActions.getCacheComponentRegistry(cache.getAdvancedCache()).getComponent(SearchStatsRetriever.class);
      return CompletionStages.join(searchStatsRetriever.getSearchStatistics().computeSnapshot());
   }
}
