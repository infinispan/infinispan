package org.infinispan.eviction.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.conflict.impl.SegmentHashTracker;
import org.infinispan.conflict.impl.SegmentHasher;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.stats.impl.StatsCollector;

import com.google.errorprone.annotations.ThreadSafe;

@Scope(Scopes.NAMED_CACHE)
@ThreadSafe
public class EvictionManagerImpl<K, V> implements EvictionManager<K, V> {
   @Inject CacheNotifier<K, V> cacheNotifier;
   @Inject ComponentRef<AsyncInterceptorChain> interceptorChain;
   @Inject Configuration cfg;
   @Inject StatsCollector simpleCacheStatsCollector;
   @Inject KeyPartitioner keyPartitioner;

   private CacheMgmtInterceptor cacheMgmtInterceptor;
   private SegmentHashTracker segmentHashTracker;

   @Start
   public void findCacheMgmtInterceptor() {
      // Allow the interceptor chain to start later, otherwise we'd have a dependency cycle
      cacheMgmtInterceptor = interceptorChain.wired().findInterceptorExtending(CacheMgmtInterceptor.class);
   }

   public void setSegmentHashTracker(SegmentHashTracker tracker) {
      this.segmentHashTracker = tracker;
   }

   @Override
   public CompletionStage<Void> onEntryEviction(Map<K, Map.Entry<K,V>> evicted, FlagAffectedCommand command) {
      if (segmentHashTracker != null && segmentHashTracker.isEnabled() && !segmentHashTracker.hasStores()) {
         for (Map.Entry<K, Map.Entry<K, V>> e : evicted.entrySet()) {
            K key = e.getKey();
            V value = e.getValue().getValue();
            int segment = keyPartitioner.getSegment(key);
            SegmentHasher.HashAndBucket hb = segmentHashTracker.computeHashAndBucket(key, value);
            segmentHashTracker.recordRemove(segment, hb.bucket(), hb.hash());
         }
      }
      CompletionStage<Void> stage = cacheNotifier.notifyCacheEntriesEvicted(evicted.values(), ImmutableContext.INSTANCE, command);
      if (cfg.statistics().enabled()) {
         updateEvictionStatistics(evicted);
      }

      return stage;
   }

   private void updateEvictionStatistics(Map<K, Map.Entry<K, V>> evicted) {
      if (cacheMgmtInterceptor != null) {
         cacheMgmtInterceptor.addEvictions(evicted.size());
      } else if (simpleCacheStatsCollector != null) {
         simpleCacheStatsCollector.recordEvictions(evicted.size());
      }
   }
}
