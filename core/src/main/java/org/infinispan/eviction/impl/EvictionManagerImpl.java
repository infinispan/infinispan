package org.infinispan.eviction.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.context.impl.ImmutableContext;
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

import net.jcip.annotations.ThreadSafe;

@Scope(Scopes.NAMED_CACHE)
@ThreadSafe
public class EvictionManagerImpl<K, V> implements EvictionManager<K, V> {
   @Inject CacheNotifier<K, V> cacheNotifier;
   @Inject ComponentRef<AsyncInterceptorChain> interceptorChain;
   @Inject Configuration cfg;
   @Inject StatsCollector simpleCacheStatsCollector;

   private CacheMgmtInterceptor cacheMgmtInterceptor;

   @Start
   public void findCacheMgmtInterceptor() {
      // Allow the interceptor chain to start later, otherwise we'd have a dependency cycle
      cacheMgmtInterceptor = interceptorChain.wired().findInterceptorExtending(CacheMgmtInterceptor.class);
   }

   @Override
   public CompletionStage<Void> onEntryEviction(Map<K, Map.Entry<K,V>> evicted, FlagAffectedCommand command) {
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
