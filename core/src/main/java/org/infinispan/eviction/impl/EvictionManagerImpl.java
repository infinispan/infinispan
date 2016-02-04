package org.infinispan.eviction.impl;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.ImmutableContext;
import org.infinispan.eviction.EvictionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.CacheMgmtInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.notifications.cachelistener.CacheNotifier;

import java.util.List;
import java.util.Map;

@ThreadSafe
public class EvictionManagerImpl<K, V> implements EvictionManager<K, V> {
   // components to be injected
   private CacheNotifier<K, V> cacheNotifier;
   private InterceptorChain interceptorChain;
   private Configuration cfg;

   @Inject
   public void initialize(CacheNotifier<K, V> cacheNotifier, Configuration cfg,  InterceptorChain chain) {
      this.cacheNotifier = cacheNotifier;
      this.cfg = cfg;
      this.interceptorChain = chain;
   }

   @Override
   public void onEntryEviction(Map<? extends K, InternalCacheEntry<? extends K, ? extends V>> evicted) {
      // don't reuse the threadlocal context as we don't want to include eviction
      // operations in any ongoing transaction, nor be affected by flags
      // especially see ISPN-1154: it's illegal to acquire locks in a committing transaction
      InvocationContext ctx = ImmutableContext.INSTANCE;
      // This is important because we make no external guarantees on the thread
      // that will execute this code, so it could be the user thread, or could
      // be the eviction thread.
      // However, when a user calls cache.evict(), you do want to carry over the
      // contextual information, hence it makes sense for the notifyCacheEntriesEvicted()
      // call to carry on taking an InvocationContext object.
      cacheNotifier.notifyCacheEntriesEvicted(evicted.values(), ctx, null);

      if (cfg.jmxStatistics().enabled()) {
         updateEvictionStatistics(evicted);
      }
   }

   private void updateEvictionStatistics(Map<? extends K, InternalCacheEntry<? extends K, ? extends V>> evicted) {
      List<CommandInterceptor> interceptors = interceptorChain.getInterceptorsWhichExtend(CacheMgmtInterceptor.class);
      if (!interceptors.isEmpty()) {
         CacheMgmtInterceptor mgmtInterceptor = (CacheMgmtInterceptor) interceptors.get(0);
         mgmtInterceptor.addEvictions(evicted.size());
      }
   }
}
