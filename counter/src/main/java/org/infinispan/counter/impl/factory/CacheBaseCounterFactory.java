package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.listener.CounterManagerNotificationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.concurrent.BlockingManager;

/**
 * Base class to create counters stored in a {@link Cache}.
 *
 * @since 14.0
 */
@Scope(Scopes.NONE)
public abstract class CacheBaseCounterFactory<K extends CounterKey> {

   @Inject BlockingManager blockingManager;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject CounterManagerNotificationManager notificationManager;

   protected CompletionStage<AdvancedCache<K, CounterValue>> cache(CounterConfiguration configuration) {
      return configuration.storage() == Storage.VOLATILE ?
            getCounterCacheAsync().thenApply(this::transformCacheToVolatile) :
            getCounterCacheAsync();
   }

   CompletionStage<AdvancedCache<K, CounterValue>> getCounterCacheAsync() {
      return blockingManager.supplyBlocking(this::getCounterCacheSync, "get-counter-cache");
   }

   private AdvancedCache<K, CounterValue> getCounterCacheSync() {
      Cache<K, CounterValue> cache = cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME);
      return cache.getAdvancedCache();
   }

   private AdvancedCache<K, CounterValue> transformCacheToVolatile(AdvancedCache<K, CounterValue> cache) {
      return cache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE);
   }

}
