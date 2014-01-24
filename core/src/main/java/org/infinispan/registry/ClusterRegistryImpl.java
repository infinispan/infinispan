package org.infinispan.registry;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.KeyFilter;
import org.infinispan.transaction.TransactionMode;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of the ClusterRegistry. Stores all the information in a replicated cache that is lazily
 * instantiated on the first access. This means that if the EmbeddedCacheManager doesn't use the metadata the
 * underlying cache never gets instantiated.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public class ClusterRegistryImpl<S, K, V> implements ClusterRegistry<S, K, V> {

   public static final String GLOBAL_REGISTRY_CACHE_NAME = "__cluster_registry_cache__";

   private EmbeddedCacheManager cacheManager;
   private volatile Cache<ScopedKey<S, K>, V> clusterRegistryCache;
   private volatile AdvancedCache<ScopedKey<S, K>, V> clusterRegistryCacheWithoutReturn;

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public void put(S scope, K key, V value) {
      if (value == null) throw new IllegalArgumentException("Null values are not allowed");
      startRegistryCache();
      clusterRegistryCacheWithoutReturn.put(new ScopedKey<S, K>(scope, key), value);
   }

   @Override
   public void put(S scope, K key, V value, long lifespan, TimeUnit unit) {
      if (value == null) throw new IllegalArgumentException("Null values are not allowed");
      startRegistryCache();
      clusterRegistryCacheWithoutReturn.put(new ScopedKey<S, K>(scope, key), value, lifespan, unit);
   }

   @Override
   public void remove(S scope, K key) {
      startRegistryCache();
      clusterRegistryCacheWithoutReturn.remove(new ScopedKey<S, K>(scope, key));
   }

   @Override
   public V get(S scope, K key) {
      startRegistryCache();
      return clusterRegistryCache.get(new ScopedKey<S, K>(scope, key));
   }

   @Override
   public boolean containsKey(S scope, K key) {
      V v = get(scope, key);
      return v != null;
   }

   @Override
   public Set<K> keys(S scope) {
      startRegistryCache();
      Set<K> result = new HashSet<K>();
      for (ScopedKey<S, K> key : clusterRegistryCache.keySet()) {
         if (key.hasScope(scope)) {
            result.add(key.getKey());
         }
      }
      return result;
   }

   @Override
   public void clear(S scope) {
      startRegistryCache();
      for (ScopedKey<S, K> key : clusterRegistryCache.keySet()) {
         if (key.hasScope(scope)) {
            clusterRegistryCacheWithoutReturn.remove(key);
         }
      }
   }

   @Override
   public void clearAll() {
      startRegistryCache();
      clusterRegistryCache.clear();
   }

   @Override
   public void addListener(final S scope, final Object listener) {
      startRegistryCache();
      clusterRegistryCache.addListener(listener, new KeyFilter() {
         @Override
         public boolean accept(Object key) {
            // All keys are known to be of type ScopedKey
            ScopedKey<S, K> scopedKey = (ScopedKey<S, K>) key;
            return scopedKey.hasScope(scope);
         }
      });
   }

   @Override
   public void addListener(final S scope, final KeyFilter keyFilter, final Object listener) {
      startRegistryCache();
      clusterRegistryCache.addListener(listener, new KeyFilter() {
         @Override
         public boolean accept(Object key) {
            // All keys are known to be of type ScopedKey
            ScopedKey<S, K> scopedKey = (ScopedKey<S, K>) key;
            return scopedKey.hasScope(scope) && keyFilter.accept(scopedKey.getKey());
         }
      });
   }

   @Override
   public void removeListener(Object listener) {
      if (clusterRegistryCache != null) {
         clusterRegistryCache.removeListener(listener);
      }
   }

   /**
    * Start the cache lazily: if the cluster registry is not needed at all then this won't ever be started.
    */
   private void startRegistryCache() {
      if (clusterRegistryCache == null) {
         synchronized (this) {
            if (clusterRegistryCache != null) return;
            cacheManager.defineConfiguration(GLOBAL_REGISTRY_CACHE_NAME, getRegistryCacheConfig());
            clusterRegistryCache = cacheManager.getCache(GLOBAL_REGISTRY_CACHE_NAME);
            clusterRegistryCacheWithoutReturn = clusterRegistryCache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES);
         }
      }
   }

   private Configuration getRegistryCacheConfig() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

      //allow the registry to work for local caches as well as clustered caches
      CacheMode cacheMode = isClustered() ? CacheMode.REPL_SYNC : CacheMode.LOCAL;
      configurationBuilder.clustering().cacheMode(cacheMode);

      //use a transactional cache for high consistency as writes are expected to be rare in this cache
      configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      //fetch the state (redundant as state transfer this is enabled by default, keep it here to document the intention)
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);

      return configurationBuilder.build();
   }

   private boolean isClustered() {
      GlobalConfiguration globalConfiguration = cacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      return globalConfiguration.isClustered();
   }
}
