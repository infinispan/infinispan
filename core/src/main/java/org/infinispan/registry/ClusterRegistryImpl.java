package org.infinispan.registry;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.TransactionMode;

import java.util.HashSet;
import java.util.Set;

/**
 * Default implementation of the ClusterRegistry. Stores all the information in a replicated cache that is lazily
 * instantiated on the first access. This means that if the EmbeddedCacheManager doesn't use the metadata the
 * underlaying cache never gets instantiated.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public class ClusterRegistryImpl<S, K, V> implements ClusterRegistry<S, K, V> {

   public static final String GLOBAL_REGISTRY_CACHE_NAME = "__cluster_registry_cache__";

   private EmbeddedCacheManager cacheManager;
   private Cache<ScopedKey<S,K>, V> clusterRegistryCache;

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public V put(S scope, K key, V value) {
      if (value == null) throw new IllegalArgumentException("Null value not allowed!");
      startRegistryCache();
      return clusterRegistryCache.put(new ScopedKey<S,K>(scope, key), value);
   }

   @Override
   public V remove(S scope, K key) {
      startRegistryCache();
      return clusterRegistryCache.remove(new ScopedKey(scope, key));
   }

   @Override
   public V get(S scope, K key) {
      startRegistryCache();
      return clusterRegistryCache.get(new ScopedKey<S,K>(scope, key));
   }

   @Override
   public Set<K> keys(S scope) {
      startRegistryCache();
      Set result = new HashSet();
      for (ScopedKey<S,K> key : clusterRegistryCache.keySet()) {
         if (key.hasScope(scope)) {
            result.add(key.getKey());
         }
      }
      return result;
   }

   @Override
   public void clear(S scope) {
      startRegistryCache();
      for (ScopedKey key : clusterRegistryCache.keySet()) {
         if (key.hasScope(scope)) {
            clusterRegistryCache.remove(key);
         }
      }
   }

   @Override
   public void clearAll() {
      startRegistryCache();
      clusterRegistryCache.clear();
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
