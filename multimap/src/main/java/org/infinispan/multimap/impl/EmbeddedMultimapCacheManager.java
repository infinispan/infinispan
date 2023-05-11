package org.infinispan.multimap.impl;

import static org.infinispan.multimap.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;
import org.infinispan.multimap.configuration.EmbeddedMultimapConfiguration;
import org.infinispan.multimap.configuration.MultimapCacheManagerConfiguration;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Embedded implementation of {@link MultimapCacheManager}
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class EmbeddedMultimapCacheManager<K, V> implements MultimapCacheManager<K, V> {

   private static final String MULTIMAP_SCOPE = "multimap";

   private final EmbeddedCacheManager cacheManager;
   private final Map<String, EmbeddedMultimapConfiguration> preConfigured;
   private final AdvancedCache<ScopedState, EmbeddedMultimapConfiguration> stateCache;
   private final Map<String, CompletionStage<MultimapCache<K, V>>> caches = new ConcurrentHashMap<>(16);

   public EmbeddedMultimapCacheManager(EmbeddedCacheManager embeddedMultimapCacheManager) {
      this.cacheManager = embeddedMultimapCacheManager;
      GlobalConfiguration globalConfiguration = embeddedMultimapCacheManager.getGlobalComponentRegistry().getGlobalConfiguration();
      MultimapCacheManagerConfiguration configuration = globalConfiguration.module(MultimapCacheManagerConfiguration.class);
      this.preConfigured = configuration == null
            ? Collections.emptyMap()
            : configuration.multimaps();
      this.stateCache = cacheManager
            .<ScopedState, EmbeddedMultimapConfiguration>getCache(GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME)
            .getAdvancedCache();
   }

   @Override
   public Configuration defineConfiguration(String name, Configuration configuration) {
      Configuration registered = cacheManager.defineConfiguration(name, configuration);
      boolean defined = false;
      MultimapCacheManagerConfiguration cmc = configuration.module(MultimapCacheManagerConfiguration.class);
      if (cmc != null) {
         EmbeddedMultimapConfiguration mc = cmc.multimaps().get(name);
         if (mc != null) {
            CompletionStages.join(defineConfigurationAsync(mc));
            defined = true;
         }
      }

      if (!defined) {
         EmbeddedMultimapConfiguration mc = configuration.module(EmbeddedMultimapConfiguration.class);
         if (mc != null && mc.name().equals(name)) {
            CompletionStages.join(defineConfigurationAsync(mc));
         }
      }
      return registered;
   }

   @Override
   public CompletionStage<Boolean> defineConfiguration(Configuration cacheConfiguration, EmbeddedMultimapConfiguration multimapConfiguration) {
      cacheManager.defineConfiguration(multimapConfiguration.name(), cacheConfiguration);
      return defineConfiguration(multimapConfiguration);
   }

   @Override
   public CompletionStage<Boolean> defineConfiguration(EmbeddedMultimapConfiguration configuration) {
      return defineConfigurationAsync(configuration);
   }

   private CompletionStage<Boolean> defineConfigurationAsync(EmbeddedMultimapConfiguration configuration) {
      final String name = configuration.name();
      return checkGlobalConfiguration(name)
            .thenCompose(existent -> {
               if (existent != null) {
                  // Already statically defined.
                  return CompletableFutures.completedFalse();
               }

               return stateCache.putIfAbsentAsync(stateKey(name), configuration)
                     .thenApply(Objects::isNull);
            });
   }

   private CompletionStage<EmbeddedMultimapConfiguration> checkGlobalConfiguration(String name) {
      for (EmbeddedMultimapConfiguration configuration : preConfigured.values()) {
         if (configuration.name().equals(name)) {
            return stateCache.putIfAbsentAsync(stateKey(name), configuration)
                  .thenApply(c -> c == null ? configuration : c);
         }
      }
      return CompletableFutures.completedNull();
   }

   private static ScopedState stateKey(String name) {
      return new ScopedState(MULTIMAP_SCOPE, name);
   }

   @Override
   public MultimapCache<K, V> get(String name) {
      CompletionStage<MultimapCache<K, V>> cs = caches.computeIfAbsent(name, this::createMultimapCache);
      return CompletionStages.join(cs);
   }

   @Override
   public MultimapCache<K, V> get(String name, boolean supportsDuplicates) {
      CompletionStage<MultimapCache<K, V>> cs = caches.computeIfAbsent(name, ignore -> {
         Cache<K, Bucket<V>> cache = cacheManager.getCache(name, true);
         return CompletableFuture.completedFuture(new EmbeddedMultimapCache<>(cache, supportsDuplicates));
      });
      return CompletionStages.join(cs);
   }

   /**
    * Provides an api to manipulate key/values with lists.
    *
    * @param cacheName, name of the cache
    * @return EmbeddedMultimapListCache
    */
   public EmbeddedMultimapListCache<K, V> getMultimapList(String cacheName) {
      Cache<K, ListBucket<V>> cache = cacheManager.getCache(cacheName);
      if (cache == null) {
         throw new IllegalStateException("Cache must exist: " + cacheName);
      }
      return new EmbeddedMultimapListCache<>(cache);
   }

   private CompletionStage<MultimapCache<K, V>> createMultimapCache(String name) {
      EmbeddedMultimapConfiguration configuration = stateCache.get(stateKey(name));
      if (configuration == null) {
         return checkGlobalConfiguration(name)
               .thenApply(cfg -> {
                  if (cfg == null) {
                     throw CONTAINER.undefinedConfiguration(name);
                  }
                  return new EmbeddedMultimapCache<>(cacheManager.getCache(name, true), cfg.supportsDuplicates());
               });
      }

      Cache<K, Bucket<V>> cache = cacheManager.getCache(name, true);
      return CompletableFuture.completedFuture(new EmbeddedMultimapCache<>(cache, configuration.supportsDuplicates()));
   }
}
