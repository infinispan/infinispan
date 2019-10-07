package org.infinispan.server.core;

import static org.infinispan.server.core.LifecycleCallbacks.SERVER_STATE_CACHE;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.infinispan.commons.CacheException;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.api.embedded.EmbeddedMultimapCacheManagerFactory;
import org.infinispan.multimap.api.embedded.MultimapCache;
import org.infinispan.multimap.api.embedded.MultimapCacheManager;

/**
 * Abstract class providing stock implementations for {@link CacheIgnoreAware} so all that is required is to extend
 * this class.
 * @author gustavonalle
 * @author wburns
 * @since 9.0
 */
@SuppressWarnings("unchecked")
public class AbstractCacheIgnoreAware implements CacheIgnoreAware {
   private static final String IGNORED_CACHES_KEY = "ignored-caches";

   private MultimapCache<String, String> stateCache;

   public void initialize(EmbeddedCacheManager cacheManager) {
      MultimapCacheManager<String, String> multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cacheManager);
      stateCache = multimapCacheManager.get(SERVER_STATE_CACHE);
   }

   public CompletionStage<Boolean> unignore(String cacheName) {
      return stateCache.remove(IGNORED_CACHES_KEY, cacheName);
   }

   public CompletionStage<Void> ignoreCache(String cacheName) {
      return stateCache.put(IGNORED_CACHES_KEY, cacheName);
   }

   public boolean isCacheIgnored(String cacheName) {
      try {
         Optional<CacheEntry<String, Collection<String>>> values = stateCache.getEntry(IGNORED_CACHES_KEY).get();
         return values.map(entry -> entry.getValue().contains(cacheName)).orElse(false);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw new CacheException(e.getCause());
      }
   }

   @Override
   public CompletionStage<Collection<String>> getIgnoredCaches(String cacheManager) {
      return stateCache.get(IGNORED_CACHES_KEY);
   }
}
