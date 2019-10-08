package org.infinispan.server.core;

import static org.infinispan.server.core.LifecycleCallbacks.SERVER_STATE_CACHE;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;

/**
 * Manages the cache blacklisting for a given {@link EmbeddedCacheManager}
 *
 * @since 10.0
 */
public final class CacheIgnoreManager {
   private static final String IGNORED_CACHES_KEY = "ignored-caches";

   private final Cache<String, Set<String>> cache;
   private final Set<String> ignoredCaches = ConcurrentHashMap.newKeySet();
   private final Object listener = new CacheListener();

   public CacheIgnoreManager(EmbeddedCacheManager cacheManager) {
      this.cache = cacheManager.getCache(SERVER_STATE_CACHE);
      this.updateLocalCopy(cache.get(IGNORED_CACHES_KEY));
      this.cache.addListener(listener);
   }

   public CompletableFuture<Void> unignoreCache(String cacheName) {
      ignoredCaches.remove(cacheName);
      return cache.putAsync(IGNORED_CACHES_KEY, ignoredCaches).thenApply(r -> null);
   }

   public CompletableFuture<Void> ignoreCache(String cacheName) {
      ignoredCaches.add(cacheName);
      return cache.putAsync(IGNORED_CACHES_KEY, ignoredCaches).thenApply(r -> null);
   }

   public Set<String> getIgnoredCaches() {
      return Collections.unmodifiableSet(ignoredCaches);
   }

   public boolean isCacheIgnored(String cacheName) {
      return ignoredCaches.contains(cacheName);
   }

   private void updateLocalCopy(Set<String> value) {
      if (value != null) {
         synchronized (this) {
            ignoredCaches.clear();
            ignoredCaches.addAll(value);
         }
      }
   }

   public void stop() {
      if (cache != null) {
         cache.removeListener(listener);
      }
   }

   @Listener
   private final class CacheListener {
      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<String, Set<String>> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getKey().equals(IGNORED_CACHES_KEY)) {
            updateLocalCopy(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<String, Set<String>> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getKey().equals(IGNORED_CACHES_KEY)) {
            updateLocalCopy(e.getValue());
         }
      }
   }

}
