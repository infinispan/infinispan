package org.infinispan.server.core;

import static org.infinispan.server.core.LifecycleCallbacks.SERVER_STATE_CACHE;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Manages the cache blacklisting for a given {@link EmbeddedCacheManager}
 *
 * @since 10.0
 */
public final class CacheIgnoreManager {
   private static final String IGNORED_CACHES_KEY = "ignored-caches";

   private final Cache<String, IgnoredCaches> cache;
   private final IgnoredCaches ignored = new IgnoredCaches();
   private final Object listener = new CacheListener();
   private volatile boolean hasIgnores;
   private volatile boolean stopped;

   public CacheIgnoreManager(EmbeddedCacheManager cacheManager) {
      this.cache = cacheManager.getCache(SERVER_STATE_CACHE);
      this.updateLocalCopy(cache.get(IGNORED_CACHES_KEY));
      this.cache.addListener(listener);
   }

   public CompletableFuture<Void> unignoreCache(String cacheName) {
      synchronized (this) {
         ignored.caches.remove(cacheName);
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   public CompletableFuture<Void> ignoreCache(String cacheName) {
      synchronized (this) {
         ignored.caches.add(cacheName);
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);

      }
   }

   public Set<String> getIgnoredCaches() {
      return Collections.unmodifiableSet(ignored.caches);
   }

   boolean isCacheIgnored(String cacheName) {
      return hasIgnores && ignored.caches.contains(cacheName);
   }

   private void updateLocalCopy(IgnoredCaches ignored) {
      if (ignored != null) {
         synchronized (this) {
            this.ignored.caches.clear();
            this.ignored.caches.addAll(ignored.caches);
            hasIgnores = !this.ignored.caches.isEmpty();
         }
      }
   }

   public void stop() {
      synchronized (this) {
         if (!stopped)
            if (cache != null) {
               cache.removeListener(listener);
            }
         stopped = true;
      }
   }

   @Listener
   private final class CacheListener {
      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<String, IgnoredCaches> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getKey().equals(IGNORED_CACHES_KEY)) {
            updateLocalCopy(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<String, IgnoredCaches> e) {
         if (!e.isOriginLocal() && !e.isPre() && e.getKey().equals(IGNORED_CACHES_KEY)) {
            updateLocalCopy(e.getValue());
         }
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.IGNORED_CACHES)
   static class IgnoredCaches {
      final Set<String> caches;

      IgnoredCaches() {
         this(ConcurrentHashMap.newKeySet());
      }

      @ProtoFactory
      IgnoredCaches(Set<String> caches) {
         // ProtoStream cannot use KeySetView directly as it does not have a zero args constructor
         this.caches = ConcurrentHashMap.newKeySet(caches.size());
         this.caches.addAll(caches);
      }

      @ProtoField(number = 1, collectionImplementation = HashSet.class)
      Set<String> getCaches() {
         return caches;
      }
   }
}
