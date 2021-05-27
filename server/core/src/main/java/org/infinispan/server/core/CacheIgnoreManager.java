package org.infinispan.server.core;

import static org.infinispan.server.core.LifecycleCallbacks.SERVER_STATE_CACHE;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.util.ByteString;

/**
 * Manages the cache blacklisting for a given {@link EmbeddedCacheManager}
 *
 * @since 10.0
 */
@Scope(Scopes.GLOBAL)
public final class CacheIgnoreManager {
   private static final String IGNORED_CACHES_KEY = "ignored-caches";

   private Cache<String, IgnoredCaches> cache;
   private final IgnoredCaches ignored = new IgnoredCaches();
   private final Set<ByteString> ignoredCachesByteString = ConcurrentHashMap.newKeySet();
   private final CacheListener listener = new CacheListener();
   private volatile boolean hasIgnores;
   private volatile boolean stopped;

   @Inject
   EmbeddedCacheManager cacheManager;

   CacheIgnoreManager() {
   }

   @Start
   public void start() {
      cache = cacheManager.getCache(SERVER_STATE_CACHE);
      updateLocalCopy(cache.get(IGNORED_CACHES_KEY));
      cache.addListener(listener);
   }

   public CompletableFuture<Void> unignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      synchronized (this) {
         ignored.caches.remove(cacheName);
         ignoredCachesByteString.remove(ByteString.fromString(cacheName));
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   public CompletableFuture<Void> ignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      synchronized (this) {
         ignored.caches.add(cacheName);
         ignoredCachesByteString.add(ByteString.fromString(cacheName));
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   public Set<String> getIgnoredCaches() {
      return Collections.unmodifiableSet(ignored.caches);
   }

   public boolean isCacheIgnored(ByteString cacheName) {
      return hasIgnores && ignoredCachesByteString.contains(cacheName);
   }

   public boolean isCacheIgnored(String cacheName) {
      return hasIgnores && ignored.caches.contains(cacheName);
   }

   private void updateLocalCopy(IgnoredCaches ignored) {
      if (ignored != null) {
         synchronized (this) {
            // Readers do not synchronize, so they should never see an empty map
            this.ignored.caches.addAll(ignored.caches);
            this.ignored.caches.retainAll(ignored.caches);

            for (String c : ignored.caches) {
               this.ignoredCachesByteString.add(ByteString.fromString(c));
            }
            ignoredCachesByteString.removeIf(c -> !ignored.caches.contains(c.toString()));

            hasIgnores = !this.ignored.caches.isEmpty();
         }
      }
   }

   @Stop
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
   static final class IgnoredCaches {

      @ProtoField(number = 1, collectionImplementation = HashSet.class)
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
   }
}
