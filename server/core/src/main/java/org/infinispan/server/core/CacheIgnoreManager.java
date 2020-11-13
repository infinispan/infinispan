package org.infinispan.server.core;

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
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopeFilter;
import org.infinispan.globalstate.ScopedState;
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

/**
 * Manages the cache blacklisting for a given {@link EmbeddedCacheManager}
 *
 * @since 10.0
 */
@Scope(Scopes.GLOBAL)
public final class CacheIgnoreManager {
   private static final ScopedState IGNORED_CACHES_KEY = new ScopedState("ignored-caches", "ignored-caches");

   private Cache<ScopedState, Object> cache;
   private final IgnoredCaches ignored = new IgnoredCaches();
   private final CacheListener listener = new CacheListener();
   private volatile boolean hasIgnores;
   private volatile boolean stopped;

   @Inject
   EmbeddedCacheManager cacheManager;

   @Inject
   GlobalConfigurationManager configurationManager;

   CacheIgnoreManager() {
   }

   @Start
   public void start() {
      cache = configurationManager.getStateCache();
      updateLocalCopy((IgnoredCaches) cache.get(IGNORED_CACHES_KEY));
      cache.addListener(listener, new ScopeFilter(IGNORED_CACHES_KEY.getScope()), null);
   }

   public CompletableFuture<Void> unignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
      synchronized (this) {
         ignored.caches.remove(cacheName);
         hasIgnores = !ignored.caches.isEmpty();
         return cache.putAsync(IGNORED_CACHES_KEY, ignored).thenApply(r -> null);
      }
   }

   public CompletableFuture<Void> ignoreCache(String cacheName) {
      SecurityActions.checkPermission(cacheManager, AuthorizationPermission.ADMIN);
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

   @Listener(observation = Listener.Observation.POST)
   private final class CacheListener {
      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<String, IgnoredCaches> e) {
         if (!e.isOriginLocal()) {
            updateLocalCopy(e.getValue());
         }
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<String, IgnoredCaches> e) {
         if (!e.isOriginLocal()) {
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
