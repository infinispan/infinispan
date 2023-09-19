package org.infinispan.xsite;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Maps caches from remote site to local caches.
 *
 * @since 15.0
 */
@Scope(Scopes.GLOBAL)
@Listener
public class XSiteCacheMapper {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject GlobalComponentRegistry registry;
   @Inject ConfigurationManager configurationManager;
   @Inject CacheManagerNotifier notifier;

   private final Map<RemoteCacheInfo, LocalCacheInfoImpl> localCachesMap = new ConcurrentHashMap<>();

   public LocalCacheInfo findLocalCache(String remoteSite, ByteString remoteCacheName) {
      var key = new RemoteCacheInfo(remoteSite, remoteCacheName);
      return localCachesMap.computeIfAbsent(key, this::lookupLocalCaches);
   }

   // for testing only
   Optional<LocalCacheInfo> peekLocalCacheForRemoteSite(String remoteSite, ByteString remoteCache) {
      return Optional.ofNullable(localCachesMap.get(new RemoteCacheInfo(remoteSite, remoteCache)));
   }

   @Start
   public void start() {
      notifier.addListener(this);
   }

   @Stop
   public void stop() {
      notifier.removeListener(this);
      localCachesMap.clear();
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent event) {
      var cacheName = ByteString.fromString(event.getCacheName());
      log.debugf("On cache stopped event. Removing map to cache %s", cacheName);
      localCachesMap.entrySet().removeIf(entry -> entry.getValue().cacheName.equals(cacheName));
   }

   private LocalCacheInfoImpl lookupLocalCaches(RemoteCacheInfo remoteCache) {
      var optConf = getCacheNames().stream()
            .map(this::getConfiguration)
            .filter(Objects::nonNull)
            .filter(remoteCache::isBackupFor)
            .findFirst()
            .or(() -> findConfiguration(remoteCache.originCache.toString()))
            .map(NamedConfiguration::toLocalCacheInfo);
      if (optConf.isPresent()) {
         var local = optConf.get();
         log.debugf("Found local cache '%s' is backup for cache '%s' from site '%s'", local.cacheName, remoteCache.originCache,
               remoteCache.originSite);
         return optConf.get();
      }
      log.debugf("No local cache found for cache '%s' from site '%s'", remoteCache.originCache, remoteCache.originSite);
      return null;
   }

   private static boolean isLocal(Configuration configuration) {
      return !configuration.clustering().cacheMode().isClustered();
   }

   private Optional<NamedConfiguration> findConfiguration(String cacheName) {
      var c = configurationManager.getConfiguration(cacheName, false);
      return c == null ? Optional.empty() : Optional.of(new NamedConfiguration(cacheName, c));
   }

   private NamedConfiguration getConfiguration(String cacheName) {
      return findConfiguration(cacheName).orElse(null);
   }

   private Collection<String> getCacheNames() {
      return registry.getCacheManager().getCacheNames();
   }

   public interface LocalCacheInfo {
      ByteString cacheName();

      boolean isLocalOnly();
   }

   private static class LocalCacheInfoImpl implements LocalCacheInfo {
      private final ByteString cacheName;
      private final boolean local;

      LocalCacheInfoImpl(ByteString cacheName, boolean local) {
         this.cacheName = Objects.requireNonNull(cacheName);
         this.local = local;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         var that = (LocalCacheInfoImpl) o;
         return local == that.local && Objects.equals(cacheName, that.cacheName);
      }

      @Override
      public int hashCode() {
         return Objects.hash(cacheName, local);
      }

      @Override
      public ByteString cacheName() {
         return cacheName;
      }

      @Override
      public boolean isLocalOnly() {
         return local;
      }

      @Override
      public String toString() {
         return "LocalCacheInfoImpl{" +
               "cacheName=" + cacheName +
               ", local=" + local +
               '}';
      }
   }

   private static class RemoteCacheInfo {
      private final ByteString originSite;
      private final ByteString originCache;

      RemoteCacheInfo(String originSite, ByteString originCache) {
         this.originSite = XSiteNamedCache.cachedByteString(Objects.requireNonNull(originSite));
         this.originCache = Objects.requireNonNull(originCache);
      }

      boolean isBackupFor(NamedConfiguration namedConfiguration) {
         return namedConfiguration.configuration.sites().backupFor().isBackupFor(originSite.toString(), originCache.toString());
      }


      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }
         var remoteSiteCache = (RemoteCacheInfo) o;
         return Objects.equals(originSite, remoteSiteCache.originSite) &&
               Objects.equals(originCache, remoteSiteCache.originCache);
      }

      @Override
      public int hashCode() {
         return Objects.hash(originSite, originCache);
      }

      @Override
      public String toString() {
         return "RemoteCacheInfo{" +
               "originSite=" + originSite +
               ", originCache=" + originCache +
               '}';
      }
   }

   private static class NamedConfiguration {
      final ByteString name;
      final Configuration configuration;

      NamedConfiguration(String name, Configuration configuration) {
         this.name = ByteString.fromString(Objects.requireNonNull(name));
         this.configuration = Objects.requireNonNull(configuration);
      }

      LocalCacheInfoImpl toLocalCacheInfo() {
         return new LocalCacheInfoImpl(name, isLocal(configuration));
      }

      @Override
      public String toString() {
         return "NamedConfiguration{" +
               "name=" + name +
               ", configuration=" + configuration +
               '}';
      }
   }

}
