package org.infinispan.rest.cachemanager;

import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.rest.logging.Log;
import org.infinispan.upgrade.RollingUpgradeManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Manages caches instances used during rest requests.
 */
public class RestCacheManager<V> {

   protected final static Log logger = LogFactory.getLog(RestCacheManager.class, Log.class);

   private final EmbeddedCacheManager instance;
   private final InternalCacheRegistry icr;
   private final Predicate<? super String> isCacheIgnored;
   private final boolean allowInternalCacheAccess;
   private final Map<String, AdvancedCache<Object, V>> knownCaches = new ConcurrentHashMap<>(4, 0.9f, 16);
   private final RemoveCacheListener removeCacheListener;

   public RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.icr = instance.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      this.allowInternalCacheAccess = instance.getCacheManagerConfiguration().security().authorization().enabled();
      removeCacheListener = new RemoveCacheListener();
      SecurityActions.addListener(instance, removeCacheListener);
   }

   @SuppressWarnings("unchecked")
   public AdvancedCache<Object, V> getCache(String name, MediaType keyContentType, MediaType valueContentType) {
      if (isCacheIgnored.test(name)) {
         throw logger.cacheUnavailable(name);
      }
      if (keyContentType == null || valueContentType == null) {
         throw logger.missingRequiredMediaType(name);
      }
      checkCacheAvailable(name);
      String cacheKey = name + "-" + keyContentType.toString() + valueContentType.getTypeSubtype();
      AdvancedCache<Object, V> registered = knownCaches.get(cacheKey);
      if (registered != null) return registered;

      AdvancedCache<String, V> cache = instance.<String, V>getCache(name).getAdvancedCache();
      tryRegisterMigrationManager(cache);

      AdvancedCache<Object, V> encodedCache = (AdvancedCache<Object, V>) cache.getAdvancedCache()
            .withMediaType(keyContentType.toString(), valueContentType.toString());

      knownCaches.putIfAbsent(cacheKey, encodedCache);
      return encodedCache;
   }

   public AdvancedCache<Object, V> getCache(String name) {
      return getCache(name, MATCH_ALL, MATCH_ALL);
   }

   private void checkCacheAvailable(String cacheName) {
      if (!BasicCacheContainer.DEFAULT_CACHE_NAME.equals(cacheName) && !instance.getCacheNames().contains(cacheName))
         throw logger.cacheNotFound(cacheName);
      if (icr.isPrivateCache(cacheName)) {
         throw logger.requestNotAllowedToInternalCaches(cacheName);
      } else if (!allowInternalCacheAccess && icr.isInternalCache(cacheName) && !icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.USER)) {
         throw logger.requestNotAllowedToInternalCachesWithoutAuthz(cacheName);
      }
   }

   public CacheEntry<Object, V> getInternalEntry(String cacheName, Object key, MediaType keyContentType, MediaType mediaType) {
      return getInternalEntry(cacheName, key, false, keyContentType, mediaType);
   }

   public void remove(String cacheName, Object key, MediaType keyContentType, boolean async) {
      Cache<Object, V> cache = getCache(cacheName, keyContentType, MediaType.MATCH_ALL);
      if (async) {
         cache.removeAsync(key);
      } else {
         cache.remove(key);
      }

   }

   public MediaType getValueConfiguredFormat(String cacheName) {
      return getCache(cacheName).getCacheConfiguration().encoding().valueDataType().mediaType();
   }

   public CacheEntry<Object, V> getInternalEntry(String cacheName, Object key, boolean skipListener, MediaType keyContentType, MediaType mediaType) {
      AdvancedCache<Object, V> cache =
            skipListener ? getCache(cacheName, keyContentType, mediaType).withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : getCache(cacheName, keyContentType, mediaType);

      return cache.getCacheEntry(key);
   }

   public String getNodeName() {
      Address addressToBeReturned = instance.getAddress();
      if (addressToBeReturned == null) {
         return "0.0.0.0";
      }
      return addressToBeReturned.toString();
   }

   public String getServerAddress() {
      Transport transport = instance.getTransport();
      if (transport instanceof JGroupsTransport) {
         return transport.getPhysicalAddresses().toString();
      }
      return "0.0.0.0";
   }

   public String getPrimaryOwner(String cacheName, Object key) {
      DistributionManager dm = getCache(cacheName).getDistributionManager();
      if (dm == null) {
         //this is a local cache
         return "0.0.0.0";
      }
      return dm.getCacheTopology().getDistribution(key).primary().toString();
   }

   public EmbeddedCacheManager getInstance() {
      return instance;
   }

   @SuppressWarnings("unchecked")
   private void tryRegisterMigrationManager(AdvancedCache<?, ?> cache) {
      ComponentRegistry cr = cache.getComponentRegistry();
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new RestSourceMigrator(cache));
   }

   public void stop() {
      if (removeCacheListener != null) {
         SecurityActions.removeListener(instance, removeCacheListener);
      }
   }

   @Listener
   class RemoveCacheListener {
      @CacheStopped
      public void cacheStopped(CacheStoppedEvent event) {
         knownCaches.keySet().stream().filter(k -> k.startsWith(event.getCacheName() + "-")).forEach(knownCaches::remove);
      }
   }

}
