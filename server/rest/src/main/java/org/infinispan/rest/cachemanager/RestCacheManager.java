package org.infinispan.rest.cachemanager;

import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;

import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.logging.Log;
import org.infinispan.security.Security;
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
      this.icr = SecurityActions.getGlobalComponentRegistry(instance).getComponent(InternalCacheRegistry.class);
      this.allowInternalCacheAccess = SecurityActions.getCacheManagerConfiguration(instance)
            .security().authorization().enabled();
      removeCacheListener = new RemoveCacheListener();
      SecurityActions.addListener(instance, removeCacheListener);
   }

   @SuppressWarnings("unchecked")
   public AdvancedCache<Object, V> getCache(String name, MediaType keyContentType, MediaType valueContentType, RestRequest request) {
      Subject subject = request.getSubject();
      Flag[] flags = request.getFlags();
      if (isCacheIgnored.test(name)) {
         throw logger.cacheUnavailable(name);
      }
      if (keyContentType == null || valueContentType == null) {
         throw logger.missingRequiredMediaType(name);
      }
      checkCacheAvailable(name);
      String cacheKey = name + "-" + keyContentType.toString() + valueContentType.getTypeSubtype();
      AdvancedCache<Object, V> cache = knownCaches.get(cacheKey);
      if (cache == null) {
         cache = instance.<Object, V>getCache(name).getAdvancedCache();
         tryRegisterMigrationManager(cache);

         cache = (AdvancedCache<Object, V>) cache.getAdvancedCache()
               .withMediaType(keyContentType.toString(), valueContentType.toString())
               .withFlags(IGNORE_RETURN_VALUES);

         knownCaches.putIfAbsent(cacheKey, cache);
      }
      if (flags != null && flags.length > 0) cache = cache.withFlags(flags);
      return subject == null ? cache : cache.withSubject(subject);
   }

   public AdvancedCache<Object, V> getCache(String name, RestRequest restRequest) {
      return getCache(name, MATCH_ALL, MATCH_ALL, restRequest);
   }

   private void checkCacheAvailable(String cacheName) {
      if (!instance.getCacheNames().contains(cacheName))
         throw logger.cacheNotFound(cacheName);
      if (icr.isPrivateCache(cacheName)) {
         throw logger.requestNotAllowedToInternalCaches(cacheName);
      } else if (!allowInternalCacheAccess && icr.isInternalCache(cacheName) && !icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.USER)) {
         throw logger.requestNotAllowedToInternalCachesWithoutAuthz(cacheName);
      }
   }

   public Collection<String> getCacheNames() {
      return instance.getCacheNames();
   }

   public CompletionStage<CacheEntry<Object, V>> getInternalEntry(String cacheName, Object key, MediaType keyContentType, MediaType mediaType, RestRequest request) {
      return getInternalEntry(cacheName, key, false, keyContentType, mediaType, request);
   }

   public CompletionStage<V> remove(String cacheName, Object key, MediaType keyContentType, RestRequest restRequest) {
      Cache<Object, V> cache = getCache(cacheName, keyContentType, MediaType.MATCH_ALL, restRequest);
      return cache.removeAsync(key);
   }

   public CompletionStage<CacheEntry<Object, V>> getPrivilegedInternalEntry(AdvancedCache<Object, V> cache, Object key, boolean skipListener) {
      AdvancedCache<Object, V> cacheSkip = skipListener ? cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : cache;
      return SecurityActions.getCacheEntryAsync(cacheSkip, key);
   }

   private CompletionStage<CacheEntry<Object, V>> getInternalEntry(AdvancedCache<Object, V> cache, Object key, boolean skipListener) {
      return skipListener ? cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntryAsync(key) : cache.getCacheEntryAsync(key);
   }

   public MediaType getValueConfiguredFormat(String cacheName, RestRequest restRequest) {
      return SecurityActions.getCacheConfiguration(getCache(cacheName, restRequest)).encoding().valueDataType().mediaType();
   }

   private CompletionStage<CacheEntry<Object, V>> getInternalEntry(String cacheName, Object key, boolean skipListener, MediaType keyContentType, MediaType mediaType, RestRequest restRequest) {
      return getInternalEntry(getCache(cacheName, keyContentType, mediaType, restRequest), key, skipListener);
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

   public String getPrimaryOwner(String cacheName, Object key, RestRequest restRequest) {
      DistributionManager dm = SecurityActions.getDistributionManager(getCache(cacheName, restRequest));
      if (dm == null) {
         //this is a local cache
         return "0.0.0.0";
      }
      return dm.getCacheTopology().getDistribution(key).primary().toString();
   }

   public String getBackupOwners(String cacheName, Object key, RestRequest restRequest) {
      DistributionManager dm = SecurityActions.getDistributionManager(getCache(cacheName, restRequest));
      if (dm == null) {
         //this is a local cache
         return "0.0.0.0";
      }
      return dm.getCacheTopology().getDistribution(key).writeBackups().stream().map(a -> a.toString()).collect(Collectors.joining(" "));
   }

   public EmbeddedCacheManager getInstance() {
      return instance;
   }

   public EmbeddedCacheManagerAdmin getCacheManagerAdmin(RestRequest restRequest) {
      Subject subject = restRequest.getSubject();
      if (subject == null) {
         return instance.administration();
      } else {
         return Security.doAs(subject, (PrivilegedAction<EmbeddedCacheManagerAdmin>) () -> instance.administration().withSubject(subject));
      }
   }

   @SuppressWarnings("unchecked")
   private void tryRegisterMigrationManager(AdvancedCache<?, ?> cache) {
      ComponentRegistry cr = SecurityActions.getCacheComponentRegistry(cache);
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
