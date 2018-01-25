package org.infinispan.rest.cachemanager;

import static org.infinispan.commons.dataconversion.MediaType.MATCH_ALL;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.util.Map;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.UTF8CompatEncoder;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
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
   private final Map<String, AdvancedCache<String, V>> knownCaches =
         CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   public RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.icr = instance.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      this.allowInternalCacheAccess = instance.getCacheManagerConfiguration().security().authorization().enabled();
   }

   @SuppressWarnings("unchecked")
   public AdvancedCache<String, V> getCache(String name, MediaType requestedMediaType) {
      if (isCacheIgnored.test(name)) {
         throw logger.cacheUnavailable(name);
      }
      if (requestedMediaType == null) {
         throw logger.missingRequiredMediaType(name);
      }
      AdvancedCache<String, V> registered = knownCaches.get(name + requestedMediaType.getTypeSubtype());
      if (registered != null) return registered;

      checkCacheAvailable(name);
      AdvancedCache<String, V> cache = instance.<String, V>getCache(name).getAdvancedCache();
      tryRegisterMigrationManager(cache);

      if (name.equals(PROTOBUF_METADATA_CACHE_NAME)) {
         cache = (AdvancedCache<String, V>) cache.withEncoding(UTF8CompatEncoder.class);
      } else {
         Configuration cacheConfiguration = cache.getCacheConfiguration();
         if (cacheConfiguration.memory().storageType() == StorageType.OFF_HEAP) {
            cache = (AdvancedCache<String, V>) cache.withKeyEncoding(UTF8Encoder.class);
         }
         cache = (AdvancedCache<String, V>) cache.withMediaType(TEXT_PLAIN_TYPE, requestedMediaType.toString());
      }

      knownCaches.putIfAbsent(name + requestedMediaType.getTypeSubtype(), cache);
      return cache;
   }

   public AdvancedCache<String, V> getCache(String name) {
      return getCache(name, MATCH_ALL);
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

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key, MediaType mediaType) {
      return getInternalEntry(cacheName, key, false, mediaType);
   }

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key) {
      return getInternalEntry(cacheName, key, false, MATCH_ALL);
   }

   public MediaType getValueConfiguredFormat(String cacheName) {
      return getCache(cacheName).getCacheConfiguration().encoding().valueDataType().mediaType();
   }

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key, boolean skipListener, MediaType mediaType) {
      AdvancedCache<String, V> cache =
            skipListener ? getCache(cacheName, mediaType).withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : getCache(cacheName, mediaType);

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

   public String getPrimaryOwner(String cacheName, String key) {
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
}
