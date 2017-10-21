package org.infinispan.rest.cachemanager;

import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;

import java.util.Map;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.UTF8CompatEncoder;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.ContentTypeConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.rest.cachemanager.exceptions.CacheUnavailableException;
import org.infinispan.rest.operations.exceptions.NoCacheFoundException;
import org.infinispan.upgrade.RollingUpgradeManager;

public class RestCacheManager<V> {
   private final EmbeddedCacheManager instance;
   private final Predicate<? super String> isCacheIgnored;
   private final boolean allowInternalCacheAccess;
   private Map<String, AdvancedCache<String, V>> knownCaches = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   public RestCacheManager(EmbeddedCacheManager instance) {
      this(instance, s -> Boolean.FALSE);
   }

   private RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.allowInternalCacheAccess = instance.getCacheManagerConfiguration().security().authorization().enabled();
   }

   @SuppressWarnings("unchecked")
   public AdvancedCache<String, V> getCache(String name, MediaType mediaType) {
      String cacheKey = mediaType == null ? name : name + mediaType.getTypeSubtype();
      AdvancedCache<String, V> registered = knownCaches.get(cacheKey);

      if (registered != null) return registered;

      if (name.equals(PROTOBUF_METADATA_CACHE_NAME)) {
         return (AdvancedCache<String, V>) instance.getCache(PROTOBUF_METADATA_CACHE_NAME).getAdvancedCache()
               .withEncoding(UTF8CompatEncoder.class);
      }

      AdvancedCache<String, V> knownCache = getCache(name);

      AdvancedCache<?, ?> encodedCache = knownCache.getAdvancedCache();
      StorageType storageType = encodedCache.getCacheConfiguration().memory().storageType();
      if (mediaType != null) {
         encodedCache = encodedCache.withMediaType(MediaType.TEXT_PLAIN_TYPE, mediaType.toString());
      }
      if (storageType == StorageType.OFF_HEAP) {
         encodedCache = encodedCache.withKeyEncoding(UTF8Encoder.class);
      }
      AdvancedCache<String, V> decoratedCache = (AdvancedCache<String, V>) encodedCache;
      knownCaches.putIfAbsent(cacheKey, decoratedCache);
      return decoratedCache;
   }

   private AdvancedCache<String, V> getCache(String name) {
      if (isCacheIgnored.test(name)) {
         throw new CacheUnavailableException("Cache with name '" + name + "' is temporarily unavailable.");
      }
      boolean isKnownCache = knownCaches.containsKey(name);
      if (!BasicCacheContainer.DEFAULT_CACHE_NAME.equals(name) && !isKnownCache && !instance.getCacheNames().contains(name))
         throw new NoCacheFoundException("Cache with name '" + name + "' not found amongst the configured caches");

      if (isKnownCache) {
         return knownCaches.get(name);
      } else {
         InternalCacheRegistry icr = instance.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (icr.isPrivateCache(name)) {
            throw new CacheUnavailableException(String.format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", name));
         } else if (!allowInternalCacheAccess && icr.isInternalCache(name) && !icr.internalCacheHasFlag(name, InternalCacheRegistry.Flag.USER)) {
            throw new CacheUnavailableException(String.format("Remote requests are not allowed to internal caches when authorization is disabled. Do no send remote requests to cache '%s'", name));
         }
         Cache<String, V> cache = name.equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ? instance.getCache() : instance.getCache(name);
         tryRegisterMigrationManager(cache);
         return cache.getAdvancedCache();
      }
   }

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key, MediaType mediaType) {
      return getInternalEntry(cacheName, key, false, mediaType);
   }

   public MediaType getConfiguredMediaType(String cacheName) {
      ContentTypeConfiguration valueMediaType = getCache(cacheName).getCacheConfiguration().encoding().valueDataType();
      if (!valueMediaType.isMediaTypeChanged()) {
         return null;
      }
      return valueMediaType.mediaType();
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

   public EncoderRegistry encoderRegistry() {
      return getInstance().getGlobalComponentRegistry().getComponent(EncoderRegistry.class);
   }

   public String getPrimaryOwner(String cacheName, String key, MediaType contentType) {
      DistributionManager dm = getCache(cacheName, contentType).getDistributionManager();
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
   private void tryRegisterMigrationManager(Cache<String, V> cache) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new RestSourceMigrator(cache));
   }
}
