package org.infinispan.rest.cachemanager;

import java.util.Map;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.dataconversion.GlobalMarshallerEncoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.util.CollectionFactory;
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
import org.infinispan.rest.cachemanager.exceptions.CacheNotFoundException;
import org.infinispan.rest.cachemanager.exceptions.CacheUnavailableException;
import org.infinispan.upgrade.RollingUpgradeManager;

public class RestCacheManager<V> {
   private final EmbeddedCacheManager instance;
   private final Predicate<? super String> isCacheIgnored;
   private final boolean allowInternalCacheAccess;
   private Map<String, AdvancedCache<String, V>> knownCaches = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   public RestCacheManager(EmbeddedCacheManager instance) {
      this(instance, s -> Boolean.FALSE);
   }

   public RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.allowInternalCacheAccess = instance.getCacheManagerConfiguration().security().authorization().enabled();
   }


   public AdvancedCache<String, V> getCache(String name) {
      if (isCacheIgnored.test(name)) {
         throw new CacheUnavailableException("Cache with name '" + name + "' is temporarily unavailable.");
      }
      boolean isKnownCache = knownCaches.containsKey(name);
      if (!BasicCacheContainer.DEFAULT_CACHE_NAME.equals(name) && !isKnownCache && !instance.getCacheNames().contains(name))
         throw new CacheNotFoundException("Cache with name '" + name + "' not found amongst the configured caches");

      if (isKnownCache) {
         return knownCaches.get(name);
      } else {
         InternalCacheRegistry icr = instance.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
         if (icr.isPrivateCache(name)) {
            throw new CacheUnavailableException(String.format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", name));
         } else if (!allowInternalCacheAccess && icr.isInternalCache(name)) {
            throw new CacheUnavailableException(String.format("Remote requests are not allowed to internal caches when authorization is disabled. Do no send remote requests to cache '%s'", name));
         }
         Cache<String, V> cache = name.equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ? instance.getCache() : instance.getCache(name);
         tryRegisterMigrationManager(cache);
         AdvancedCache<String, V> restCache = cache.getAdvancedCache();
         if(cache.getCacheConfiguration().compatibility().enabled()) {
            restCache = (AdvancedCache<String, V>) restCache.withEncoding(IdentityEncoder.class);
         }
         if(cache.getCacheConfiguration().memory().storageType() == StorageType.OFF_HEAP) {
            restCache = (AdvancedCache<String, V>) restCache.withEncoding(GlobalMarshallerEncoder.class);
         }
         knownCaches.put(name, restCache);
         return restCache;
      }
   }

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key) {
      return getInternalEntry(cacheName, key, false);
   }

   public CacheEntry<String, V> getInternalEntry(String cacheName, String key, boolean skipListener) {
      AdvancedCache<String, V> cache =
            skipListener ? getCache(cacheName).withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : getCache(cacheName);

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

   public void tryRegisterMigrationManager(Cache<String, V> cache) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new RestSourceMigrator(cache));
   }
}
