package org.infinispan.rest;

import java.util.Map;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.upgrade.RollingUpgradeManager;

class RestCacheManager {
   private final EmbeddedCacheManager instance;
   private final Predicate<? super String> isCacheIgnored;
   private final boolean allowInternalCacheAccess;
   private Map<String, AdvancedCache<String, byte[]>> knownCaches = CollectionFactory.makeConcurrentMap(4, 0.9f, 16);

   public RestCacheManager(EmbeddedCacheManager instance) {
      this(instance, s -> Boolean.FALSE);
   }

   public RestCacheManager(EmbeddedCacheManager instance, Predicate<? super String> isCacheIgnored) {
      this.instance = instance;
      this.isCacheIgnored = isCacheIgnored;
      this.allowInternalCacheAccess = instance.getCacheManagerConfiguration().security().authorization().enabled();
   }


   AdvancedCache<String, byte[]> getCache(String name) {
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
         Cache<String, byte[]> cache = name.equals(BasicCacheContainer.DEFAULT_CACHE_NAME) ? instance.getCache() :
               instance.getCache(name);
         tryRegisterMigrationManager(cache);
         knownCaches.put(name, cache.getAdvancedCache());
         return cache.getAdvancedCache();
      }
   }

   byte[] getEntry(String cacheName, String key) {
      return getCache(cacheName).get(key);
   }

   <V> CacheEntry<String, V> getInternalEntry(String cacheName, String key) {
      return getInternalEntry(cacheName, key, false);
   }

   <V> CacheEntry<String, V> getInternalEntry(String cacheName, String key, boolean skipListener) {
      AdvancedCache<String, byte[]> cache =
            skipListener ? getCache(cacheName).withFlags(Flag.SKIP_LISTENER_NOTIFICATION) : getCache(cacheName);

      return (CacheEntry<String, V>) cache.getCacheEntry(key);
   }

   public Address getNodeName() {
      return instance.getAddress();
   }


   String getServerAddress() {
      Transport transport = instance.getTransport();
      if (transport instanceof JGroupsTransport) {
         return transport.getPhysicalAddresses().toString();
      }
      return null;
   }

   String getPrimaryOwner(String cacheName, String key) {
      DistributionManager dm = getCache(cacheName).getDistributionManager();
      return dm.getPrimaryLocation(key).toString();
   }

   EmbeddedCacheManager getInstance() {
      return instance;
   }

   void tryRegisterMigrationManager(Cache<String, byte[]> cache) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      RollingUpgradeManager migrationManager = cr.getComponent(RollingUpgradeManager.class);
      if (migrationManager != null) migrationManager.addSourceMigrator(new RestSourceMigrator(cache));
   }
}
