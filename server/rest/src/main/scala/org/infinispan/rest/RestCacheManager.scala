package org.infinispan.rest

import org.infinispan.server.hotrod.RestSourceMigrator
import org.infinispan.upgrade.RollingUpgradeManager
import org.infinispan.{AdvancedCache, Cache}
import org.infinispan.commons.api.BasicCacheContainer
import org.infinispan.commons.util.CollectionFactory
import org.infinispan.container.entries.{CacheEntry, InternalCacheEntry, MVCCEntry}
import org.infinispan.context.Flag
import org.infinispan.distribution.DistributionManager
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.registry.InternalCacheRegistry
import org.infinispan.remoting.transport.Address
import org.infinispan.remoting.transport.jgroups.JGroupsTransport

class RestCacheManager(instance: EmbeddedCacheManager, isCacheIgnored: String => Boolean = Function.const(false)) {

   private val allowInternalCacheAccess = instance.getCacheManagerConfiguration.security().authorization().enabled()

   private val knownCaches : java.util.Map[String, AdvancedCache[String, Array[Byte]]] =
      CollectionFactory.makeConcurrentMap(4, 0.9f, 16)

   def getCache(name: String): AdvancedCache[String, Array[Byte]] = {
      if(isCacheIgnored(name)) {
         throw new CacheUnavailableException("Cache with name '" + name + "' is temporarily unavailable.")
      }
      val isKnownCache = knownCaches.containsKey(name)
      if (name != BasicCacheContainer.DEFAULT_CACHE_NAME && !isKnownCache && !instance.getCacheNames.contains(name))
         throw new CacheNotFoundException("Cache with name '" + name + "' not found amongst the configured caches")

      if (isKnownCache) {
         knownCaches.get(name)
      } else {
         val icr = instance.getGlobalComponentRegistry.getComponent(classOf[InternalCacheRegistry])
         if (icr.isPrivateCache(name)) {
            throw new CacheUnavailableException("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'".format(name))
         } else if (!allowInternalCacheAccess && icr.isInternalCache(name)) {
            throw new CacheUnavailableException("Remote requests are not allowed to internal caches when authorization is disabled. Do no send remote requests to cache '%s'".format(name))
         }
         val cache =
            if (name == BasicCacheContainer.DEFAULT_CACHE_NAME)
               instance.getCache[String, Array[Byte]]()
            else
               instance.getCache[String, Array[Byte]](name)
         tryRegisterMigrationManager(cache)
         knownCaches.put(name, cache.getAdvancedCache)
         cache.getAdvancedCache
      }
   }

   def getEntry(cacheName: String, key: String): Array[Byte] = getCache(cacheName).get(key)

   def getInternalEntry[V](cacheName: String, key: String, skipListener: Boolean = false): CacheEntry[String, V] = {
      val cache =
         if (skipListener) getCache(cacheName).withFlags(Flag.SKIP_LISTENER_NOTIFICATION)
         else getCache(cacheName)

      cache.getCacheEntry(key) match {
         case ice: InternalCacheEntry[String, V] => ice
         case null => null
         case mvcc: MVCCEntry[String, V] => cache.getCacheEntry(key)  // FIXME: horrible re-get to be fixed by ISPN-3460
      }
   }

   def getNodeName: Address = instance.getAddress

   def getServerAddress: String =
      instance.getTransport match {
         case trns: JGroupsTransport => trns.getPhysicalAddresses.toString
         case null => null
      }

   def getPrimaryOwner(cacheName: String, key: String): String =
      getCache(cacheName).getDistributionManager match {
         case dm: DistributionManager => dm.getPrimaryLocation(key).toString
         case null => null
      }

   def getInstance = instance

   def tryRegisterMigrationManager(cache: Cache[String, Array[Byte]]) {
      val cr = cache.getAdvancedCache.getComponentRegistry
      val migrationManager = cr.getComponent(classOf[RollingUpgradeManager])
      if (migrationManager != null) migrationManager.addSourceMigrator(new RestSourceMigrator(cache))
   }

}
