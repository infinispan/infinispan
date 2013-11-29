package org.infinispan.server.hotrod

import logging.Log
import scala.collection.JavaConversions._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.{QueryFacade, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.commons.util.CollectionFactory
import org.infinispan.commons.equivalence.AnyEquivalence
import org.infinispan.remoting.transport.Address
import org.infinispan.configuration.cache.{Configuration, CacheMode, ConfigurationBuilder}
import org.infinispan.context.Flag
import org.infinispan.upgrade.RollingUpgradeManager
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration
import java.util.ServiceLoader
import org.infinispan.util.concurrent.IsolationLevel

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information
 * on startup and shutdown.
 *
 * TODO: It's too late for 5.1.1 series. In 5.2, split class into: local and cluster hot rod servers
 * This should safe some memory for the local case and the code should be cleaner
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodServer extends AbstractProtocolServer("HotRod") with Log {

   type SuitableConfiguration = HotRodServerConfiguration

   // Type aliases to reduce boilerplate definitions
   type Bytes = Array[Byte]
   type Cache = org.infinispan.Cache[Bytes, Bytes]
   type AddressCache = org.infinispan.Cache[Address, ServerAddress]

   private var isClustered: Boolean = _
   private var clusterAddress: Address = _
   private var address: ServerAddress = _
   private var addressCache: AddressCache = _
   private val knownCaches : java.util.Map[String, Cache] = CollectionFactory.makeConcurrentMap(4, 0.9f, 16)
   private var queryFacades: Seq[QueryFacade] = _

   def getAddress: ServerAddress = address

   def getQueryFacades: Seq[QueryFacade] = queryFacades

   override def getEncoder = new HotRodEncoder(getCacheManager, this)

   override def getDecoder : HotRodDecoder =
      new HotRodDecoder(getCacheManager, transport, this)

   override def startInternal(configuration: HotRodServerConfiguration, cacheManager: EmbeddedCacheManager) {
      this.configuration = configuration

      // 1. Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.startInternal(configuration, cacheManager)

      isClustered = cacheManager.getCacheManagerConfiguration.transport().transport() != null
      if (isClustered) {
         defineTopologyCacheConfig(cacheManager)
         if (isDebugEnabled)
            debug("Externally facing address is %s:%d", configuration.proxyHost, configuration.proxyPort)

         addSelfToTopologyView(cacheManager)
      }

      queryFacades = loadQueryFacades()
   }

   private def loadQueryFacades(): Seq[QueryFacade] =
      ServiceLoader.load(classOf[QueryFacade], getClass().getClassLoader()).toSeq

   override def startTransport() {
      // Start predefined caches
      preStartCaches()

      super.startTransport()
   }

   override def startDefaultCache = {
      val cache = cacheManager.getCache[AnyRef, AnyRef](configuration.defaultCacheName())
      validateCacheConfiguration(cache.getCacheConfiguration)
      cache
   }

   private def preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         if (!cacheName.startsWith(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX)) {
            val cache = cacheManager.getCache(cacheName)
            validateCacheConfiguration(cache.getCacheConfiguration)
         }
      }
   }

   private def validateCacheConfiguration(cacheCfg: Configuration) {
      val isolationLevel = cacheCfg.locking().isolationLevel()
      if (isolationLevel == IsolationLevel.REPEATABLE_READ
              || isolationLevel == IsolationLevel.SERIALIZABLE)
         throw log.invalidIsolationLevel(isolationLevel)
   }

   private def addSelfToTopologyView(cacheManager: EmbeddedCacheManager) {
      addressCache = cacheManager.getCache(configuration.topologyCacheName)
      clusterAddress = cacheManager.getAddress
      address = new ServerAddress(configuration.proxyHost, configuration.proxyPort)
      cacheManager.addListener(new CrashedMemberDetectorListener(addressCache, this))
      // Map cluster address to server endpoint address
      debug("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address)
      // Guaranteed delivery required since if data is lost, there won't be
      // any further cache calls, so negative acknowledgment can cause issues.
      addressCache.getAdvancedCache
              .withFlags(Flag.SKIP_CACHE_LOAD, Flag.GUARANTEED_DELIVERY)
              .put(clusterAddress, address)
   }

   private def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager) {
      val topoCfg = cacheManager.getCacheConfiguration(configuration.topologyCacheName)
      if (topoCfg != null) {
         throw log.invalidTopologyCache(configuration.topologyCacheName)
      }
      cacheManager.defineConfiguration(configuration.topologyCacheName,
         createTopologyCacheConfig(cacheManager.getCacheManagerConfiguration.transport().distributedSyncTimeout()).build())
   }

   protected def createTopologyCacheConfig(distSyncTimeout: Long): ConfigurationBuilder = {
      val builder = new ConfigurationBuilder
      builder.clustering().cacheMode(CacheMode.REPL_SYNC).sync().replTimeout(configuration.topologyReplTimeout)
             .locking().lockAcquisitionTimeout(configuration.topologyLockTimeout)
             .eviction().strategy(EvictionStrategy.NONE)
             .expiration().lifespan(-1).maxIdle(-1)
             // Topology cache uses Object based equals/hashCodes
             .dataContainer()
                .keyEquivalence(AnyEquivalence.getInstance())
                .valueEquivalence(AnyEquivalence.getInstance())

      if (configuration.topologyStateTransfer) {
         builder
            .clustering()
               .stateTransfer()
                  .awaitInitialTransfer(configuration.topologyAwaitInitialTransfer)
                  .fetchInMemoryState(true)
                  .timeout(distSyncTimeout + configuration.topologyReplTimeout)
      } else {
         builder.persistence().addClusterLoader().remoteCallTimeout(configuration.topologyReplTimeout)
      }

      builder
   }

   def isCacheNameKnown(cacheName: String) = {
      cacheName != null && !cacheName.isEmpty && !(knownCaches containsKey cacheName)
   }

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager, skipCacheCheck: Boolean): Cache = {
      var cache: Cache = null
      if (!skipCacheCheck) cache = knownCaches.get(cacheName)

      if (cache == null) {
         val validCacheName = if (cacheName.isEmpty) configuration.defaultCacheName else cacheName
         val tmpCache = cacheManager.getCache[Bytes, Bytes](validCacheName)
         val compatibility = tmpCache.getCacheConfiguration.compatibility().enabled()
         val indexing = tmpCache.getCacheConfiguration.indexing().enabled()

         // Use flag when compatibility is enabled, otherwise it's unnecessary
         if (compatibility || indexing)
            cache = tmpCache.getAdvancedCache.withFlags(Flag.OPERATION_HOTROD)
         else
            cache = tmpCache

         knownCaches.put(cacheName, cache)
         // make sure we register a Migrator for this cache!
         tryRegisterMigrationManager(cacheName, cache)
      }

      cache
   }

   def tryRegisterMigrationManager(cacheName: String, cache: Cache) {
      val cr = cache.getAdvancedCache.getComponentRegistry
      val migrationManager = cr.getComponent(classOf[RollingUpgradeManager])
      if (migrationManager != null) migrationManager.addSourceMigrator(new HotRodSourceMigrator(cache))
   }

   private[hotrod] def getAddressCache = addressCache

}

object HotRodServer {
   val DEFAULT_TOPOLOGY_ID = -1
}
