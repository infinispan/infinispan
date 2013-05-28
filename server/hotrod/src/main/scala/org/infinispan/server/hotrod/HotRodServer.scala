/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.server.hotrod

import logging.Log
import scala.collection.JavaConversions._
import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.infinispan.server.core.AbstractProtocolServer
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{CollectionFactory, AnyEquivalence}
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.context.Flag
import org.infinispan.upgrade.RollingUpgradeManager
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration

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

   import HotRodServer._

   type SuitableConfiguration = HotRodServerConfiguration

   private var isClustered: Boolean = _
   private var clusterAddress: Address = _
   private var address: ServerAddress = _
   private var addressCache: Cache[Address, ServerAddress] = _
   private val knownCaches : java.util.Map[String, Cache[Array[Byte], Array[Byte]]] =
         CollectionFactory.makeConcurrentMap(4, 0.9f, 16)

   def getAddress: ServerAddress = address

   override def getEncoder = new HotRodEncoder(getCacheManager, this)

   override def getDecoder : HotRodDecoder = {
      val hotRodDecoder = new HotRodDecoder(getCacheManager, transport, this)
      hotRodDecoder.versionGenerator = this.versionGenerator
      hotRodDecoder
   }

   override def start(configuration: HotRodServerConfiguration, cacheManager: EmbeddedCacheManager) {
      this.configuration = configuration

      // 1. Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.start(configuration, cacheManager)

      isClustered = cacheManager.getCacheManagerConfiguration.transport().transport() != null
      if (isClustered) {
         defineTopologyCacheConfig(cacheManager)
         if (isDebugEnabled)
            debug("Externally facing address is %s:%d", configuration.proxyHost, configuration.proxyPort)

         addSelfToTopologyView(cacheManager)
      }
   }

   override def startTransport() {
      // Start predefined caches
      preStartCaches()

      super.startTransport()
   }

   override def startDefaultCache = {
      cacheManager.getCache()
   }

   private def preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         if (!cacheName.startsWith(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX)) {
            cacheManager.getCache(cacheName)
         }
      }
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
         builder.clustering().stateTransfer().fetchInMemoryState(true)
                 .timeout(distSyncTimeout + configuration.topologyReplTimeout)
      } else {
         builder.loaders().addClusterCacheLoader().remoteCallTimeout(configuration.topologyReplTimeout)
      }

      builder
   }

   def isCacheNameKnown(cacheName: String) = {
      cacheName != null && !cacheName.isEmpty && !(knownCaches containsKey cacheName)
   }

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager, skipCacheCheck: Boolean): Cache[Array[Byte], Array[Byte]] = {
      var cache: Cache[Array[Byte], Array[Byte]] = null
      if (!skipCacheCheck) cache = knownCaches.get(cacheName)

      if (cache == null) {
         if (cacheName.isEmpty)
            cache = cacheManager.getCache[Array[Byte], Array[Byte]]()
                    .getAdvancedCache.withFlags(Flag.OPERATION_HOTROD)
         else
            cache = cacheManager.getCache[Array[Byte], Array[Byte]](cacheName)
                    .getAdvancedCache.withFlags(Flag.OPERATION_HOTROD)

         knownCaches.put(cacheName, cache)
         // make sure we register a Migrator for this cache!
         tryRegisterMigrationManager(cacheName, cache)
      }

      cache
   }

   def tryRegisterMigrationManager(cacheName: String, cache: Cache[Array[Byte], Array[Byte]]) {
      val cr = cache.getAdvancedCache.getComponentRegistry
      val migrationManager = cr.getComponent(classOf[RollingUpgradeManager])
      if (migrationManager != null) migrationManager.addSourceMigrator(new HotRodSourceMigrator(cache))
   }

   private[hotrod] def getAddressCache = addressCache

}

object HotRodServer {
   val DEFAULT_TOPOLOGY_ID = -1
}
