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

import ch.ServerHashSeed
import logging.Log
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import org.infinispan.remoting.transport.Address
import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.infinispan.server.core.{CacheValue, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{TypedProperties, ByteArrayKey}
import org.infinispan.server.core.Main._
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig
import org.infinispan.config.{CacheLoaderManagerConfig, Configuration}
import org.infinispan.Cache

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information
 * on startup and shutdown.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodServer extends AbstractProtocolServer("HotRod") with Log {
   import HotRodServer._
   private var isClustered: Boolean = _
   private var clusterAddress: Address = _
   private var address: ServerAddress = _
   private var addressCache: Cache[Address, ServerAddress] = _
   private var topologyUpdateTimeout: Long = _

   def getAddress: ServerAddress = address

   override def getEncoder = new HotRodEncoder(getCacheManager)

   override def getDecoder : HotRodDecoder = {
      val hotRodDecoder = new HotRodDecoder(getCacheManager, transport)
      hotRodDecoder.versionGenerator = this.versionGenerator
      hotRodDecoder
   }

   override def start(p: Properties, cacheManager: EmbeddedCacheManager) {
      val properties = if (p == null) new Properties else p
      val defaultPort = 11222
      isClustered = cacheManager.getGlobalConfiguration.getTransportClass != null
      if (isClustered) {
         val typedProps = TypedProperties.toTypedProperties(properties)
         defineTopologyCacheConfig(cacheManager, typedProps)
         // Retrieve host and port early on to populate topology cache
         val externalHost = typedProps.getProperty(PROP_KEY_PROXY_HOST,
               typedProps.getProperty(PROP_KEY_HOST, HOST_DEFAULT, true))
         val externalPort = typedProps.getIntProperty(PROP_KEY_PROXY_PORT,
               typedProps.getIntProperty(PROP_KEY_PORT, defaultPort, true))
         if (isDebugEnabled)
            debug("Externally facing address is %s:%d", externalHost, externalPort)

         addSelfToTopologyView(externalHost, externalPort, cacheManager)
      }

      super.start(properties, cacheManager, defaultPort)
   }

   override def startTransport(idleTimeout: Int, tcpNoDelay: Boolean,
         sendBufSize: Int, recvBufSize: Int, typedProps: TypedProperties) {
      // Start predefined caches
      preStartCaches

      super.startTransport(idleTimeout, tcpNoDelay, sendBufSize, recvBufSize, typedProps)
   }

   override def startDefaultCache = {
      injectServerHashSeed(cacheManager.getDefaultConfiguration)
      cacheManager.getCache()
   }

   private def preStartCaches {
      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         if (cacheName != ADDRESS_CACHE_NAME) {
            cacheManager.defineConfiguration(cacheName, injectServerHashSeed(new Configuration))
            cacheManager.getCache(cacheName)
         }
      }
   }

   private def injectServerHashSeed(cfg: Configuration): Configuration =
      cfg.fluent().clustering().hash().hashSeed(new ServerHashSeed(addressCache)).build()

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: EmbeddedCacheManager) {
      addressCache = cacheManager.getCache(ADDRESS_CACHE_NAME)
      clusterAddress = cacheManager.getAddress
      address = new ServerAddress(host, port)
      cacheManager.addListener(new CrashedMemberDetectorListener)
      // Map cluster address to server endpoint address
      debug("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address)
      addressCache.put(clusterAddress, address)
   }

   private def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager, typedProps: TypedProperties) {
      cacheManager.defineConfiguration(ADDRESS_CACHE_NAME,
         createTopologyCacheConfig(typedProps,
            cacheManager.getGlobalConfiguration.getDistributedSyncTimeout))
   }

   protected def createTopologyCacheConfig(typedProps: TypedProperties, distSyncTimeout: Long): Configuration = {
      val lockTimeout = typedProps.getLongProperty(PROP_KEY_TOPOLOGY_LOCK_TIMEOUT, TOPO_LOCK_TIMEOUT_DEFAULT, true)
      val replTimeout = typedProps.getLongProperty(PROP_KEY_TOPOLOGY_REPL_TIMEOUT, TOPO_REPL_TIMEOUT_DEFAULT, true)
      val doStateTransfer = typedProps.getBooleanProperty(PROP_KEY_TOPOLOGY_STATE_TRANSFER, TOPO_STATE_TRANSFER_DEFAULT, true)
      topologyUpdateTimeout = typedProps.getLongProperty(PROP_KEY_TOPOLOGY_UPDATE_TIMEOUT, TOPO_UPDATE_TIMEOUT_DEFAULT, true)

      val topologyCacheConfig = new Configuration
      topologyCacheConfig.setCacheMode(CacheMode.REPL_SYNC)
      topologyCacheConfig.setSyncReplTimeout(replTimeout) // Milliseconds
      topologyCacheConfig.setLockAcquisitionTimeout(lockTimeout) // Milliseconds
      topologyCacheConfig.setEvictionStrategy(EvictionStrategy.NONE); // No eviction
      topologyCacheConfig.setExpirationLifespan(-1); // No maximum lifespan
      topologyCacheConfig.setExpirationMaxIdle(-1); // No maximum idle time
      if (doStateTransfer) {
         topologyCacheConfig.setFetchInMemoryState(true) // State transfer required
         // State transfer timeout should be bigger than the distributed lock timeout
         topologyCacheConfig.setStateRetrievalTimeout(distSyncTimeout + replTimeout)
      } else {
         // Otherwise configure a cluster cache loader
         val loaderConfigs = new CacheLoaderManagerConfig
         val clusterLoaderConfig = new ClusterCacheLoaderConfig
         clusterLoaderConfig.setRemoteCallTimeout(replTimeout)
         loaderConfigs.addCacheLoaderConfig(clusterLoaderConfig)
         topologyCacheConfig.setCacheLoaderManagerConfig(loaderConfigs)
      }
      topologyCacheConfig
   }

   private[hotrod] def getAddressCache = addressCache

   @Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
   class CrashedMemberDetectorListener {

      @ViewChanged
      def handleViewChange(e: ViewChangedEvent) {
         val cacheManager = e.getCacheManager
         // Only the coordinator can potentially make modifications related to crashed members.
         // This is to avoid all nodes trying to make the same modification which would be wasteful and lead to deadlocks.
         if (cacheManager.isCoordinator) {
            trace("View change received on coordinator: %s", e)
            try {
               val newMembers = asScalaIterator(e.getNewMembers.iterator())
               val oldMembers = asScalaIterator(e.getOldMembers.iterator())
               val goneMembers = oldMembers.filterNot(newMembers contains)
               if (goneMembers.hasNext) {
                  trace("Somone left the cluster, oldMembers=%s newMembers=%s", oldMembers, newMembers)
                  goneMembers.foreach { addr =>
                     trace("Remove %s from address cache", addr)
                     addressCache.remove(addr)
                  }
               }
            } catch {
               case t: Throwable => logErrorDetectingCrashedMember(t)
            }
         }
      }

   }

}

object HotRodServer {
   val ADDRESS_CACHE_NAME = "___hotRodTopologyCache"

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager): Cache[ByteArrayKey, CacheValue] = {
      if (cacheName.isEmpty) cacheManager.getCache[ByteArrayKey, CacheValue]
      else cacheManager.getCache(cacheName)
   }   
}

