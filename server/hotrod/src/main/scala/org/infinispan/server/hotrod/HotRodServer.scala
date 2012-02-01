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
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.notifications.Listener
import scala.collection.JavaConversions._
import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.infinispan.server.core.{CacheValue, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{TypedProperties, ByteArrayKey}
import org.infinispan.server.core.Main._
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig
import org.infinispan.config.{CacheLoaderManagerConfig, Configuration}
import org.infinispan.Cache
import org.infinispan.notifications.cachelistener.annotation.{CacheEntryRemoved, CacheEntryCreated}
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent
import org.infinispan.remoting.transport.{Transport, Address}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information
 * on startup and shutdown.
 *
 * TODO: It's too late for 5.1.1 series. In 5.2, split class into: local and cluster hot rod servers
 * This should safe some memory for the local case and the code should be cleaner
 * TODO: In 5.2, convert the clustered hot rod server to new configuration too
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
   private var viewId: AtomicInteger = _

   def getAddress: ServerAddress = address

   override def getEncoder = new HotRodEncoder(getCacheManager, viewId)

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
         viewId = new AtomicInteger()

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
      cacheManager.getCache()
   }

   private def preStartCaches {
      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         if (cacheName != ADDRESS_CACHE_NAME) {
            cacheManager.getCache(cacheName)
         }
      }
   }

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: EmbeddedCacheManager) {
      addressCache = cacheManager.getCache(ADDRESS_CACHE_NAME)
      addressCache.addListener(new ViewIdUpdater(
            addressCache.getAdvancedCache.getRpcManager.getTransport))
      clusterAddress = cacheManager.getAddress
      address = new ServerAddress(host, port)
      cacheManager.addListener(new CrashedMemberDetectorListener(addressCache))
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
      topologyCacheConfig.setUseReplQueue(false) // Avoid getting mixed up with default config
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

   /**
    * Listener that provides guarantees for view id updates. So, a view id will
    * only be considered to have changed once the address cache has been
    * updated to add or remove an address from the cache. That way, when the
    * encoder makes the view id comparison (client provided vs server side
    * view id), it has the guarantees that the address cache has already been
    * updated.
    */
   @Listener
   class ViewIdUpdater(transport: Transport) {

      @CacheEntryCreated @CacheEntryRemoved
      def viewIdUpdate(event: CacheEntryEvent[Address, ServerAddress]) {
         // Only update view id once cache has been updated
         if (!event.isPre) {
            val localViewId = transport.getViewId
            // Could this be a lazySet? We want the value to eventually be set,
            // clients could wait a little bit for the view id to be updated
            // on the server side...
            viewId.set(localViewId)
            if (isTraceEnabled) {
               log.tracef("Address cache had %s for key %s. View id is now %d",
                          event.getType, event.getKey, localViewId)
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

