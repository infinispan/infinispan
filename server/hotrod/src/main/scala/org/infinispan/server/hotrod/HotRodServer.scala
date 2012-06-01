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
import scala.collection.JavaConversions._
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.{CacheValue, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{TypedProperties, ByteArrayKey}
import org.infinispan.server.core.Main._
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig
import org.infinispan.Cache
import org.infinispan.remoting.transport.Address
import org.infinispan.util.concurrent.ConcurrentMapFactory
import annotation.tailrec
import org.infinispan.context.{InvocationContext, Flag}
import org.infinispan.commands.write.PutKeyValueCommand
import org.infinispan.interceptors.base.BaseCustomInterceptor
import org.infinispan.interceptors.EntryWrappingInterceptor
import org.infinispan.config.{CustomInterceptorConfig, CacheLoaderManagerConfig, Configuration}
import java.util.{Collections, Properties}

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
   private var viewId: Int = DEFAULT_VIEW_ID
   private val knownCaches : java.util.Map[String, Cache[ByteArrayKey, CacheValue]] = ConcurrentMapFactory.makeConcurrentMap(4, 0.9f, 16)
   private val isTrace = isTraceEnabled

   def getAddress: ServerAddress = address

   def getViewId: Int = viewId

   def setViewId(viewId: Int) {
      trace("Set view id to %d", viewId)
      this.viewId = viewId
   }

   override def getEncoder = new HotRodEncoder(getCacheManager, this)

   override def getDecoder : HotRodDecoder = {
      val hotRodDecoder = new HotRodDecoder(getCacheManager, transport, this)
      hotRodDecoder.versionGenerator = this.versionGenerator
      hotRodDecoder
   }

   override def start(p: Properties, cacheManager: EmbeddedCacheManager) {
      val properties = if (p == null) new Properties else p
      val defaultPort = 11222

      // 1. Start default cache and the endpoint before adding self to
      // topology in order to avoid topology updates being used before
      // endpoint is available.
      super.start(properties, cacheManager, defaultPort)

      isClustered = cacheManager.getCacheManagerConfiguration.transport().transport() != null
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
   }

   override def startTransport(idleTimeout: Int, tcpNoDelay: Boolean,
         sendBufSize: Int, recvBufSize: Int, typedProps: TypedProperties) {
      // Start predefined caches
      preStartCaches()

      super.startTransport(idleTimeout, tcpNoDelay, sendBufSize, recvBufSize, typedProps)
   }

   override def startDefaultCache = {
      cacheManager.getCache()
   }

   private def preStartCaches() {
      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         if (cacheName != HotRodServer.ADDRESS_CACHE_NAME) {
            cacheManager.getCache(cacheName)
         }
      }
   }

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: EmbeddedCacheManager) {
      addressCache = cacheManager.getCache(HotRodServer.ADDRESS_CACHE_NAME)
      clusterAddress = cacheManager.getAddress
      address = new ServerAddress(host, port)
      cacheManager.addListener(new CrashedMemberDetectorListener(addressCache, this))
      // Map cluster address to server endpoint address
      debug("Map %s cluster address with %s server endpoint in address cache", clusterAddress, address)
      addressCache.getAdvancedCache.withFlags(Flag.SKIP_CACHE_LOAD)
              .put(clusterAddress, address)
   }

   private def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager, typedProps: TypedProperties) {
      cacheManager.defineConfiguration(HotRodServer.ADDRESS_CACHE_NAME,
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
      // Set custom interceptor to update view for HotRod clients
      val cic = new CustomInterceptorConfig(new ViewIdUpdater(), false, false,
            -1, null, classOf[EntryWrappingInterceptor].getName)
      topologyCacheConfig.setCustomInterceptors(Collections.singletonList(cic))

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

   def isCacheNameKnown(cacheName: String) = {
      cacheName != null && !cacheName.isEmpty && !(knownCaches containsKey cacheName)
   }

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager, skipCacheCheck: Boolean): Cache[ByteArrayKey, CacheValue] = {
      var cache: Cache[ByteArrayKey, CacheValue] = null 
      if (!skipCacheCheck) cache = knownCaches.get(cacheName)

      if (cache == null) {
         if (cacheName.isEmpty) 
            cache = cacheManager.getCache[ByteArrayKey, CacheValue]
         else 
            cache = cacheManager.getCache(cacheName)

         knownCaches.put(cacheName, cache)
      }

      cache
   }

   private[hotrod] def getAddressCache = addressCache

   /**
    * Cache interceptor that provides guarantees for view id updates. So, a
    * view id will only be considered to have changed once the address cache
    * has been updated to add an address from the cache. That way, when the
    * encoder makes the view id comparison (client provided vs server side
    * view id), it has the guarantees that the address cache has already been
    * updated.
    *
    * The choice of using interceptor (rather than a cache listener) is
    * intentional because the consistency it provides stronger guarantees on
    * the visibility of cache state than a cache listener.
    */
   private class ViewIdUpdater extends BaseCustomInterceptor {

      override def visitPutKeyValueCommand(ctx: InvocationContext,
              cmd: PutKeyValueCommand): AnyRef = {
         // First, let the put execute
         val ret = super.visitPutKeyValueCommand(ctx, cmd)
         // The key set contains the new address too
         val cachedAddresses = cache.keySet()
         val transport = embeddedCacheManager.getTransport
         val clusterMembers = transport.getMembers
         if (isCacheAndViewSynched(clusterMembers.iterator(), cachedAddresses)) {
            // Once the address cache has all the nodes in the view
            val localViewId = transport.getViewId
            setViewId(localViewId)
            if (isTrace) {
               trace("Address cache added %s for cluster address %s. View id is now %d",
                  cmd.getKey, cmd.getValue, localViewId)
            }
         } else if (isTrace) {
            tracef("View [%s] (id=%d) is not yet fully present in address" +
                    " cache [%s], with [%s] new joining node.",
               clusterMembers, transport.getViewId, cachedAddresses, cmd.getKey)
         }
         ret
      }

      /**
       * Check whether the address cache and the JGroups view are synced.
       * They are considered to be synced when all the addresses in the
       * JGroups view are present in the address cache.
       *
       * This check is necessary to make sure that the view id is only
       * updated once the cache contains all the members in the cluster,
       * otherwise partial views could be returned leading to uneven request
       * balancing.
       */
      @tailrec
      private def isCacheAndViewSynched(clusterIt: java.util.Iterator[Address],
              addrs: java.util.Set[_]): Boolean = {
         if (!clusterIt.hasNext) true
         else {
            val clusterAddr = clusterIt.next()
            if (!addrs.contains(clusterAddr))
               false
            else
               isCacheAndViewSynched(clusterIt, addrs)
         }
      }

   }

}

object HotRodServer {

   val ADDRESS_CACHE_NAME = "___hotRodTopologyCache"
   val DEFAULT_VIEW_ID = -1

}
