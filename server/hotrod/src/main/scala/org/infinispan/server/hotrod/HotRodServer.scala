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
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import java.util.concurrent.TimeUnit._
import org.infinispan.{CacheException, Cache}
import org.infinispan.remoting.transport.Address
import org.infinispan.manager.EmbeddedCacheManager
import java.util.{Properties, Random}
import org.infinispan.server.core.{CacheValue, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{TypedProperties, ByteArrayKey, Util};
import org.infinispan.server.core.Main._
import org.infinispan.loaders.cluster.ClusterCacheLoaderConfig
import collection.mutable
import collection.immutable
import org.infinispan.util.concurrent.TimeoutException
import org.infinispan.remoting.transport.jgroups.SuspectException
import org.infinispan.context.Flag
import org.infinispan.config.{CacheLoaderManagerConfig, Configuration}

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
   private var address: TopologyAddress = _
   private var topologyCache: Cache[String, TopologyView] = _
   private val rand = new Random
   private var topologyUpdateTimeout: Long = _

   def getAddress: TopologyAddress = address

   override def getEncoder = new HotRodEncoder(getCacheManager)

   override def getDecoder : HotRodDecoder = {
      val hotRodDecoder: HotRodDecoder = new HotRodDecoder(getCacheManager, transport)
      hotRodDecoder.versionGenerator = this.versionGenerator
      hotRodDecoder
   }

   override def start(p: Properties, cacheManager: EmbeddedCacheManager) {
      val properties = if (p == null) new Properties else p
      isClustered = cacheManager.getGlobalConfiguration.getTransportClass != null
      if (isClustered)
         defineTopologyCacheConfig(cacheManager, TypedProperties.toTypedProperties(properties))
         
      super.start(properties, cacheManager, 11222)
   }

   override def startTransport(idleTimeout: Int, tcpNoDelay: Boolean,
         sendBufSize: Int, recvBufSize: Int, typedProps: TypedProperties) {
      // Start rest of the caches and self to view once we know for sure that we need to start
      // and we know that the rank calculator listener is registered

      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator))
         cacheManager.getCache(cacheName)

      // If clustered, set up a cache for topology information
      if (isClustered) {
         val externalHost = typedProps.getProperty(PROP_KEY_PROXY_HOST, getHost)
         val externalPort = typedProps.getIntProperty(PROP_KEY_PROXY_PORT, getPort)
         if (isDebugEnabled)
            debug("Externally facing address is %s:%d", externalHost, externalPort)

         addSelfToTopologyView(externalHost, externalPort, cacheManager)
      }

      super.startTransport(idleTimeout, tcpNoDelay, sendBufSize, recvBufSize, typedProps)
   }

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: EmbeddedCacheManager) {
      topologyCache = cacheManager.getCache(TopologyCacheName)
      address = createTopologyAddress(host, port)
      val isDebug = isDebugEnabled
      if (isDebug) debug("Local topology address is %s", address)
      cacheManager.addListener(new CrashedMemberDetectorListener)
      val updated = updateTopologyView(false, System.currentTimeMillis()) { view =>
         if (view != null) {
            val newMembers = view.members ::: List(address)
            val newView = TopologyView(view.topologyId + 1, newMembers)
            val updated = updateTopologyCacheEntry(view, newView) {
               (view, newView) => topologyCache.replace("view", view, newView)
            }
            if (isDebug && updated) debug("Added %s to topology, new view is %s", address, newView)
            updated
         } else {
            val newMembers = List(address)
            val newView = TopologyView(1, newMembers)
            val updated = updateTopologyCacheEntry(null, newView) {
               (view, newView) => topologyCache.putIfAbsent("view", newView) == null
            }
            if (isDebug && updated) debug("First member to start, topology view is %s", newView)
            updated
         }
      }
      if (!updated)
         throw new CacheException("Unable to update topology view, so aborting startup")
   }

   private def updateTopologyCacheEntry(view: TopologyView, newView: TopologyView)
                                       (cacheFct: (TopologyView, TopologyView) => Boolean): Boolean = {
      var updated = false
      try {
         updated = cacheFct(view, newView)
      } catch {
         case e: TimeoutException =>
            debug(e, "Timed out while trying to update new view [%s]", newView)
         case e: SuspectException =>
            debug(e, "Node suspected while trying to replicate new view [%s]", newView)
         case t: Throwable => throw t
      }
      updated
   }

   private def createTopologyAddress(host: String, port: Int): TopologyAddress = {
      val hashIds = mutable.Map.empty[String, Seq[Int]]

      // Default cache hash id
     updateHashIds(hashIds, cacheManager.getCache(), "")

      // Rest of cache has ids except the topology cache of course!
      for (cacheName <- asScalaIterator(cacheManager.getCacheNames.iterator)) {
         updateHashIds(hashIds, cacheManager.getCache(cacheName), cacheName)
      }

      TopologyAddress(host, port, immutable.Map[String, Seq[Int]]() ++ hashIds,
                      cacheManager.getAddress)
   }

   private def updateHashIds(hashIds: mutable.Map[String, Seq[Int]], cache: Cache[ByteArrayKey, CacheValue], hashIdKey: String) {
      val clusterAddress: Address = cacheManager.getAddress
      val cacheDm = cache.getAdvancedCache.getDistributionManager
      // TODO The following could be a bit more elegant
      // TODO And could be consolidated with other waiting time such as to update view
      if (cacheDm != null) {
          // sleep time between retries
         val minSleepTime = 500
         val maxSleepTime = 2000
         val maxWaitTime = cache.getConfiguration.getRehashRpcTimeout() * 10 // after which we give up!
         val giveupTime = System.currentTimeMillis + maxWaitTime
         var hashIdRetrieved = false
         val isTrace = isTraceEnabled
         do {
            try {
               val distHashIds = cacheDm.getConsistentHash.getHashIds(clusterAddress)
               if (isTrace)
                  trace("Cluster address (%s) has these hash ids associated: %s", clusterAddress, distHashIds)
               // Once hash ids retrieved, make them immutable and update the topology address
               hashIds += (hashIdKey -> asScalaBuffer(distHashIds.asInstanceOf[java.util.List[Int]]).toList)
               if (isTrace)
                  trace("After scala transformation, cluster address (%s) has these hash ids associated: %s", clusterAddress, hashIds)
               hashIdRetrieved = true
            } catch {
               case u: UnsupportedOperationException => {
                  if (isDebugEnabled)
                     debug("Unable to get all hash ids due to rehashing being in process.")
                  var time = rand.nextInt((maxSleepTime - minSleepTime) / 10)
                  time = (time * 10) + minSleepTime
                  if (isTrace)
                     trace("Sleeping for %s", Util.prettyPrintTime(time))
                  Thread.sleep(time) // sleep for a while and retry
               }
            }
         } while(!hashIdRetrieved && System.currentTimeMillis() < giveupTime)

         if (!hashIdRetrieved)
            throw new CacheException("Unable to retrieve hash ids for cache with name=%s on startup".format(cache.getName))
      }
   }

   private def updateTopologyView(replaced: Boolean, updateStartTime: Long)(f: TopologyView => Boolean): Boolean = {
      val giveupTime = updateStartTime + topologyUpdateTimeout
      if (replaced || System.currentTimeMillis() > giveupTime) replaced
      else updateTopologyView(isViewUpdated(f), giveupTime)(f)
   }

   private def isViewUpdated(f: TopologyView => Boolean): Boolean = {
      val currentView = topologyCache.get("view")
      val updated = f(currentView)
      if (!updated) {
         val minSleepTime = 500
         val maxSleepTime = 2000 // sleep time between retries
         var time = rand.nextInt((maxSleepTime - minSleepTime) / 10)
         time = (time * 10) + minSleepTime;
         if (isTraceEnabled) trace("Concurrent modification in topology view, sleeping for %s", Util.prettyPrintTime(time))
         Thread.sleep(time); // sleep for a while and retry
      }
      updated
   }

   override def stop {
      super.stop
      if (isClustered && topologyCache != null)
         removeSelfFromTopologyView
   }

   protected def removeSelfFromTopologyView {
      // Graceful shutdown, remove this node as member and install new view
      val currentView = topologyCache.get("view")
      // Comparing cluster address should be enough. Full object comparison could fail if hash id map has changed.
      val newMembers = currentView.members.filterNot(_.clusterAddress == address.clusterAddress)
      val isDebug = isDebugEnabled
      if (newMembers.length != (currentView.members.length - 1)) {
         if (isDebug) debug("Cluster member %s was not filtered out of the current view %s", address, currentView)
      } else {
         val newView = TopologyView(currentView.topologyId + 1, newMembers)
         // When stopping, attempt to remove self from topology but lock very
         // briefly and swallow any failures. This way, stop procedure does
         // not lock for long and it's not noisy. Any failures are not hugely
         // important cos once the JGroups transport is closed, the coordinator
         // will think the member crashed and will update the topology view.
         topologyCache.getAdvancedCache.withFlags(
            Flag.FAIL_SILENTLY, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT,
            Flag.FORCE_ASYNCHRONOUS)
                 .replace("view", currentView, newView)

         if (isDebug)
            debug("Attempted to update topology view (%s) on best-effort", newView)
      }
   }

   private def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager, typedProps: TypedProperties) {
      cacheManager.defineConfiguration(TopologyCacheName,
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

   @Listener(sync = false) // Use a separate thread to avoid blocking the view handler thread
   private class CrashedMemberDetectorListener {

      @ViewChanged
      def handleViewChange(e: ViewChangedEvent) {
         val isTrace = isTraceEnabled
         val cacheManager = e.getCacheManager
         // Only the coordinator can potentially make modifications related to crashed members.
         // This is to avoid all nodes trying to make the same modification which would be wasteful and lead to deadlocks.
         if (cacheManager.isCoordinator) {
            try {
               val newMembers = e.getNewMembers
               val oldMembers = e.getOldMembers
               // Someone left the cluster, verify whether it did it gracefully or crashed.
               if (oldMembers.size > newMembers.size) {
                  checkCrashedMembers(asScalaBuffer(newMembers).toList,
                                      asScalaBuffer(oldMembers).toList)
               }
            } catch {
               case t: Throwable => logErrorDetectingCrashedMember(t)
            }
         }
      }

      private def checkCrashedMembers(newMembers: List[Address], oldMembers: List[Address]) {
         val isTrace = isTraceEnabled
         val goneMembers = oldMembers.filterNot(newMembers contains)
         val updated = updateTopologyView(false, System.currentTimeMillis()) { currentView =>
            if (currentView != null) {
               var tmpMembers = currentView.members
               for (goneMember <- goneMembers) {
                  if (isTrace) trace("Old member %s is not in new view %s, did it crash?", goneMember, newMembers)
                  // If old memmber is in topology, it means that it had an abnormal ending
                  val (isCrashed, crashedTopologyMember) = isOldMemberInTopology(goneMember, currentView)
                  if (isCrashed) {
                     if (isTrace) {
                        trace("Old member %s with topology address %s is still present in Hot Rod topology " +
                           "%s, so must have crashed or was unable to update the view while stopping.",
                           goneMember, crashedTopologyMember, currentView)
                     }
                     tmpMembers = tmpMembers.filterNot(_ == crashedTopologyMember)
                     if (isTrace) trace("After removal, new Hot Rod topology is %s", tmpMembers)
                  }
               }
               if (tmpMembers.size < currentView.members.size) {
                  val newView = TopologyView(currentView.topologyId + 1, tmpMembers)
                  topologyCache.replace("view", currentView, newView)
               } else {
                  true // Mark as topology updated because there was no need to do so
               }
            } else {
               logViewNullWhileDetectingCrashedMember
               true
            }
         }

         if (!updated) {
            logUnableToUpdateView
         }
      }

      private def isOldMemberInTopology(oldMember: Address, view: TopologyView): (Boolean, TopologyAddress) = {
         // TODO: If members was stored as a map, this would be more efficient
         for (member <- view.members) {
            if (member.clusterAddress == oldMember) {
               return (true, member)
            }
         }
         (false, null)
      }

   }

}

object HotRodServer {
   val TopologyCacheName = "___hotRodTopologyCache"

   def getCacheInstance(cacheName: String, cacheManager: EmbeddedCacheManager): Cache[ByteArrayKey, CacheValue] = {
      if (cacheName.isEmpty) cacheManager.getCache[ByteArrayKey, CacheValue]
      else cacheManager.getCache(cacheName)
   }   
}

