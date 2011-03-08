package org.infinispan.server.hotrod

import org.infinispan.server.core.transport.{Decoder, Encoder}
import org.infinispan.config.Configuration
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
import org.infinispan.server.core.{CacheValue, Logging, AbstractProtocolServer}
import org.infinispan.eviction.EvictionStrategy
import org.infinispan.util.{TypedProperties, ByteArrayKey, Util};
import org.infinispan.server.core.Main._

/**
 * Hot Rod server, in charge of defining its encoder/decoder and, if clustered, update the topology information
 * on startup and shutdown.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class HotRodServer extends AbstractProtocolServer("HotRod") with Logging {
   import HotRodServer._
   private var isClustered: Boolean = _
   private var address: TopologyAddress = _
   private var topologyCache: Cache[String, TopologyView] = _
   private val rand = new Random
   private val maxWaitTime = SECONDS.toMillis(30) // TODO: Make this configurable?

   def getAddress: TopologyAddress = address

   override def getEncoder: Encoder = new HotRodEncoder(getCacheManager)

   override def getDecoder: Decoder = new HotRodDecoder(getCacheManager)

   override def start(p: Properties, cacheManager: EmbeddedCacheManager) {
      val properties = if (p == null) new Properties else p
      isClustered = cacheManager.getGlobalConfiguration.getTransportClass != null
      if (isClustered)
         defineTopologyCacheConfig(cacheManager)
         
      super.start(properties, cacheManager, 11222)
   }

   override def startTransport(idleTimeout: Int, tcpNoDelay: Boolean, sendBufSize: Int, recvBufSize: Int, typedProps: TypedProperties) {
      // Start rest of the caches and self to view once we know for sure that we need to start
      // and we know that the rank calculator listener is registered

      // Start defined caches to avoid issues with lazily started caches
      for (cacheName <- asIterator(cacheManager.getCacheNames.iterator))
         cacheManager.getCache(cacheName)

      // If clustered, set up a cache for topology information
      if (isClustered) {
         val externalHost = typedProps.getProperty(PROP_KEY_PROXY_HOST, getHost)
         val externalPort = typedProps.getIntProperty(PROP_KEY_PROXY_PORT, getPort)
         if (isDebugEnabled)
            debug("Externally facing address is {0}:{1}", externalHost, externalPort)

         addSelfToTopologyView(externalHost, externalPort, cacheManager)
      }

      super.startTransport(idleTimeout, tcpNoDelay, sendBufSize, recvBufSize, typedProps)
   }

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: EmbeddedCacheManager) {
      topologyCache = cacheManager.getCache(TopologyCacheName)
      address = TopologyAddress(host, port, Map.empty, cacheManager.getAddress)
      val isDebug = isDebugEnabled
      if (isDebug) debug("Local topology address is {0}", address)
      cacheManager.addListener(new CrashedMemberDetectorListener)
      val updated = updateTopologyView(false, System.currentTimeMillis()) { currentView =>
         if (currentView != null) {
            val newMembers = currentView.members ::: List(address)
            val newView = TopologyView(currentView.topologyId + 1, newMembers)
            val updated = topologyCache.replace("view", currentView, newView)
            if (isDebug && updated) debug("Added {0} to topology, new view is {1}", address, newView)
            updated
         } else {
            val newMembers = List(address)
            val newView = TopologyView(1, newMembers)
            val updated = topologyCache.putIfAbsent("view", newView) == null
            if (isDebug && updated) debug("First member to start, topology view is {0}", newView)
            updated
         }
      }
      if (!updated)
         throw new CacheException("Unable to update topology view, so aborting startup")
   }

   private def updateTopologyView(replaced: Boolean, updateStartTime: Long)(f: TopologyView => Boolean): Boolean = {
      val giveupTime = updateStartTime + maxWaitTime
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
         if (isTraceEnabled) trace("Concurrent modification in topology view, sleeping for {0}", Util.prettyPrintTime(time))
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
         if (isDebug) debug("Cluster member {0} was not filtered out of the current view {1}", address, currentView)
      } else {
         val newView = TopologyView(currentView.topologyId + 1, newMembers)
         // TODO: Consider replace with 0 lock timeout and fail silently to avoid hold ups. Crash member detector can deal with any failures.
         val replaced = topologyCache.replace("view", currentView, newView)
         if (isDebug && !replaced) {
            debug("Attempt to update topology view failed due to a concurrent modification. " +
                  "Ignoring since logic to deal with crashed members will deal with it.")
         } else if (isDebug) {
            debug("Removed {0} from topology view, new view is {1}", address, newView)
         }
      }
   }

   protected def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager) {
      val topologyCacheConfig = new Configuration
      topologyCacheConfig.setCacheMode(CacheMode.REPL_SYNC)
      topologyCacheConfig.setSyncReplTimeout(10000) // Milliseconds
      topologyCacheConfig.setFetchInMemoryState(true) // State transfer required
      topologyCacheConfig.setEvictionStrategy(EvictionStrategy.NONE); // No eviction
      topologyCacheConfig.setExpirationLifespan(-1); // No maximum lifespan
      topologyCacheConfig.setExpirationMaxIdle(-1); // No maximum idle time
      cacheManager.defineConfiguration(TopologyCacheName, topologyCacheConfig)
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
                  val newMembersList = asBuffer(newMembers).toList
                  val oldMembersList = asBuffer(oldMembers).toList
                  val goneMembers = oldMembersList.filterNot(newMembersList contains)
                  val updated = updateTopologyView(false, System.currentTimeMillis()) { currentView =>
                     if (currentView != null) {
                        var tmpMembers = currentView.members
                        for (goneMember <- goneMembers) {
                           if (isTrace) trace("Old member {0} is not in new view {1}, did it crash?", goneMember, newMembers)
                           // If old memmber is in topology, it means that it had an abnormal ending
                           val (isCrashed, crashedTopologyMember) = isOldMemberInTopology(goneMember, currentView)
                           if (isCrashed) {
                              if (isTrace) trace("Old member {0} with topology address {1} is still present in Hot Rod topology " +
                                    "{2}, so must have crashed.", goneMember, crashedTopologyMember, currentView)
                              tmpMembers = tmpMembers.filterNot(_ == crashedTopologyMember)
                              if (isTrace) trace("After removal, new Hot Rod topology is {0}", tmpMembers)
                           }
                        }
                        if (tmpMembers.size < currentView.members.size) {
                           val newView = TopologyView(currentView.topologyId + 1, tmpMembers)
                           topologyCache.replace("view", currentView, newView)
                        } else {
                           true // Mark as topology updated because there was no need to do so
                        }
                     } else {
                        warn("While trying to detect a crashed member, current view returned null")
                        true
                     }
                  }
                  if (!updated) {
                     warn("Unable to update topology view after a crashed member left, wait for next view change.")
                  }
               }
            } catch {
               case t: Throwable => error("Error detecting crashed member", t)
            }
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

