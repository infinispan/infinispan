package org.infinispan.server.hotrod

import org.infinispan.manager.CacheManager
import org.infinispan.server.core.transport.{Decoder, Encoder}
import org.infinispan.server.core.{Logging, AbstractProtocolServer}
import org.infinispan.config.Configuration
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.Cache
import org.infinispan.notifications.Listener
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent
import scala.collection.JavaConversions._
import org.infinispan.remoting.transport.Address
import java.util.concurrent.{ThreadFactory, Callable, Executors}
import java.util.concurrent.atomic.AtomicInteger

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */

class HotRodServer extends AbstractProtocolServer("HotRod") {
   import HotRodServer._
   private var isClustered: Boolean = _
   private var address: TopologyAddress = _

   def getAddress: TopologyAddress = address

   override def getEncoder: Encoder = new HotRodEncoder

   override def getDecoder: Decoder = new HotRodDecoder(getCacheManager)

   override def start(host: String, port: Int, cacheManager: CacheManager, masterThreads: Int, workerThreads: Int, idleTimeout: Int) {
      super.start(host, port, cacheManager, masterThreads, workerThreads, idleTimeout)
      isClustered = cacheManager.getGlobalConfiguration.getTransportClass != null
      // If clustered, set up a cache for topology information
      if (isClustered)
         addSelfToTopologyView(host, port, cacheManager)
   }

   private def addSelfToTopologyView(host: String, port: Int, cacheManager: CacheManager) {
      defineTopologyCacheConfig(cacheManager)
      val topologyCache: Cache[String, TopologyView] = cacheManager.getCache(TopologyCacheName)
      cacheManager.addListener(new CrashedMemberDetectorListener)
      address = TopologyAddress(host, port, 0, cacheManager.getAddress)
      val currentView = topologyCache.get("view")
      // TODO: If distribution configured, add hashcode of this address
      if (currentView != null) {
         val newMembers = currentView.members ::: List(address)
         val newView = TopologyView(currentView.topologyId + 1, newMembers)
         val replaced = topologyCache.replace("view", currentView, newView)
         if (!replaced) {
            // TODO: There was a concurrent view modification, get and try to install new view again.
         } else {
            debug("Added {0} to topology, new view is {1}", address, newView)
         }
      } else {
         val newMembers = List(address)
         val newView = TopologyView(1, newMembers)
         val prev = topologyCache.putIfAbsent("view", newView)
         if (prev != null) {
            // TODO: There was a concurrent view modification, get and try to install new view again.
         } else {
            debug("First member to start, topology view is {0}", newView)
         }
      }
   }

   override def stop {
      super.stop
      if (isClustered)
         removeSelfFromTopologyView
   }

   protected def removeSelfFromTopologyView {
      // Graceful shutdown, remove this node as member and install new view
      val topologyCache: Cache[String, TopologyView] = getCacheManager.getCache(TopologyCacheName)
      val currentView = topologyCache.get("view")
      // TODO: If distribution configured, add hashcode of this address
      val newMembers = currentView.members.filterNot(_ == address)
      val newView = TopologyView(currentView.topologyId + 1, newMembers)
      val replaced = topologyCache.replace("view", currentView, newView)
      if (!replaced) {
         // TODO: There was a concurrent view modification. Just give up, logic to deal with crashed/stalled members will deal with this
      } else {
         debug("Removed {0} from topology view, new view is {1}", address, newView)
      }
   }

   protected def defineTopologyCacheConfig(cacheManager: CacheManager) {
      val topologyCacheConfig = new Configuration
      topologyCacheConfig.setCacheMode(CacheMode.REPL_SYNC)
      topologyCacheConfig.setSyncReplTimeout(10000) // Milliseconds
      topologyCacheConfig.setFetchInMemoryState(true) // State transfer required
      cacheManager.defineConfiguration(TopologyCacheName, topologyCacheConfig)
   }

   @Listener
   class CrashedMemberDetectorListener {
      import HotRodServer._

      private val executor = Executors.newCachedThreadPool(new ThreadFactory(){
         val threadCounter = new AtomicInteger

         override def newThread(r: Runnable): Thread = {
            var t = new Thread(r, "CrashedMemberDetectorThread-" + threadCounter.incrementAndGet)
            t.setDaemon(true)
            t
         }
      })

      @ViewChanged
      def handleViewChange(e: ViewChangedEvent) {
         val cacheManager = e.getCacheManager
         // Only the coordinator can potentially make modifications related to crashed members.
         // This is to avoid all nodes trying to make the same modification which would be wasteful and lead to deadlocks.
         if (cacheManager.isCoordinator) {
            // Use a separate thread to avoid blocking the view handler thread
            val callable = new Callable[Void] {
               override def call = {
                  try {
                     val newMembers = e.getNewMembers
                     val oldMembers = e.getOldMembers
                     // Someone left the cluster, verify whether it did it gracefully or crashed.
                     if (oldMembers.size > newMembers.size) {
                        val topologyCache: Cache[String, TopologyView] = getCacheManager.getCache(TopologyCacheName)
                        val currentView = topologyCache.get("view")
                        var newTopologyMembers = currentView.members
                        for (oldMember <- asIterator(oldMembers.iterator)) {
                           // If old member is not amongst the new ones, check whether it's still in the topology cache
                           if (!newMembers.contains(oldMember)) {
                              trace("Old member {0} is not in new view {1}, did it crash?", oldMember, newMembers)
                              // If old memmber is in topology, it means that it had an abnormal ending
                              val (isCrashed, crashedTopologyMember) = isOldMemberInTopology(oldMember, currentView)
                              if (isCrashed) {
                                 trace("Old member {0} with topology address {1} is still present in Hot Rod topology " +
                                       "{2}, so must have crashed.", oldMember, crashedTopologyMember, currentView)
                                 newTopologyMembers = newTopologyMembers.filterNot(_ == crashedTopologyMember)
                                 trace("After removal, new Hot Rod topology is {0}", newTopologyMembers)
                              }
                           }
                        }
                        if (newTopologyMembers.size < currentView.members.size) {
                           val newView = TopologyView(currentView.topologyId + 1, newTopologyMembers)
                           val replaced = topologyCache.replace("view", currentView, newView)
                           if (!replaced) {
                              // TODO: How to deal with concurrent failures at this point?
                           }
                        }
                     }
                  } catch {
                     case t: Throwable => error("Error detecting crashed member", t)
                  }
                  null
               }
            }
            executor.submit(callable);
         }
      }

      private def isOldMemberInTopology(oldMember: Address, view: TopologyView): (Boolean, TopologyAddress) = {
         for (member <- view.members) {
            if (member.clusterAddress == oldMember) {
               return (true, member)
            }
         }
         (false, null)
      }
   }

}

object HotRodServer extends Logging {
   val TopologyCacheName = "___hotRodTopologyCache"
}

